package broadway

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultBatcherName  = "default"
	defaultBatchKey     = "default"
	defaultBatchSize    = 100
	defaultBatchTimeout = 3 * time.Second
)

type BatcherConfig struct {
	Name         string         // Unique name for this batcher (default: "default")
	BatchSize    int            // Maximum number of messages per batch (default: 100)
	BatchTimeout time.Duration  // Maximum time to wait before processing a partial batch (default: 1 second)
	Concurrency  int            // Number of concurrent batch processors
	Processor    BatchProcessor // The processor that handles batches of messages
}

// batcher is responsible for collecting individual messages into batches
// and forwarding them to batch processors. It supports grouping messages by
// batch key or partition key when partitioning is enabled.
//
// The batcher maintains queues of messages for each batch key, and processes
// batches when either:
// 1. The batch size threshold is reached
// 2. The batch timeout expires
//
// When partitioning is enabled, messages with the same partition key will
// be consistently routed to the same batch processor.
type batcher struct {
	config BatcherConfig

	_batches         *concurrentMap[string, *concurrentQueue[*Message]]
	_hr              *hashRing[*batchProcessor]
	_receiver        chan []*Message
	_terminated         atomic.Bool
	_fullyTerminated    atomic.Bool
	_processBatchesDone chan bool
	_mu                 sync.Mutex
	_terminatedCh       chan any
}

func newBatcher(config BatcherConfig) *batcher {
	if config.Name == "" {
		config.Name = defaultBatcherName
	}

	if config.BatchSize == 0 {
		config.BatchSize = defaultBatchSize
	}

	if config.BatchTimeout == 0 {
		config.BatchTimeout = defaultBatchTimeout
	}

	return &batcher{
		config:        config,
		_batches:            newConcurrentMap[string, *concurrentQueue[*Message]](),
		_hr:                 newHashRing[*batchProcessor](),
		_receiver:           make(chan []*Message),
		_processBatchesDone: make(chan bool),
		_mu:                 sync.Mutex{},
		_terminatedCh:       make(chan any),
	}
}

// run starts the batcher with the provided context, which starts
// batch processors according to the configured concurrency.
//
// Parameters:
//   - ctx: The context provided when starting the pipeline
func (b *batcher) run(ctx context.Context) {
	for i := 0; i < b.config.Concurrency; i++ {
		processor := newBatchProcessor(b.config.Processor)
		processor.run(context.WithValue(ctx, BatcherContextKey, b.config.Name))
		b._hr.addNode(processor)

		go func(p *batchProcessor) {
			panic, ok := <-p.terminated()

			if ok && panic != nil {
				b.handleProcessorPanic(p, ctx)
			}
		}(processor)
	}

	go func() {
		defer func() {
			<-b._processBatchesDone

			b.flush()

			processors := b._hr.getAllNodes()
			for _, processor := range processors {
				processor.terminate()
			}
			for _, processor := range processors {
				<-processor.terminated()
			}

			b._fullyTerminated.Store(true)
			close(b._terminatedCh)
		}()

		for messages := range b._receiver {

			for _, message := range messages {
				if _, ok := b._batches.get(message.BatchKey); !ok {
					b._batches.set(message.BatchKey, newConcurrentQueue[*Message]())
				}

				batch, _ := b._batches.get(message.BatchKey)
				batch.enqueue(message)
			}
		}
	}()

	go b.processBatches()

}

// terminate flushes all remaining messages in the queues and stops the batcher.
// After calling terminate, the batcher will no longer accept new messages.
func (b *batcher) terminate() {
	b._mu.Lock()
	defer b._mu.Unlock()

	if b._terminated.Swap(true) {
		return
	}

	close(b._receiver)
}

// send attempts to accept a batch of messages for processing.
// Returns true if the messages were accepted, false otherwise.
//
// Parameters:
//   - messages: A batch of messages to be processed.
//
// Returns:
//   - true if the messages were accepted, false otherwise
func (b *batcher) send(messages []*Message) bool {
	b._mu.Lock()
	defer b._mu.Unlock()

	if b._terminated.Load() {
		return false
	}

	b._receiver <- messages

	return true
}

// flush processes all remaining messages in the batcher's queues when the batcher
// is being terminated. It attempts to deliver all batched messages to their respective
// batch processors before shutting down.
func (b *batcher) flush() {

	for {
		for batchKey, messages := range b._batches.toMap() {
			if batch, ok := messages.dequeueAll(); ok {
				if b.processBatch(batchKey, batch) {
					b._batches.delete(batchKey)
				} else {
					messages.prepend(batch...)
				}
			} else {
				b._batches.delete(batchKey)
			}
		}

		if b._batches.len() == 0 {
			return
		}

		<-time.After(time.Millisecond * 100)
	}
}

// processBatches is the main processing loop for the batcher, running in a separate goroutine.
// It continuously monitors the message queues and processes batches when either:
//  1. A batch reaches the configured batch size
//  2. The batch timeout expires
func (b *batcher) processBatches() {
	defer close(b._processBatchesDone)
	ticker := time.NewTicker(b.config.BatchTimeout)
	defer ticker.Stop()

	for {
		if b._terminated.Load() {
			return
		}

		select {
		case <-ticker.C:
			for batchKey, messages := range b._batches.toMap() {
				if batch, ok := messages.dequeue(b.config.BatchSize); ok {
					if !b.processBatch(batchKey, batch) {
						messages.prepend(batch...)
					}
				}
			}

		default:
			// Check for any batches that have reached the batch size threshold
			for batchKey, messages := range b._batches.toMap() {
				if messages.len() >= b.config.BatchSize {
					if batch, ok := messages.dequeue(b.config.BatchSize); ok {
						if !b.processBatch(batchKey, batch) {
							messages.prepend(batch...)
						}
					}
				}
			}
		}

		time.Sleep(time.Millisecond * 100)

	}
}

// processBatch sends a batch of messages to an appropriate batch processor.
// When partitioning is enabled, it ensures that all messages with the same partition key
// are processed by the same batch processor.
//
// Parameters:
//   - batchKey: The batch key for this group of messages
//   - batch: A slice of messages to be processed as a batch
//
// Returns:
//   - true if the batch was successfully sent to a processor, false otherwise
func (b *batcher) processBatch(batchKey string, batch []*Message) bool {

	if processor, ok := b._hr.getNode(batchKey); ok {
		return processor.send(batch)
	}

	return false
}

// handleProcessorPanic is called when a batch processor panics.
// It replaces the failed processor with a new one while minimizing
// disruption to the message processing flow.
//
// The method ensures that:
//  1. The failed processor is removed from the hash ring
//  2. A new processor is created and added to the hash ring
//  3. Processors that may be affected by rehashing are temporarily paused
//     during the transition to prevent message loss or duplication
//  4. The new processor is monitored for failures
//
// Parameters:
//   - processor: The failed batch processor to be replaced
//   - ctx: The context provided when starting the pipeline
func (b *batcher) handleProcessorPanic(processor *batchProcessor, ctx context.Context) {
	b._mu.Lock()
	defer b._mu.Unlock()

	b._hr.removeNode(processor)

	newProcessor := newBatchProcessor(b.config.Processor)
	newProcessor.run(ctx)

	keySharingProcessor, _ := b._hr.getNextNode(newProcessor)

	if keySharingProcessor != nil {
		keySharingProcessor.pause()
	}

	b._hr.addNode(newProcessor)

	if keySharingProcessor != nil {
		keySharingProcessor.resume()
	}

	go func(p *batchProcessor) {
		panic, ok := <-p.terminated()

		if ok && panic != nil {
			b.handleProcessorPanic(p, ctx)
		}
	}(newProcessor)

}

func (b *batcher) terminated() <-chan any {
	return b._terminatedCh
}

func (b *batcher) isTerminated() bool {
	return b._fullyTerminated.Load()
}
