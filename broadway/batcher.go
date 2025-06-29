package broadway

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

const (
	defaultBatcherName = "default"
	defaultBatchKey    = "default"
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
type batcher interface {
	// run starts the batcher with the provided context, which starts
	// batch processors according to the configured concurrency.
	//
	// Parameters:
	//   - ctx: The context provided when starting the pipeline
	run(ctx context.Context)

	// terminate flushes all remaining messages in the queues and stops the batcher.
	// After calling terminate, the batcher will no longer accept new messages.
	terminate()

	// send attempts to accept a batch of messages for processing.
	// Returns true if the messages were accepted, false otherwise.
	//
	// Parameters:
	//   - messages: A batch of messages to be processed.
	//
	// Returns:
	//   - true if the messages were accepted, false otherwise
	send(messages []*Message) bool

	config() BatcherConfig
	onTerminated() <-chan any
	isTerminated() bool
}

type internalBatcher struct {
	_config       BatcherConfig
	messages      *concurrentMap[string, *concurrentQueue[*Message]]
	hr            *hashRing[batchProcessor]
	receiver      chan []*Message
	terminated    bool
	mu            sync.Mutex
	_onTerminated chan any
}

func newBatcher(config BatcherConfig) batcher {
	if config.Name == "" {
		config.Name = defaultBatcherName
	}

	if config.BatchSize == 0 {
		config.BatchSize = 100
	}

	if config.BatchTimeout == 0 {
		config.BatchTimeout = time.Second
	}

	return &internalBatcher{
		_config:       config,
		messages:      newConcurrentMap[string, *concurrentQueue[*Message]](),
		hr:            newHashRing[batchProcessor](),
		receiver:      make(chan []*Message),
		mu:            sync.Mutex{},
		_onTerminated: make(chan any),
	}
}

func (b *internalBatcher) run(ctx context.Context) {
	for i := 0; i < b._config.Concurrency; i++ {
		processor := newBatchProcessor(b._config.Processor)
		processor.run(context.WithValue(ctx, BatcherContextKey, b._config.Name))
		b.hr.addNode(processor)

		go func(p batchProcessor) {
			panic, ok := <-p.onTerminated()

			if ok && panic != nil {
				b.handleProcessorPanic(p, ctx)
			}
		}(processor)
	}

	go func() {
		defer func() {
			r := recover()

			if r != nil {
				fmt.Printf("batcher panicked: %v\n", r)
				fmt.Println(string(debug.Stack()))

				b.flushAndFailAll(r)
				b.terminate()
			} else {
				b.flush()
			}

			processors := b.hr.getAllNodes()
			for _, processor := range processors {
				processor.terminate()
			}

			b._onTerminated <- r
			close(b._onTerminated)
			b._onTerminated = nil
		}()

		for messages := range b.receiver {

			for _, message := range messages {
				if _, ok := b.messages.get(message.BatchKey); !ok {
					b.messages.set(message.BatchKey, newConcurrentQueue[*Message]())
				}

				batch, _ := b.messages.get(message.BatchKey)
				batch.enqueue(message)
			}
		}
	}()

	go b.processBatches()

}

func (b *internalBatcher) terminate() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.terminated {
		return
	}

	b.terminated = true
	close(b.receiver)
}

func (b *internalBatcher) send(messages []*Message) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.terminated {
		return false
	}

	b.receiver <- messages

	return true
}

// flush processes all remaining messages in the batcher's queues when the batcher
// is being terminated. It attempts to deliver all batched messages to their respective
// batch processors before shutting down.
func (b *internalBatcher) flush() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("batcher panicked: %v\n", r)
		}
	}()

	for {
		for batchKey, messages := range b.messages.toMap() {
			if batch, ok := messages.dequeueAll(); ok {
				if b.processBatch(batchKey, batch) {
					b.messages.delete(batchKey)
				} else {
					messages.prepend(batch...)
				}
			} else {
				b.messages.delete(batchKey)
			}
		}

		if b.messages.len() == 0 {
			return
		}

		time.Sleep(time.Millisecond * 100)
	}
}

func (b *internalBatcher) flushAndFailAll(r any) {
	for _, messages := range b.messages.values() {
		msgs := messages.toSlice()

		if len(msgs) == 0 {
			continue
		}

		if ack := msgs[0].ack; ack != nil {
			ack(msgs, fmt.Errorf("batcher %s panicked: %v", b._config.Name, r))
		}
	}
}

// processBatches is the main processing loop for the batcher, running in a separate goroutine.
// It continuously monitors the message queues and processes batches when either:
//  1. A batch reaches the configured batch size
//  2. The batch timeout expires
func (b *internalBatcher) processBatches() {
	ticker := time.NewTicker(b._config.BatchTimeout)
	defer ticker.Stop()

	for {
		if b.terminated {
			return
		}

		select {
		case <-ticker.C:
			for batchKey, messages := range b.messages.toMap() {
				if batch, ok := messages.dequeueMany(b._config.BatchSize); ok {
					if !b.processBatch(batchKey, batch) {
						messages.prepend(batch...)
					}
				}
			}

		default:
			// Check for any batches that have reached the batch size threshold
			for batchKey, messages := range b.messages.toMap() {
				if messages.len() >= b._config.BatchSize {
					if batch, ok := messages.dequeueMany(b._config.BatchSize); ok {
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
func (b *internalBatcher) processBatch(batchKey string, batch []*Message) bool {

	if processor, ok := b.hr.getNode(batchKey); ok {
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
func (b *internalBatcher) handleProcessorPanic(processor batchProcessor, ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.hr.removeNode(processor)

	newProcessor := newBatchProcessor(b._config.Processor)
	newProcessor.run(ctx)

	keySharingProcessor, _ := b.hr.getNextNode(newProcessor)

	if keySharingProcessor != nil {
		keySharingProcessor.pause()
	}

	b.hr.addNode(newProcessor)

	if keySharingProcessor != nil {
		keySharingProcessor.resume()
	}

	go func(p batchProcessor) {
		panic, ok := <-p.onTerminated()

		if ok && panic != nil {
			b.handleProcessorPanic(p, ctx)
		}
	}(newProcessor)
}

func (b *internalBatcher) onTerminated() <-chan any {
	return b._onTerminated
}

func (b *internalBatcher) isTerminated() bool {
	return b._onTerminated == nil
}

func (b *internalBatcher) config() BatcherConfig {
	return b._config
}
