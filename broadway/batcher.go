package broadway

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

const defaultBatcherName = "default"
const defaultBatchKey = "default"

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
	config       BatcherConfig
	messages     *concurrentMap[string, *concurrentQueue[*Message]]
	hr           *hashRing[*batchProcessor]
	receiver     chan []*Message
	terminated   bool
	mu           sync.Mutex
	onTerminated chan any
}

func newBatcher(config BatcherConfig) *batcher {
	if config.Name == "" {
		config.Name = defaultBatcherName
	}

	if config.BatchSize == 0 {
		config.BatchSize = 100
	}

	if config.BatchTimeout == 0 {
		config.BatchTimeout = time.Second
	}

	return &batcher{
		config:       config,
		messages:     newConcurrentMap[string, *concurrentQueue[*Message]](),
		hr:           newHashRing[*batchProcessor](),
		receiver:     make(chan []*Message),
		mu:           sync.Mutex{},
		onTerminated: make(chan any),
	}
}

// Run starts the batcher with the provided context, which starts
// batch processors according to the configured concurrency.
//
// Parameters:
//   - ctx: The context provided when starting the pipeline
func (b *batcher) Run(ctx context.Context) {
	for i := 0; i < b.config.Concurrency; i++ {
		processor := newBatchProcessor(b.config.Processor)
		processor.Run(context.WithValue(ctx, BatcherContextKey, b.config.Name))
		b.hr.AddNode(processor)

		go func(p *batchProcessor) {
			panic, ok := <-p.OnTerminated()

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
				b.Terminate()
			} else {
				b.flush()
			}

			processors := b.hr.GetAllNodes()
			for _, processor := range processors {
				processor.Terminate()
			}

			b.onTerminated <- r
			close(b.onTerminated)
			b.onTerminated = nil
		}()

		for messages := range b.receiver {

			for _, message := range messages {
				if _, ok := b.messages.Get(message.BatchKey); !ok {
					b.messages.Set(message.BatchKey, newConcurrentQueue[*Message]())
				}

				batch, _ := b.messages.Get(message.BatchKey)
				batch.Enqueue(message)
			}
		}
	}()

	go b.processBatches()

}

// Terminate flushes all remaining messages in the queues and stops the batcher.
// After calling Terminate, the batcher will no longer accept new messages.
func (b *batcher) Terminate() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.terminated {
		return
	}

	b.terminated = true
	close(b.receiver)
}

// Send attempts to accept a batch of messages for processing.
// Returns true if the messages were accepted, false otherwise.
//
// Parameters:
//   - messages: A batch of messages to be processed.
//
// Returns:
//   - true if the messages were accepted, false otherwise
func (b *batcher) Send(messages []*Message) bool {
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
func (b *batcher) flush() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("batcher panicked: %v\n", r)
		}
	}()

	for {
		for batchKey, messages := range b.messages.ToMap() {
			if batch, ok := messages.DequeueAll(); ok {
				if b.processBatch(batchKey, batch) {
					b.messages.Delete(batchKey)
				} else {
					messages.Prepend(batch...)
				}
			} else {
				b.messages.Delete(batchKey)
			}
		}

		if b.messages.Len() == 0 {
			return
		}

		time.Sleep(time.Millisecond * 100)
	}
}

func (b *batcher) flushAndFailAll(r any) {
	for _, messages := range b.messages.Values() {
		msgs := messages.ToSlice()

		if len(msgs) == 0 {
			continue
		}

		if ack := msgs[0].Ack(); ack != nil {
			ack(msgs, fmt.Errorf("batcher %s panicked: %v", b.config.Name, r))
		}
	}
}

// processBatches is the main processing loop for the batcher, running in a separate goroutine.
// It continuously monitors the message queues and processes batches when either:
//  1. A batch reaches the configured batch size
//  2. The batch timeout expires
func (b *batcher) processBatches() {
	ticker := time.NewTicker(b.config.BatchTimeout)
	defer ticker.Stop()

	for {
		if b.terminated {
			return
		}

		select {
		case <-ticker.C:
			for batchKey, messages := range b.messages.ToMap() {
				if batch, ok := messages.DequeueMany(b.config.BatchSize); ok {
					if !b.processBatch(batchKey, batch) {
						messages.Prepend(batch...)
					}
				}
			}

		default:
			// Check for any batches that have reached the batch size threshold
			for batchKey, messages := range b.messages.ToMap() {
				if messages.Len() >= b.config.BatchSize {
					if batch, ok := messages.DequeueMany(b.config.BatchSize); ok {
						if !b.processBatch(batchKey, batch) {
							messages.Prepend(batch...)
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

	if processor, ok := b.hr.GetNode(batchKey); ok {
		return processor.Send(batch)
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
	b.mu.Lock()
	defer b.mu.Unlock()

	b.hr.RemoveNode(processor)

	newProcessor := newBatchProcessor(b.config.Processor)
	newProcessor.Run(ctx)

	keySharingProcessor, _ := b.hr.GetNextNode(newProcessor)

	if keySharingProcessor != nil {
		keySharingProcessor.Pause()
	}

	b.hr.AddNode(newProcessor)

	if keySharingProcessor != nil {
		keySharingProcessor.Resume()
	}

	go func(p *batchProcessor) {
		panic, ok := <-p.OnTerminated()

		if ok && panic != nil {
			b.handleProcessorPanic(p, ctx)
		}
	}(newProcessor)
}

func (b *batcher) OnTerminated() <-chan any {
	return b.onTerminated
}

func (b *batcher) IsTerminated() bool {
	return b.onTerminated == nil
}

func (b *batcher) Name() string {
	return b.config.Name
}
