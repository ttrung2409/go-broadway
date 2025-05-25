package broadway

import (
	"context"
	"fmt"
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
// batch key and optionally by partition key when partitioning is enabled.
//
// The batcher maintains queues of messages for each batch key, and processes
// batches when either:
// 1. The batch size threshold is reached
// 2. The batch timeout expires
//
// When partitioning is enabled, messages with the same partition key will
// be consistently routed to the same batch processor.
type batcher struct {
	config     BatcherConfig                         // Configuration for this batcher
	messages   map[string]*concurrentQueue[*Message] // Map of batch key to message queue
	hr         *hashRing[*batchProcessor]            // Hash ring for routing batches to processors
	receiver   chan []*Message                       // Channel for receiving new messages
	terminated bool                                  // Flag indicating whether the batcher is terminated
	mu         sync.Mutex                            // Mutex for thread-safe operations
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
		config:   config,
		messages: make(map[string]*concurrentQueue[*Message]),
		hr:       newHashRing[*batchProcessor](),
		receiver: make(chan []*Message),
		mu:       sync.Mutex{},
	}
}

// Run starts the batcher with the provided context, which starts
// batch processors according to the configured concurrency.
//
// Parameters:
//   - ctx: The context provided when starting the pipeline
//
// Returns:
//   - A channel that will receive a value, possibly an error, when the batcher terminates
func (b *batcher) Run(ctx context.Context) <-chan any {
	onTerminated := make(chan any)

	for i := 0; i < b.config.Concurrency; i++ {
		processor := newBatchProcessor(b.config.Processor)
		onTerminated := processor.Run(ctx)
		b.hr.AddNode(processor)

		go func(processor *batchProcessor, onTerminated <-chan any) {
			err := <-onTerminated

			if err != nil {
				b.handleProcessorPanic(processor, ctx)
			}
		}(processor, onTerminated)
	}

	go func() {
		defer func() {
			r := recover()

			if r != nil {
				fmt.Println("batcher panicked:", r)
				b.Terminate()
			} else {
				b.flush()
			}

			processors := b.hr.GetAllNodes()
			for _, processor := range processors {
				processor.Terminate()
			}

			onTerminated <- r
			close(onTerminated)
		}()

		for messages := range b.receiver {
			for _, message := range messages {
				if _, ok := b.messages[message.BatchKey]; !ok {
					b.messages[message.BatchKey] = newConcurrentQueue[*Message]()
				}

				b.messages[message.BatchKey].Enqueue(message)
			}
		}
	}()

	go b.processBatches(ctx)

	return onTerminated
}

// Terminate stops the batcher. After calling Terminate, the batcher will no longer accept new messages.
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
//
// This method will retry processing any batches that couldn't be successfully sent
// to a batch processor, with a short delay between retries, until all messages have
// been processed or the batcher gives up.
func (b *batcher) flush() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("batcher panicked:", r)
		}
	}()

	for {
		for batchKey, messages := range b.messages {
			if batch, ok := messages.DequeueAll(); ok {
				if b.processBatch(batchKey, batch) {
					delete(b.messages, batchKey)
				} else {
					messages.Prepend(batch...)
				}
			}
		}

		if len(b.messages) == 0 {
			return
		}

		time.Sleep(time.Millisecond * 500)
	}
}

// processBatches is the main processing loop for the batcher, running in a separate goroutine.
// It continuously monitors the message queues and processes batches when either:
//  1. A batch reaches the configured batch size
//  2. The batch timeout expires
//
// The method balances efficiency (by waiting for full batches) with timely processing
// (by processing partial batches after a timeout). If a batch cannot be processed
// successfully, the messages are put back in the queue to be retried later.
func (b *batcher) processBatches(ctx context.Context) {
	for {
		if b.terminated {
			return
		}

		select {
		case <-time.After(b.config.BatchTimeout):
			// Process all batches that have been waiting for the timeout period
			for batchKey, messages := range b.messages {
				if batch, ok := messages.DequeueMany(b.config.BatchSize); ok {
					if !b.processBatch(batchKey, batch) {
						messages.Prepend(batch...)
					}
				}
			}

		default:
			// Process any batches that have reached the batch size threshold
			for batchKey, messages := range b.messages {
				if messages.Len() < b.config.BatchSize {
					continue
				}

				if batch, ok := messages.DequeueMany(b.config.BatchSize); ok {
					if !b.processBatch(batchKey, batch) {
						messages.Prepend(batch...)
					}
				}
			}
		}
	}
}

// processBatch sends a batch of messages to an appropriate batch processor.
// When partitioning is enabled, it uses the partition key of the first message
// to select a processor, ensuring that all messages with the same partition key
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
//   - processor: The failed batch processor to replace
//   - ctx: The context for creating the new processor
func (b *batcher) handleProcessorPanic(processor *batchProcessor, ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Remove the failed processor from the hash ring
	b.hr.RemoveNode(processor)

	// Create and start a new processor
	newProcessor := newBatchProcessor(b.config.Processor)
	onTerminated := newProcessor.Run(ctx)

	// Find the processor that will share hash space with the new one
	keySharingProcessor, _ := b.hr.GetNextNode(newProcessor)

	// Pause the affected processor during the transition if one exists
	if keySharingProcessor != nil {
		keySharingProcessor.Pause()
	}

	// Add the new processor to the hash ring
	b.hr.AddNode(newProcessor)

	// Resume the affected processor
	if keySharingProcessor != nil {
		keySharingProcessor.Resume()
	}

	// Monitor the new processor for failures
	go func(processor *batchProcessor, onTerminated <-chan any) {
		err := <-onTerminated

		if err != nil {
			b.handleProcessorPanic(processor, ctx)
		}
	}(newProcessor, onTerminated)
}
