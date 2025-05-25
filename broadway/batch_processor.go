package broadway

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

type BatchProcessor interface {
	// Handle processes a batch of messages and returns the processed messages
	// along with any error that occurred during processing. The returned messages
	// will be sent to the acknowledger for acknowledgment.
	//
	// Parameters:
	//   - messages: The batch of messages to process.
	//   - ctx: The context provided when starting the pipeline.
	//
	// Returns:
	//   - The processed messages for acknowledgment
	//   - An error if processing failed, or nil if successful.
	Handle(messages []*Message, ctx context.Context) ([]*Message, error)
}

type batchProcessor struct {
	BatchProcessor
	Id         string
	receiver   chan []*Message
	mu         sync.Mutex
	terminated bool
	paused     bool
}

func newBatchProcessor(p BatchProcessor) *batchProcessor {
	return &batchProcessor{
		Id:             uuid.NewString(),
		BatchProcessor: p,
		receiver:       make(chan []*Message),
		mu:             sync.Mutex{},
	}
}

// Run starts the batch processor in a goroutine with the provided context
//
// Parameters:
//   - ctx: The context provided when starting the pipeline.
//
// Returns:
//   - A channel that will receive a value, possibly an error, when the processor terminates.
func (p *batchProcessor) Run(ctx context.Context) <-chan any {
	onTerminated := make(chan any)

	go func() {
		defer func() {
			p.terminated = true
			p.mu.Unlock()

			r := recover()

			if r != nil {
				fmt.Println("batch processor panicked:", r)
				close(p.receiver)
			}

			onTerminated <- r
			close(onTerminated)
		}()

		for messages := range p.receiver {
			processedMessages, err := p.Handle(messages, ctx)

			acknowledger := messages[0].Acknowledger
			if acknowledger != nil {
				acknowledger.Ack(processedMessages, err)
			}
		}

	}()

	return onTerminated
}

// Send attempts to send a batch of messages to this batch processor for processing.
// Returns true if the batch was accepted, false if the processor is busy, paused or terminated.
//
// Parameters:
//   - batch: A batch of messages to be processed.
//
// Returns:
//   - true if the batch was accepted, false otherwise.
func (p *batchProcessor) Send(batch []*Message) bool {
	if !p.mu.TryLock() {
		return false
	}

	if p.terminated || p.paused {
		p.mu.Unlock()
		return false
	}

	p.receiver <- batch

	return true
}

// Terminate waits for the on-going processing to be completed then terminates the processor.
// After calling Terminate, the processor will no longer accept new batches.
func (p *batchProcessor) Terminate() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.terminated {
		return
	}

	p.terminated = true
	close(p.receiver)
}

// Pause temporarily stops the batch processor from accepting new batches.
// The processor will continue processing any batches already in progress.
func (p *batchProcessor) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
}

// Resume allows the batch processor to accept new batches after being paused.
func (p *batchProcessor) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
}

// ToString returns a string representation of this batch processor.
func (p *batchProcessor) ToString() string {
	return p.Id
}
