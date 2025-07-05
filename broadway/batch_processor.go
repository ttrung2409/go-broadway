package broadway

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"

	"github.com/google/uuid"
)

type BatchProcessor interface {

	// Handle processes a batch of messages and returns the processed messages
	// along with any error that occurred during processing. The returned messages
	// will be sent to the acknowledger for acknowledgment.
	//
	// Parameters:
	//   - messages: The batch of messages to be processed.
	//   - ctx: The context provided when starting the pipeline.
	//
	// Returns:
	//   - The processed messages for acknowledgment
	//   - An error if processing failed, or nil if successful.
	Handle(messages []*Message, ctx context.Context) ([]*Message, error)

	Clone() BatchProcessor
}

type batchProcessor interface {
	// run starts the batch processor in a goroutine with the provided context
	//
	// Parameters:
	//   - ctx: The context provided when starting the pipeline.
	run(ctx context.Context)

	// send attempts to send a batch of messages to this batch processor for processing.
	// Returns true if the batch was accepted, false if the processor is busy, paused or terminated.
	//
	// Parameters:
	//   - batch: A batch of messages to be processed.
	//
	// Returns:
	//   - true if the batch was accepted, false otherwise.
	send(batch []*Message) bool

	// terminate waits for the on-going processing to be completed then terminates the processor.
	// After calling terminate, the processor will no longer accept new batches.
	terminate()

	// pause temporarily stops the batch processor from accepting new batches.
	// The processor will continue processing any batches already in progress.
	pause()

	// resume allows the batch processor to accept new batches after being paused.
	resume()

	// toString returns a string representation of this batch processor.
	toString() string

	onTerminated() <-chan any
}

type internalBatchProcessor struct {
	id            string
	processor     BatchProcessor
	receiver      chan []*Message
	mu            sync.Mutex
	terminated    bool
	paused        bool
	_onTerminated chan any
}

func newBatchProcessor(p BatchProcessor) batchProcessor {
	return &internalBatchProcessor{
		id:            uuid.NewString(),
		processor:     p.Clone(),
		receiver:      make(chan []*Message),
		mu:            sync.Mutex{},
		_onTerminated: make(chan any),
	}
}

func (p *internalBatchProcessor) run(ctx context.Context) {

	go func() {
		for messages := range p.receiver {
			func(messages []*Message) {
				defer func() {
					p.mu.Unlock()

					r := recover()

					if r != nil {
						fmt.Printf("batch processor %s panicked: %v\n", p.id, r)
						fmt.Println(string(debug.Stack()))

						if ack := messages[0].ack; ack != nil {
							ack(messages,
								fmt.Errorf("batch processor %s panicked: %v", p.id, r))
						}

						p.terminate()
					}
				}()

				processedMessages, err := p.processor.Handle(messages, ctx)

				if ack := messages[0].ack; ack != nil {
					ack(processedMessages, err)
				}

			}(messages)
		}
	}()

}

func (p *internalBatchProcessor) send(batch []*Message) bool {
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

func (p *internalBatchProcessor) terminate() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.terminated {
		return
	}

	p.terminated = true
	close(p.receiver)
}

func (p *internalBatchProcessor) pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
}

func (p *internalBatchProcessor) resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
}

func (p *internalBatchProcessor) toString() string {
	return p.id
}

func (p *internalBatchProcessor) onTerminated() <-chan any {
	return p._onTerminated
}
