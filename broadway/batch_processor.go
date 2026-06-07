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

	// Clone creates a new instance of the batch processor.
	// This method is used to create multiple instances of the processor
	// for concurrent batch processing.
	//
	// Returns:
	//   - A new instance of the batch processor
	Clone() BatchProcessor
}

type batchProcessor struct {
	id string

	_processor    BatchProcessor
	_receiver     chan []*Message
	_mu           sync.Mutex
	_terminated   bool
	_paused       bool
	_terminatedCh chan any
}

func newBatchProcessor(p BatchProcessor) *batchProcessor {
	return &batchProcessor{
		id:            uuid.NewString(),
		_processor:    p.Clone(),
		_receiver:     make(chan []*Message),
		_mu:           sync.Mutex{},
		_terminatedCh: make(chan any, 1),
	}
}

// run starts the batch processor in a goroutine with the provided context
//
// Parameters:
//   - ctx: The context provided when starting the pipeline.
func (p *batchProcessor) run(ctx context.Context) {

	go func() {
		defer func() {
			r := recover()

			if r != nil {
				fmt.Printf("batch processor %s panicked: %v\n", p.id, r)
				fmt.Println(string(debug.Stack()))

				p.terminate()
			}

			p._terminatedCh <- r
			close(p._terminatedCh)
		}()

		for messages := range p._receiver {
			func(messages []*Message) {
				defer func() {
					r := recover()

					if r != nil {
						p._paused = true
						p._mu.Unlock()

						if ack := messages[0].ack; ack != nil {
							ack(messages,
								fmt.Errorf("batch processor %s panicked: %v", p.id, r))
						}

						panic(r)
					} else {
						p._mu.Unlock()
					}
				}()

				processedMessages, err := p._processor.Handle(messages, ctx)

				if ack := messages[0].ack; ack != nil {
					ack(processedMessages, err)
				}

			}(messages)
		}

	}()

}

// send attempts to send a batch of messages to this batch processor for processing.
// Returns true if the batch was accepted, false if the processor is busy, paused or terminated.
//
// Parameters:
//   - batch: A batch of messages to be processed.
//
// Returns:
//   - true if the batch was accepted, false otherwise.
func (p *batchProcessor) send(batch []*Message) bool {
	if !p._mu.TryLock() {
		return false
	}

	if p._terminated || p._paused {
		p._mu.Unlock()
		return false
	}

	p._receiver <- batch

	return true
}

// terminate waits for the on-going processing to be completed then terminates the processor.
// After calling terminate, the processor will no longer accept new batches.
func (p *batchProcessor) terminate() {
	p._mu.Lock()
	defer p._mu.Unlock()

	if p._terminated {
		return
	}

	p._terminated = true
	close(p._receiver)
}

// pause temporarily stops the batch processor from accepting new batches.
// The processor will continue processing any batches already in progress.
func (p *batchProcessor) pause() {
	p._mu.Lock()
	defer p._mu.Unlock()
	p._paused = true
}

// resume allows the batch processor to accept new batches after being paused.
func (p *batchProcessor) resume() {
	p._mu.Lock()
	defer p._mu.Unlock()
	p._paused = false
}

// toString returns a string representation of this batch processor.
func (p *batchProcessor) toString() string {
	return p.id
}

func (p *batchProcessor) terminated() <-chan any {
	return p._terminatedCh
}
