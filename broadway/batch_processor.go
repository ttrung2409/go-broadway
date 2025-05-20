package broadway

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

type BatchProcessor interface {
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

func (p *batchProcessor) Terminate() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.terminated {
		return
	}

	p.terminated = true
	close(p.receiver)
}

func (p *batchProcessor) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
}

func (p *batchProcessor) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
}

func (p *batchProcessor) ToString() string {
	return p.Id
}
