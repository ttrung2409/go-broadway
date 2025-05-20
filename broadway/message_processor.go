package broadway

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
)

const defaultMinDemand = 5
const defaultMaxDemand = 10

type MessageProcessorConfig struct {
	Concurrency int
	Processor   MessageProcessor
	MinDemand   int
	MaxDemand   int
}

type MessageProcessor interface {
	Handle(message *Message, ctx context.Context) (*Message, error)
}

type messageProcessor struct {
	MessageProcessor
	Id              string
	config          MessageProcessorConfig
	messages        *concurrentQueue[*Message]
	pendingRequests map[string]*request
	producers       map[string]*producer
	batchers        map[string]*batcher
	terminated      bool
	mu              sync.RWMutex
}

func newMessageProcessor(
	config MessageProcessorConfig,
	producers map[string]*producer,
	batchers map[string]*batcher,
) *messageProcessor {
	if config.MinDemand == 0 {
		config.MinDemand = defaultMinDemand
	}

	if config.MaxDemand == 0 {
		config.MaxDemand = defaultMaxDemand
	}

	return &messageProcessor{
		Id:               uuid.New().String(),
		MessageProcessor: config.Processor,
		config:           config,
		messages:         newConcurrentQueue[*Message](),
		pendingRequests:  make(map[string]*request),
		mu:               sync.RWMutex{},
		producers:        producers,
		batchers:         batchers,
	}
}

func (p *messageProcessor) Run(ctx context.Context) <-chan any {

	onTerminated := make(chan any)

	go func() {
		defer func() {
			r := recover()

			if r != nil {
				fmt.Println("message processor panicked:", r)
				p.Terminate()
			} else {
				p.flush(ctx)
			}

			for _, request := range p.pendingRequests {
				request.Close()
			}

			onTerminated <- r
			close(onTerminated)
		}()

		for {
			if p.terminated {
				return
			}

			if p.messages.Len() < p.config.MinDemand {
				p.request()
			}

			count := min((p.config.MaxDemand+p.config.MinDemand)/2, p.messages.Len())
			messages, ok := p.messages.DequeueMany(count)

			if !ok {
				time.Sleep(time.Millisecond * 500)
				continue
			}

			p.process(messages, ctx)
		}
	}()

	return onTerminated
}

func (p *messageProcessor) Terminate() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.terminated {
		return
	}

	p.terminated = true
}

func (p *messageProcessor) flush(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("message processor panicked:", r)
		}
	}()

	if messages, ok := p.messages.DequeueAll(); ok {
		p.process(messages, ctx)
	}
}

func (p *messageProcessor) process(messages []*Message, ctx context.Context) {
	messages = lo.Map(messages, func(message *Message, _ int) *Message {
		processedMessage, err := p.Handle(message, ctx)

		if processedMessage == nil {
			processedMessage = message
		}

		if processedMessage.Batcher == "" {
			processedMessage.Batcher = defaultBatcherName
		}

		if processedMessage.BatchKey == "" {
			processedMessage.BatchKey = defaultBatchKey
		}

		if p.concurrentBatchers() == nil && processedMessage.Acknowledger != nil {
			processedMessage.Acknowledger.Ack([]*Message{processedMessage}, err)
		}

		return processedMessage
	})

	if p.concurrentBatchers() != nil {
		messagesByBatchers := lo.GroupBy(messages, func(message *Message) string {
			return message.Batcher
		})

		for batcherName, messagesInBatcher := range messagesByBatchers {
			if batcher, ok := p.concurrentBatchers()[batcherName]; ok {
				batcher.Send(messagesInBatcher)
			}
		}
	}
}

func (p *messageProcessor) request() {
	demand := p.config.MaxDemand - p.config.MinDemand

	for producerId, producer := range p.concurrentProducers() {
		if r, ok := p.pendingRequests[producerId]; ok {
			if r.Closed() {
				delete(p.pendingRequests, producerId)
			} else {
				continue
			}
		}

		r := newRequest(demand)
		p.pendingRequests[producerId] = r

		if ok := producer.Send(r); !ok {
			continue
		}

		go func(r *request) {
			for messages := range r.Response {
				p.messages.Enqueue(messages...)
			}

			delete(p.pendingRequests, producerId)
		}(r)
	}
}

func (p *messageProcessor) concurrentProducers() map[string]*producer {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.producers
}

func (p *messageProcessor) concurrentBatchers() map[string]*batcher {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.batchers
}

func (p *messageProcessor) SetProducers(producers map[string]*producer) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.producers = producers
}

func (p *messageProcessor) SetBatchers(batchers map[string]*batcher) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.batchers = batchers
}
