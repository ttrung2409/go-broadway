package broadway

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
)

const defaultBufferSize = 10000

type ProducerConfig struct {
	Concurrency int
	Transformer Transformer
	Producer    Producer
	BufferSize  int
}

type Producer interface {
	HandleDemand(demand int) []MessagePayload
}

type producer struct {
	Producer
	Id          string
	config      ProducerConfig
	requests    *concurrentQueue[*request]
	requestChan chan *request
	mu          sync.Mutex
	terminated  bool
}

func newProducer(config ProducerConfig) *producer {
	if config.Transformer == nil {
		config.Transformer = defaultTransformer()
	}

	if config.Concurrency == 0 {
		config.Concurrency = 1
	}

	if config.BufferSize == 0 {
		config.BufferSize = defaultBufferSize
	}

	return &producer{
		Id:          uuid.New().String(),
		Producer:    config.Producer,
		config:      config,
		requests:    newConcurrentQueue[*request](),
		requestChan: make(chan *request),
		mu:          sync.Mutex{},
	}
}

func (p *producer) Run(ctx context.Context) <-chan any {
	onTerminated := make(chan any)

	go func() {
		defer func() {
			r := recover()

			if r != nil {
				fmt.Println("producer panicked:", r)
				p.Terminate()
			}

			if requests, ok := p.requests.DequeueAll(); ok {
				for _, request := range requests {
					request.Close()
				}
			}

			onTerminated <- r
			close(onTerminated)
		}()

		for request := range p.requestChan {
			p.requests.Enqueue(request)
		}
	}()

	go p.processRequests(ctx)

	return onTerminated
}

func (p *producer) Send(request *request) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.terminated {
		return false
	}

	p.requestChan <- request

	return true
}

func (p *producer) processRequests(ctx context.Context) {
	for {
		if p.terminated {
			return
		}

		<-time.After(time.Millisecond * 500)

		if p.requests.IsEmpty() {
			return
		}

		p.requests = p.requests.Filter(func(r *request) bool {
			return !r.Closed()
		})

		totalDemand := 0
		p.requests.Each(func(request *request) {
			totalDemand += request.Demand
		})

		if totalDemand == 0 {
			return
		}

		records := p.HandleDemand(totalDemand)
		messages := lo.Map(records, func(record MessagePayload, _ int) *Message {
			return p.config.Transformer(record)
		})

		for {
			if len(messages) == 0 {
				return
			}

			request, ok := p.requests.Dequeue()

			if !ok {
				return
			}

			demand := min(request.Demand, len(messages))
			ok = request.Send(messages[:demand])

			if !ok {
				continue
			}

			messages = messages[demand:]
			request.Demand -= demand

			if request.Demand == 0 {
				request.Close()
			} else {
				p.requests.Prepend(request)
			}
		}
	}
}

func (p *producer) Terminate() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.terminated {
		return
	}

	p.terminated = true
	close(p.requestChan)
}
