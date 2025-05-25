package broadway

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
)

// ProducerConfig defines the configuration for a message producer.
type ProducerConfig struct {
	Concurrency          int         // Number of concurrent producer workers (default: 1)
	Transformer          Transformer // Transforms raw message payloads into Messages (default: a transformer that wraps payload in a message)
	Producer             Producer    // The actual implementation that generates message payloads
	partitionKeyResolver PartitionKeyResolver
}

// Producer is the interface that must be implemented when define a pipeline.
// Producers generate message payloads in response to demand from the pipeline.
type Producer interface {
	// HandleDemand generates message payloads in response to demand from the pipeline.
	//
	// Parameters:
	//   - demand: The number of messages requested by the pipeline.
	//
	// Returns:
	//   - A slice of message payloads that will be transformed into Messages.
	HandleDemand(demand int) []MessagePayload
}

type messageProcessorResolver func(partitionKey string) (string, bool)

type producer struct {
	Producer
	Id                       string
	config                   ProducerConfig
	requests                 *concurrentSlice[*request]
	requestChan              chan *request
	mu                       sync.Mutex
	terminated               bool
	messageProcessorResolver messageProcessorResolver
	messages                 []*Message
}

func newProducer(config ProducerConfig, messageProcessorResolver messageProcessorResolver) *producer {
	if config.Transformer == nil {
		config.Transformer = defaultTransformer(config.partitionKeyResolver)
	}

	if config.Concurrency == 0 {
		config.Concurrency = 1
	}

	return &producer{
		Id:                       uuid.New().String(),
		Producer:                 config.Producer,
		config:                   config,
		requests:                 newConcurrentSlice[*request](),
		requestChan:              make(chan *request),
		mu:                       sync.Mutex{},
		messageProcessorResolver: messageProcessorResolver,
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

			requests := p.requests.Drain()
			for _, request := range requests {
				request.Close()
			}

			onTerminated <- r
			close(onTerminated)
		}()

		for request := range p.requestChan {
			p.requests.Add(request)
		}
	}()

	go p.processRequests()

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

func (p *producer) processRequests() {
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

		var messages []*Message

		// Standard non-partitioned processing
		records := p.HandleDemand(totalDemand)
		messages = lo.Map(records, func(record MessagePayload, _ int) *Message {
			message := p.config.Transformer(record)

			if p.config.partitionKeyResolver != nil {
				message.PartitionKey = p.config.partitionKeyResolver(record)
			}

			return message
		})

		p.messages = append(p.messages, messages...)

		if p.config.partitionKeyResolver != nil {
			p.sendPartitionedMessages()
		} else {
			p.sendMessages()
		}

	}
}

func (p *producer) sendMessages() {
	for {
		if len(p.messages) == 0 {
			return
		}

		request, ok := p.requests.Shift()

		if !ok {
			return
		}

		demand := min(request.Demand, len(p.messages))
		ok = request.Send(p.messages[:demand])

		if !ok {
			continue
		}

		p.messages = p.messages[demand:]
		request.Demand -= demand

		if request.Demand == 0 {
			request.Close()
		} else {
			p.requests.Prepend(request)
		}
	}
}

func (p *producer) sendPartitionedMessages() {
	partitionedMessages := lo.GroupBy(p.messages, func(message *Message) string {
		return message.PartitionKey
	})

	for partitionKey, partition := range partitionedMessages {
		processorId, ok := p.messageProcessorResolver(partitionKey)
		if !ok {
			continue
		}

		request, ok := p.requests.Find(func(r *request) bool {
			return r.MessageProcessorId == processorId
		})

		if !ok {
			continue
		}

		demand := min(request.Demand, len(partition))
		ok = request.Send(partition[:demand])
		if !ok {
			continue
		}

		request.Demand -= demand
		p.messages = lo.Filter(p.messages, func(m *Message, _ int) bool {
			return !lo.Contains(partition[:demand], m)
		})

		if request.Demand == 0 {
			request.Close()
			p.requests.Remove(request)
		}
	}

}

// Terminate stops the producer and releases all resources.
// After calling Terminate, the producer will no longer accept new requests.
// This method is thread-safe and idempotent.
func (p *producer) Terminate() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.terminated {
		return
	}

	p.terminated = true
	close(p.requestChan)
}
