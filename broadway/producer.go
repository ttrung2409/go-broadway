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
	Concurrency          int      // Number of concurrent producer workers (default: 1)
	Producer             Producer // The actual implementation that generates message payloads
	partitionKeyResolver PartitionKeyResolver
}

// Producer is the interface that must be implemented when define a pipeline.
// Producers generate message payloads in response to demand from the pipeline.
type Producer interface {
	New() Producer

	// HandleDemand generates message payloads in response to demand from the pipeline.
	//
	// Parameters:
	//   - demand: The number of messages requested by the pipeline.
	//
	// Returns:
	//   - A slice of message payloads
	HandleDemand(demand int) []MessagePayload
}

type messageProcessorResolver func(partitionKey string) (string, bool)

type producer struct {
	Producer
	Id string

	config                   ProducerConfig
	requests                 *concurrentSlice[*request]
	requestChan              chan *request
	messages                 []*Message
	mu                       sync.Mutex
	terminated               bool
	messageProcessorResolver messageProcessorResolver
	messageAck               Acknowledger
}

func newProducer(config ProducerConfig, messageProcessorResolver messageProcessorResolver, messageAck Acknowledger) *producer {

	if config.Concurrency == 0 {
		config.Concurrency = 1
	}

	return &producer{
		Id:                       uuid.New().String(),
		Producer:                 config.Producer.New(),
		config:                   config,
		requests:                 newConcurrentSlice[*request](),
		requestChan:              make(chan *request),
		mu:                       sync.Mutex{},
		messageProcessorResolver: messageProcessorResolver,
		messageAck:               messageAck,
	}
}

func (p *producer) Run(ctx context.Context) <-chan any {
	onTerminated := make(chan any)

	go func() {
		defer func() {
			r := recover()

			if r != nil {
				fmt.Printf("producer panicked: %v\n", r)
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
			continue
		}

		p.requests = p.requests.Filter(func(r *request) bool {
			return !r.IsClosed()
		})

		totalDemand := 0
		p.requests.ForEach(func(request *request) {
			totalDemand += request.Demand
		})

		if totalDemand == 0 {
			continue
		}

		var messages []*Message

		payloads := p.HandleDemand(totalDemand)
		messages = lo.Map(payloads, func(payload MessagePayload, _ int) *Message {
			partitionKey := ""
			if p.config.partitionKeyResolver != nil {
				partitionKey = p.config.partitionKeyResolver(payload)
			}

			message := newMessage(messageArgs{payload: payload, ack: p.messageAck, partitionKey: partitionKey})

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
		ok = request.Reply(p.messages[:demand])

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
		return message.PartitionKey()
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

		fmt.Printf("Sending %d messages to processor '%s'\n", len(partition), processorId)

		ok = request.Reply(partition)
		if !ok {
			continue
		}

		request.Demand -= len(partition)
		p.messages = lo.Filter(p.messages, func(m *Message, _ int) bool {
			return !lo.Contains(partition, m)
		})

		if request.Demand <= 0 {
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
