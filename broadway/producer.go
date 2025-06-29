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
	mu                       sync.Mutex
	terminated               bool
	drained                  bool
	messageProcessorResolver messageProcessorResolver
	messageAck               Acknowledger
	onDrained                chan bool
	onTerminated             chan any
}

func newProducer(
	config ProducerConfig,
	messageProcessorResolver messageProcessorResolver,
	messageAck Acknowledger,
) *producer {

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
		onDrained:                make(chan bool),
		onTerminated:             make(chan any),
	}
}

// Run starts the producer with the provided context in a goroutine.
//
// Parameters:
//   - ctx: The context provided when starting the pipeline.
func (p *producer) Run(ctx context.Context) {

	go func() {
		defer func() {
			r := recover()

			if r != nil {
				fmt.Printf("producer panicked: %v\n", r)
				p.Terminate()
			}

			for _, request := range p.requests.ToSlice() {
				request.Close()
			}

			p.onTerminated <- r
			close(p.onTerminated)
		}()

		for request := range p.requestChan {
			p.requests.Add(request)
		}
	}()

	p.processRequests()

}

// Send submits a request to the producer for processing.
//
// Parameters:
//   - request: The request containing demand information from a message processor
//
// Returns:
//   - true if the request was sent successfully, otherwise false
func (p *producer) Send(request *request) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.terminated {
		return false
	}

	p.requestChan <- request

	return true
}

// processRequests continuously processes incoming requests by generating messages
// in response to demand and sending them to the appropriate message processors.
func (p *producer) processRequests() {
	go func() {

		for {

			if p.terminated {
				return
			}

			if p.requests.Len() == 0 {
				continue
			}

			p.requests = p.requests.Filter(func(r *request) bool {
				return !r.IsClosed()
			})

			totalDemand := 0
			for _, request := range p.requests.ToSlice() {
				totalDemand += request.Demand
			}

			if totalDemand == 0 {
				continue
			}

			payloads := make([]MessagePayload, 0)
			if !p.terminated {
				payloads = p.HandleDemand(totalDemand)
			}

			messages := lo.Map(payloads, func(payload MessagePayload, _ int) *Message {
				partitionKey := ""
				if p.config.partitionKeyResolver != nil {
					partitionKey = p.config.partitionKeyResolver(payload)
				}

				message := newMessage(
					messageArgs{payload: payload, ack: p.messageAck, partitionKey: partitionKey},
				)

				return message
			})

			if p.config.partitionKeyResolver != nil {
				p.sendPartitionedMessages(messages)
			} else {
				p.sendMessages(messages)
			}

			p.drained = len(messages) == 0

			if p.drained {
				select {
				case p.onDrained <- true: // sent successfully
				default: // No receiver, ignore
				}
			}

			<-time.After(time.Millisecond * 100)
		}
	}()
}

// sendMessages distributes messages to message processors in a round-robin fashion,
// without regard to partition keys.
func (p *producer) sendMessages(messages []*Message) {
	type augmentedRequest struct {
		request        *request
		originalDemand int
		fullfilled     bool
	}

	requests := lo.Map(p.requests.ToSlice(), func(r *request, _ int) *augmentedRequest {
		return &augmentedRequest{
			request:        r,
			originalDemand: r.Demand,
		}
	})

	index := 0

	for {
		if len(messages) == 0 {
			break
		}

		if index == len(requests) {
			index = 0
		}

		r := requests[index]

		demand := min(r.request.Demand, len(messages))
		ok := r.request.Reply(messages[:demand])

		if !ok {
			continue
		}

		messages = messages[demand:]
		r.request.Demand -= demand

		// If there is no demand left, the request is considered fulfilled.
		// Reset the demand to its original value so that the request can
		// still accept more messages in case the messages are not fully consumed
		// by other message processors.
		if r.request.Demand == 0 {
			r.request.Demand = r.originalDemand
			r.fullfilled = true
		}

		index++
	}

	for _, r := range requests {
		if r.fullfilled {
			r.request.Close()
			p.requests.Remove(r.request)
		}
	}

}

// sendPartitionedMessages distributes messages to message processors based on their
// partition keys. Messages with the same partition key are guaranteed to be processed
// by the same message processor. This ensures ordering of related messages.
func (p *producer) sendPartitionedMessages(messages []*Message) {
	partitionedMessages := lo.GroupBy(messages, func(message *Message) string {
		return message.PartitionKey()
	})

	for partitionKey, partition := range partitionedMessages {
		processorId, ok := p.messageProcessorResolver(partitionKey)
		if !ok {
			continue
		}

		request, ok := lo.Find(p.requests.ToSlice(), func(r *request) bool {
			return r.MessageProcessorId == processorId
		})

		if !ok {
			continue
		}

		ok = request.Reply(partition)
		if !ok {
			continue
		}

		request.Demand -= len(partition)

		if request.Demand <= 0 {
			request.Close()
			p.requests.Remove(request)
		}
	}
}

// Terminate closes pending requests and stops the producer.
// After calling Terminate, the producer will no longer accept new requests.
func (p *producer) Terminate() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.terminated {
		return
	}

	p.terminated = true
	close(p.requestChan)
}

func (p *producer) OnDrained() <-chan bool {
	return p.onDrained
}

func (p *producer) OnTerminated() <-chan any {
	return p.onTerminated
}

func (p *producer) IsDrained() bool {
	return p.drained
}

func (p *producer) IsTerminated() bool {
	return p.terminated
}
