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
	buffer                   []*Message
	mu                       sync.Mutex
	terminating              bool
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
				p.terminate()
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

	go func() {
		for {
			if p.terminating && len(p.buffer) == 0 {
				p.terminate()
				return
			}

			time.Sleep(time.Millisecond * 100)
		}
	}()
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

			if p.terminating && len(p.buffer) == 0 {
				return
			}

			if p.requests.IsEmpty() {
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
			if !p.terminating {
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

			p.buffer = append(p.buffer, messages...)

			if p.config.partitionKeyResolver != nil {
				p.sendPartitionedMessages()
			} else {
				p.sendMessages()
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
func (p *producer) sendMessages() {
	for {
		if len(p.buffer) == 0 {
			return
		}

		request, ok := p.requests.Shift()

		if !ok {
			return
		}

		demand := min(request.Demand, len(p.buffer))
		ok = request.Reply(p.buffer[:demand])

		if !ok {
			continue
		}

		p.buffer = p.buffer[demand:]
		request.Demand -= demand

		if request.Demand == 0 {
			request.Close()
		} else {
			p.requests.Prepend(request)
		}
	}
}

// sendPartitionedMessages distributes messages to message processors based on their
// partition keys. Messages with the same partition key are guaranteed to be processed
// by the same message processor. This ensures ordering of related messages.
func (p *producer) sendPartitionedMessages() {
	partitionedMessages := lo.GroupBy(p.buffer, func(message *Message) string {
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

		ok = request.Reply(partition)
		if !ok {
			continue
		}

		request.Demand -= len(partition)
		p.buffer = lo.Filter(p.buffer, func(m *Message, _ int) bool {
			return !lo.Contains(partition, m)
		})

		if request.Demand <= 0 {
			request.Close()
			p.requests.Remove(request)
		}
	}
}

func (p *producer) terminate() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.terminated {
		return
	}

	p.terminated = true
	close(p.requestChan)
}

// Terminate closes pending requests and stops the producer.
// After calling Terminate, the producer will no longer accept new requests.
func (p *producer) Terminate() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.terminating {
		return
	}

	p.terminating = true
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

func (p *producer) BufferCount() int {
	return len(p.buffer)
}
