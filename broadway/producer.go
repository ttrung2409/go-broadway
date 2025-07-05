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
	// HandleDemand generates message payloads in response to demand from the pipeline.
	//
	// Parameters:
	//   - demand: The number of messages requested by the pipeline.
	//   - ctx: The context provided when starting the pipeline.
	//
	// Returns:
	//   - A slice of message payloads
	HandleDemand(demand int, ctx context.Context) []MessagePayload

	Clone() Producer
}

type producer interface {
	id() string

	// Run starts the producer with the provided context in a goroutine.
	//
	// Parameters:
	//   - ctx: The context provided when starting the pipeline.
	run(ctx context.Context)

	// send submits a request to the producer for processing.
	//
	// Parameters:
	//   - request: The request containing demand information from a message processor
	//
	// Returns:
	//   - true if the request was sent successfully, otherwise false
	send(request *request) bool

	// terminate closes pending requests and stops the producer.
	// After calling terminate, the producer will no longer accept new requests.
	terminate()

	onDrained() <-chan bool
	onTerminated() <-chan any
	isDrained() bool
	isTerminated() bool

	producer() Producer
}

type messageProcessorResolver func(partitionKey string) (string, bool)

type internalProducer struct {
	_id                      string
	_producer                Producer
	config                   ProducerConfig
	requests                 *concurrentSlice[*request]
	requestChan              chan *request
	mu                       sync.Mutex
	terminated               bool
	drained                  bool
	messageProcessorResolver messageProcessorResolver
	messageAck               Acknowledger
	_onDrained               chan bool
	_onTerminated            chan any
}

func newProducer(
	producer Producer,
	config ProducerConfig,
	messageProcessorResolver messageProcessorResolver,
	messageAck Acknowledger,
) producer {

	if config.Concurrency == 0 {
		config.Concurrency = 1
	}

	return &internalProducer{
		_id:                      uuid.NewString(),
		_producer:                producer.Clone(),
		config:                   config,
		requests:                 newConcurrentSlice[*request](),
		requestChan:              make(chan *request),
		mu:                       sync.Mutex{},
		messageProcessorResolver: messageProcessorResolver,
		messageAck:               messageAck,
		_onDrained:               make(chan bool),
		_onTerminated:            make(chan any),
	}
}

func (p *internalProducer) run(ctx context.Context) {

	go func() {
		for request := range p.requestChan {
			p.requests.add(request)
		}
	}()

	go func() {
		defer func() {
			r := recover()

			if r != nil {
				fmt.Printf("producer panicked: %v\n", r)
				p.terminate()
			}

			for _, request := range p.requests.toSlice() {
				request.close()
			}

			p._onTerminated <- r
			close(p._onTerminated)
		}()

		p.processRequests(ctx)
	}()

}

func (p *internalProducer) send(request *request) bool {
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
func (p *internalProducer) processRequests(ctx context.Context) {

	for {

		if p.terminated {
			return
		}

		if p.requests.len() == 0 {
			continue
		}

		p.requests = p.requests.filter(func(r *request) bool {
			return !r.isClosed()
		})

		totalDemand := 0
		for _, request := range p.requests.toSlice() {
			totalDemand += request.demand
		}

		if totalDemand == 0 {
			continue
		}

		payloads := make([]MessagePayload, 0)
		if !p.terminated {
			payloads = p._producer.HandleDemand(totalDemand, ctx)
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
			case p._onDrained <- true: // sent successfully
			default: // No receiver, ignore
			}
		}

		<-time.After(time.Millisecond * 100)
	}
}

// sendMessages distributes messages to message processors in a round-robin fashion,
// without regard to partition keys.
func (p *internalProducer) sendMessages(messages []*Message) {
	type augmentedRequest struct {
		request        *request
		originalDemand int
		fullfilled     bool
	}

	requests := lo.Map(p.requests.toSlice(), func(r *request, _ int) *augmentedRequest {
		return &augmentedRequest{
			request:        r,
			originalDemand: r.demand,
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

		demand := min(r.request.demand, len(messages))
		ok := r.request.reply(messages[:demand])

		if !ok {
			continue
		}

		messages = messages[demand:]
		r.request.demand -= demand

		// If there is no demand left, the request is considered fulfilled.
		// Reset the demand to its original value so that the request can
		// still accept more messages in case the messages are not fully consumed
		// by other message processors.
		if r.request.demand == 0 {
			r.request.demand = r.originalDemand
			r.fullfilled = true
		}

		index++
	}

	for _, r := range requests {
		if r.fullfilled {
			r.request.close()
			p.requests.remove(r.request)
		}
	}

}

// sendPartitionedMessages distributes messages to message processors based on their
// partition keys. Messages with the same partition key are guaranteed to be processed
// by the same message processor. This ensures ordering of related messages.
func (p *internalProducer) sendPartitionedMessages(messages []*Message) {
	partitionedMessages := lo.GroupBy(messages, func(message *Message) string {
		return message.PartitionKey
	})

	for partitionKey, partition := range partitionedMessages {
		processorId, ok := p.messageProcessorResolver(partitionKey)
		if !ok {
			continue
		}

		request, ok := lo.Find(p.requests.toSlice(), func(r *request) bool {
			return r.messageProcessorId == processorId
		})

		if !ok {
			continue
		}

		ok = request.reply(partition)
		if !ok {
			continue
		}

		request.demand -= len(partition)

		if request.demand <= 0 {
			request.close()
			p.requests.remove(request)
		}
	}
}

func (p *internalProducer) terminate() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.terminated {
		return
	}

	p.terminated = true
	close(p.requestChan)
}

func (p *internalProducer) onDrained() <-chan bool {
	return p._onDrained
}

func (p *internalProducer) onTerminated() <-chan any {
	return p._onTerminated
}

func (p *internalProducer) isDrained() bool {
	return p.drained
}

func (p *internalProducer) isTerminated() bool {
	return p.terminated
}

func (p *internalProducer) id() string {
	return p._id
}

func (p *internalProducer) producer() Producer {
	return p._producer
}
