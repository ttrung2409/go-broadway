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

	// Clone creates a new instance of the producer.
	// This method is used to create multiple instances of the producer
	// for concurrent processing.
	//
	// Returns:
	//   - A new instance of the producer
	Clone() Producer
}

type messageProcessorResolver func(partitionKey string) (string, bool)

type producer struct {
	id       string
	producer Producer
	config   ProducerConfig

	_requests                 *concurrentSlice[*request]
	_requestChan              chan *request
	_mu                       sync.Mutex
	_terminated               bool
	_idle                     bool
	_messageProcessorResolver messageProcessorResolver
	_messageAck               Acknowledger
	_idleCh                   chan bool
	_terminatedCh             chan any
}

func newProducer(
	handler Producer,
	config ProducerConfig,
	messageProcessorResolver messageProcessorResolver,
	messageAck Acknowledger,
) *producer {

	if config.Concurrency == 0 {
		config.Concurrency = 1
	}

	return &producer{
		id:                        uuid.NewString(),
		producer:                  handler.Clone(),
		config:                    config,
		_requests:                 newConcurrentSlice[*request](),
		_requestChan:              make(chan *request),
		_mu:                       sync.Mutex{},
		_messageProcessorResolver: messageProcessorResolver,
		_messageAck:               messageAck,
		_idleCh:                   make(chan bool),
		_terminatedCh:             make(chan any),
	}
}

// Run starts the producer with the provided context in a goroutine.
//
// Parameters:
//   - ctx: The context provided when starting the pipeline.
func (p *producer) run(ctx context.Context) {

	go func() {
		for request := range p._requestChan {
			p._requests.add(request)
		}
	}()

	go func() {
		defer func() {
			r := recover()

			if r != nil {
				fmt.Printf("producer panicked: %v\n", r)
				p.terminate()
			}

			for _, request := range p._requests.toSlice() {
				request.close()
			}

			p._terminatedCh <- r
			close(p._terminatedCh)
		}()

		p.processRequests(ctx)
	}()

}

// send submits a request to the producer for processing.
//
// Parameters:
//   - request: The request containing demand information from a message processor
//
// Returns:
//   - true if the request was sent successfully, otherwise false
func (p *producer) send(request *request) bool {
	p._mu.Lock()
	defer p._mu.Unlock()

	if p._terminated {
		return false
	}

	p._requestChan <- request

	return true
}

// processRequests continuously processes incoming requests by generating messages
// in response to demand and sending them to the appropriate message processors.
func (p *producer) processRequests(ctx context.Context) {

	for {

		if p._terminated {
			return
		}

		if p._requests.len() == 0 {
			continue
		}

		p._requests = p._requests.filter(func(r *request) bool {
			return !r.isClosed()
		})

		totalDemand := 0
		for _, request := range p._requests.toSlice() {
			totalDemand += request.demand
		}

		if totalDemand == 0 {
			continue
		}

		payloads := make([]MessagePayload, 0)
		if !p._terminated {
			payloads = p.producer.HandleDemand(totalDemand, ctx)
		}

		messages := lo.Map(payloads, func(payload MessagePayload, _ int) *Message {
			partitionKey := ""
			if p.config.partitionKeyResolver != nil {
				partitionKey = p.config.partitionKeyResolver(payload)
			}

			message := newMessage(
				messageArgs{payload: payload, ack: p._messageAck, partitionKey: partitionKey},
			)

			return message
		})

		if p.config.partitionKeyResolver != nil {
			p.sendPartitionedMessages(messages)
		} else {
			p.sendMessages(messages)
		}

		p._idle = len(messages) == 0

		if p._idle {
			select {
			case p._idleCh <- true: // sent successfully
			default: // No receiver, ignore
			}
		}

		<-time.After(time.Millisecond * 100)
	}
}

// sendMessages distributes messages to message processors in a round-robin fashion,
// without regard to partition keys.
func (p *producer) sendMessages(messages []*Message) {

	for {
		if len(messages) == 0 {
			return
		}

		for _, request := range p._requests.toSlice() {
			if len(messages) == 0 {
				break
			}

			demand := min(request.demand, len(messages))
			ok := request.reply(messages[:demand])

			if !ok {
				if request.isClosed() {
					p._requests.remove(request)
				}

				continue
			}

			messages = messages[demand:]
			request.demand -= demand
		}

		for _, r := range p._requests.toSlice() {
			if r.demand <= 0 {
				r.close()
				p._requests.remove(r)
			}
		}

		<-time.After(time.Millisecond * 100)
	}

}

// sendPartitionedMessages distributes messages to message processors based on their
// partition keys. Messages with the same partition key are guaranteed to be processed
// by the same message processor. This ensures ordering of related messages.
func (p *producer) sendPartitionedMessages(messages []*Message) {

	partitionedMessages := lo.GroupBy(messages, func(message *Message) string {
		return message.PartitionKey
	})

	for {

		if len(partitionedMessages) == 0 {
			return
		}

		for partitionKey, partition := range partitionedMessages {
			processorId, ok := p._messageProcessorResolver(partitionKey)
			if !ok {
				continue
			}

			request, ok := lo.Find(p._requests.toSlice(), func(r *request) bool {
				return r.messageProcessorId == processorId
			})

			if !ok {
				continue
			}

			ok = request.reply(partition)
			if !ok {
				if request.isClosed() {
					p._requests.remove(request)
				}

				continue
			}

			request.demand -= len(partition)

			delete(partitionedMessages, partitionKey)
		}

		for _, r := range p._requests.toSlice() {
			if r.demand <= 0 {
				r.close()
				p._requests.remove(r)
			}
		}

		<-time.After(time.Millisecond * 100)
	}

}

// terminate closes pending requests and stops the producer.
// After calling terminate, the producer will no longer accept new requests.
func (p *producer) terminate() {
	p._mu.Lock()
	defer p._mu.Unlock()

	if p._terminated {
		return
	}

	p._terminated = true
	close(p._requestChan)
}

func (p *producer) idle() <-chan bool {
	return p._idleCh
}

func (p *producer) terminated() <-chan any {
	return p._terminatedCh
}

func (p *producer) isIdle() bool {
	return p._idle
}

func (p *producer) isTerminated() bool {
	return p._terminated
}
