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

// MessageProcessorConfig defines the configuration for message processors
// that handle individual messages in the Broadway pipeline.
type MessageProcessorConfig struct {
	Concurrency int              // Number of concurrent message processor workers (default: 1)
	Processor   MessageProcessor // The processor implementation that processes messages
	MinDemand   int              // Minimum number of messages to request from producers (default: 5)
	MaxDemand   int              // Maximum number of messages to request from producers (default: 10)
}

// MessageProcessor is the interface that must be implemented by components
// that process individual messages. It defines a single method for handling
// individual messages with a context.
type MessageProcessor interface {
	New() MessageProcessor

	// Handle processes a single message and returns the processed message
	// along with any error that occurred during processing. The implementation
	// should be safe for concurrent use.
	//
	// Parameters:
	//   - message: The message to process.
	//   - ctx: The context for the processing operation. Can be used to handle timeouts or cancellation.
	//
	// Returns:
	//   - The processed message, or nil if the message should be discarded.
	//   - An error if processing failed, or nil if successful.
	Handle(message *Message, ctx context.Context) (*Message, error)
}

type messageProcessor struct {
	MessageProcessor
	Id string

	config          MessageProcessorConfig
	messages        *concurrentQueue[*Message]
	pendingRequests *concurrentMap[string, *request]
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
		MessageProcessor: config.Processor.New(),
		config:           config,
		messages:         newConcurrentQueue[*Message](),
		pendingRequests:  newConcurrentMap[string, *request](),
		mu:               sync.RWMutex{},
		producers:        producers,
		batchers:         batchers,
	}
}

// Run starts the message processor with the provided context.
// It processes messages from producers and forwards them to batchers.
// Returns a channel that will receive a value when the processor terminates,
// either due to context cancellation or an error.
//
// Parameters:
//   - ctx: The context that controls the lifecycle of the processor. When cancelled,
//     the processor will shut down gracefully.
//
// Returns:
//   - A channel that will receive a value (possibly an error) when the processor terminates.
func (p *messageProcessor) Run(ctx context.Context) <-chan any {

	onTerminated := make(chan any)

	go func() {
		defer func() {
			r := recover()

			if r != nil {
				fmt.Printf("message processor panicked: %v\n", r)
				p.Terminate()
			} else {
				p.flush(ctx)
			}

			p.pendingRequests.ForEach(func(_ string, request *request) bool {
				request.Close()

				return true
			})

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

// Terminate stops the message processor and releases all resources.
// After calling Terminate, the processor will no longer process new messages.
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
			fmt.Printf("message processor panicked: %v\n", r)
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

		if processedMessage.PartitionKey() != "" {
			processedMessage.BatchKey = processedMessage.PartitionKey()
		}

		if (p.batchers == nil || err != nil) && processedMessage.Ack() != nil {
			processedMessage.Ack()([]*Message{processedMessage}, err)
		}

		return processedMessage
	})

	if p.batchers != nil {
		messagesByBatchers := lo.GroupBy(messages, func(message *Message) string {
			return message.Batcher
		})

		for batcherName, messagesInBatcher := range messagesByBatchers {
			if batcher, ok := p.batchers[batcherName]; ok {
				batcher.Send(messagesInBatcher)
			}
		}
	}
}

func (p *messageProcessor) request() {

	demand := p.config.MaxDemand - p.config.MinDemand

	for producerId, producer := range p.producers {
		if r, ok := p.pendingRequests.Get(producerId); ok {
			if r.IsClosed() {
				p.pendingRequests.Delete(producerId)
			} else {
				continue
			}
		}

		fmt.Printf("requesting %d messages \n", demand)

		r := newRequest(p.Id, demand)

		if ok := producer.Send(r); !ok {
			continue
		}

		p.pendingRequests.Set(producerId, r)

		go func(r *request, producerId string) {
			for messages := range r.Response {
				p.messages.Enqueue(messages...)
			}

			p.pendingRequests.Delete(producerId)
		}(r, producerId)
	}
}

func (p *messageProcessor) SetProducers(producers map[string]*producer) {
	p.producers = producers
}

func (p *messageProcessor) SetBatchers(batchers map[string]*batcher) {
	p.batchers = batchers
}

// ToString returns a string representation of this message processor.
// This method is used by the hash ring for consistent hashing.
//
// Returns:
//   - The unique ID of the message processor.
func (p *messageProcessor) ToString() string {
	return p.Id
}
