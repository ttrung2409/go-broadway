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

type MessageProcessor interface {
	New() MessageProcessor

	// Handle processes a single message and returns the processed message
	// along with any error that occurred during processing.
	//
	// Parameters:
	//   - message: The message to be processed.
	//   - ctx: The context provided when starting the pipeline
	//
	// Returns:
	//   - The processed message
	//   - An error if processing failed, or nil if successful.
	Handle(message *Message, ctx context.Context) (*Message, error)
}

type messageProcessor struct {
	MessageProcessor
	Id string

	config          MessageProcessorConfig
	messages        *concurrentQueue[*Message]
	pendingRequests *concurrentMap[string, *request]
	producers       *concurrentMap[string, *producer]
	batchers        *concurrentMap[string, *batcher]
	paused          bool
	terminated      bool
	mu              sync.RWMutex

	onTerminated chan any
}

// newMessageProcessor creates a new message processor with the given configuration,
// producers, and batchers. It initializes the processor and sets default values
// for configuration parameters if they're not provided.
//
// Parameters:
//   - config: Configuration for the message processor
//   - producers: A map of producer IDs to producer instances
//   - batchers: A map of batcher names to batcher instances
//
// Returns:
//   - An initialized message processor
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
		producers:        newConcurrentMap(producers),
		batchers:         newConcurrentMap(batchers),
		onTerminated:     make(chan any),
	}
}

// Run starts the message processor with the provided context.
// It processes messages from producers and forwards them to batchers.
//
// Parameters:
//   - ctx: The context provided when starting the pipeline.
func (p *messageProcessor) Run(ctx context.Context) {

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

			p.onTerminated <- r
			close(p.onTerminated)
			p.onTerminated = nil
		}()

		for {
			if p.terminated {
				return
			}

			if p.messages.Len() < p.config.MinDemand {
				p.request()
			}

			total := min((p.config.MaxDemand+p.config.MinDemand)/2, p.messages.Len())
			messages, ok := p.messages.DequeueMany(total)

			if !ok {
				time.Sleep(time.Millisecond * 100)
				continue
			}

			p.process(messages, ctx)
		}
	}()

}

// Terminate flushes all remaining messages in the queue and stop the message processor.
// After calling Terminate, the processor will no longer process new messages.
func (p *messageProcessor) Terminate() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.terminated {
		return
	}

	p.terminated = true
}

func (p *messageProcessor) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
}

// Resume allows the batch processor to accept new batches after being paused.
func (p *messageProcessor) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
}

// flush processes any remaining messages in the queue before shutdown.
// This ensures that all messages received before termination are properly processed.
//
// Parameters:
//   - ctx: The context provided when starting the pipeline.
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

// process handles a batch of messages by passing messages to the configured processor.
// It then routes them to the appropriate batchers according to their batcher and batch key,
// or acknowledges them if no batcher is defined.
//
// Parameters:
//   - messages: A slice of messages to be processed.
//   - ctx: The context provided when starting the pipeline.
func (p *messageProcessor) process(messages []*Message, ctx context.Context) {
	hasBatchers := p.batchers.Len() > 0

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

		if (!hasBatchers || err != nil) && processedMessage.Ack() != nil {
			processedMessage.Ack()([]*Message{processedMessage}, err)
		}

		if err != nil {
			return nil
		}

		return processedMessage
	})

	messages = lo.Filter(messages, func(message *Message, _ int) bool {
		return message != nil
	})

	if hasBatchers {

		messagesByBatchers := lo.GroupBy(messages, func(message *Message) string {
			return message.Batcher
		})

		for batcherName, messagesInBatcher := range messagesByBatchers {
			if batcher, ok := p.batchers.Get(batcherName); ok {
				batcher.Send(messagesInBatcher)
			}
		}
	}
}

// request sends requests to the producers to obtain more messages.
func (p *messageProcessor) request() {

	demand := p.config.MaxDemand - p.config.MinDemand

	for producerId, producer := range p.producers.ToMap() {
		if r, ok := p.pendingRequests.Get(producerId); ok {
			if r.IsClosed() {
				p.pendingRequests.Delete(producerId)
			} else {
				continue
			}
		}

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

// SetProducers assigns a map of producers to this message processor.
// The message processor will request messages from these producers
//
// Parameters:
//   - producers: A map of producer IDs to producer instances.
func (p *messageProcessor) SetProducers(producers map[string]*producer) {
	p.producers.Reset(producers)
}

// SetBatchers assigns a map of batchers to this message processor.
// Processed messages will be sent to these batchers for further processing.
//
// Parameters:
//   - batchers: A map of batcher names to batcher instances.
func (p *messageProcessor) SetBatchers(batchers map[string]*batcher) {
	p.batchers.Reset(batchers)
}

// ToString returns a string representation of this message processor.
// This method is used by the hash ring for consistent hashing.
//
// Returns:
//   - The unique ID of the message processor.
func (p *messageProcessor) ToString() string {
	return p.Id
}

func (p *messageProcessor) OnTerminated() <-chan any {
	return p.onTerminated
}

func (p *messageProcessor) IsTerminated() bool {
	return p.onTerminated == nil
}
