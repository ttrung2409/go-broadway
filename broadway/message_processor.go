package broadway

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
)

const (
	defaultMinDemand = 5
	defaultMaxDemand = 10
)

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

type messageProcessor interface {
	id() string

	// Run starts the message processor with the provided context.
	// It processes messages from producers and forwards them to batchers.
	//
	// Parameters:
	//   - ctx: The context provided when starting the pipeline.
	run(ctx context.Context)

	// terminate flushes all remaining messages in the queue and stop the message processor.
	// After calling terminate, the processor will no longer process new messages.
	terminate()

	// pause temporarily stops the message processor from accepting new messages.
	pause()

	// resume allows the message processor to accept new messages after being paused.
	resume()

	// setProducers assigns a map of producers to this message processor.
	// The message processor will request messages from these producers
	//
	// Parameters:
	//   - producers: A map of producer IDs to producer instances.
	setProducers(producers map[string]producer)

	// setBatchers assigns a map of batchers to this message processor.
	// Processed messages will be sent to these batchers for further processing.
	//
	// Parameters:
	//   - batchers: A map of batcher names to batcher instances.
	setBatchers(batchers map[string]batcher)

	onTerminated() <-chan any
	isTerminated() bool

	// toString returns a string representation of this message processor.
	toString() string
}

type internalMessageProcessor struct {
	_id             string
	processor       MessageProcessor
	config          MessageProcessorConfig
	messages        *concurrentQueue[*Message]
	pendingRequests *concurrentMap[string, *request]
	producers       *concurrentMap[string, producer]
	batchers        *concurrentMap[string, batcher]
	paused          bool
	terminated      bool
	mu              sync.RWMutex
	_onTerminated   chan any
}

func newMessageProcessor(
	config MessageProcessorConfig,
	producers map[string]producer,
	batchers map[string]batcher,
) messageProcessor {
	if config.MinDemand == 0 {
		config.MinDemand = defaultMinDemand
	}

	if config.MaxDemand == 0 {
		config.MaxDemand = defaultMaxDemand
	}

	return &internalMessageProcessor{
		_id:             uuid.New().String(),
		processor:       config.Processor.New(),
		config:          config,
		messages:        newConcurrentQueue[*Message](),
		pendingRequests: newConcurrentMap[string, *request](),
		mu:              sync.RWMutex{},
		producers:       newConcurrentMap(producers),
		batchers:        newConcurrentMap(batchers),
		_onTerminated:   make(chan any),
	}
}

func (p *internalMessageProcessor) run(ctx context.Context) {

	go func() {
		defer func() {
			r := recover()

			if r != nil {
				fmt.Printf("message processor panicked: %v\n", r)
				fmt.Println(string(debug.Stack()))

				p.flushAndFailAll(r)
				p.terminate()
			} else {
				p.flush(ctx)
			}

			for _, request := range p.pendingRequests.values() {
				request.close()
			}

			p._onTerminated <- r
			close(p._onTerminated)
			p._onTerminated = nil
		}()

		for {
			if p.terminated {
				return
			}

			if p.messages.len() < p.config.MinDemand {
				p.request(p.producers.values()...)
			}

			total := min((p.config.MaxDemand+p.config.MinDemand)/2, p.messages.len())
			messages, ok := p.messages.dequeue(total)

			if !ok {
				time.Sleep(time.Millisecond * 100)
				continue
			}

			p.process(messages, ctx)
		}
	}()

}

func (p *internalMessageProcessor) terminate() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.terminated {
		return
	}

	p.terminated = true
}

func (p *internalMessageProcessor) pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
}

func (p *internalMessageProcessor) resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
}

// flush processes any remaining messages in the queue before shutdown.
// This ensures that all messages received before termination are properly processed.
//
// Parameters:
//   - ctx: The context provided when starting the pipeline.
func (p *internalMessageProcessor) flush(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("message processor panicked: %v\n", r)
		}
	}()

	if messages, ok := p.messages.dequeueAll(); ok {
		p.process(messages, ctx)
	}
}

func (p *internalMessageProcessor) flushAndFailAll(r any) {
	messages := p.messages.toSlice()
	if len(messages) == 0 {
		return
	}

	if ack := messages[0].ack; ack != nil {
		ack(messages, fmt.Errorf("message processor %s panicked %v", p._id, r))
	}
}

// process handles a batch of messages by passing messages to the configured processor.
// It then routes them to the appropriate batchers according to their batcher and batch key,
// or acknowledges them if no batcher is defined.
//
// Parameters:
//   - messages: A slice of messages to be processed.
//   - ctx: The context provided when starting the pipeline.
func (p *internalMessageProcessor) process(messages []*Message, ctx context.Context) {
	hasBatchers := p.batchers.len() > 0

	messages = lo.Map(messages, func(message *Message, _ int) *Message {
		processedMessage, err := p.processor.Handle(message, ctx)

		if processedMessage == nil {
			processedMessage = message
		}

		if processedMessage.Batcher == "" {
			processedMessage.Batcher = defaultBatcherName
		}

		if processedMessage.BatchKey == "" {
			processedMessage.BatchKey = defaultBatchKey
		}

		if processedMessage.PartitionKey != "" {
			processedMessage.BatchKey = processedMessage.PartitionKey
		}

		if (!hasBatchers || err != nil) && processedMessage.ack != nil {
			processedMessage.ack([]*Message{processedMessage}, err)
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
		p.sendToBatcher(messages)
	}
}

func (p *internalMessageProcessor) sendToBatcher(messages []*Message) {
	messagesByBatchers := lo.GroupBy(messages, func(message *Message) string {
		return message.Batcher
	})

	const maxAttempts = 10

	for batcherName, messagesInBatcher := range messagesByBatchers {
		if batcher, ok := p.batchers.get(batcherName); ok {
			attempts := 0

			for {
				if attempts > maxAttempts {
					if ack := messagesInBatcher[0].ack; ack != nil {
						ack(
							messagesInBatcher,
							fmt.Errorf(
								"failed to send messages to batcher %s after %d attempts",
								batcherName,
								maxAttempts,
							),
						)
					}

					break
				}

				if ok := batcher.send(messagesInBatcher); ok {
					break
				}

				attempts++

				time.Sleep(time.Millisecond * 100)
			}
		}
	}
}

// request sends requests to the producers to obtain more messages.
func (p *internalMessageProcessor) request(producers ...producer) {

	demand := p.config.MaxDemand - p.config.MinDemand

	for _, producer := range producers {
		if r, ok := p.pendingRequests.get(producer.id()); ok {
			if r.isClosed() {
				p.pendingRequests.delete(producer.id())
			} else {
				continue
			}
		}

		r := newRequest(p._id, demand)

		if ok := producer.send(r); !ok {
			continue
		}

		p.pendingRequests.set(producer.id(), r)

		go func(r *request, producerId string) {
			for messages := range r.response() {
				p.messages.enqueue(messages...)
			}

			p.pendingRequests.delete(producerId)
		}(r, producer.id())
	}
}

func (p *internalMessageProcessor) setProducers(producers map[string]producer) {
	p.producers.reset(producers)
}

func (p *internalMessageProcessor) setBatchers(batchers map[string]batcher) {
	p.batchers.reset(batchers)
}

func (p *internalMessageProcessor) toString() string {
	return p._id
}

func (p *internalMessageProcessor) onTerminated() <-chan any {
	return p._onTerminated
}

func (p *internalMessageProcessor) isTerminated() bool {
	return p._onTerminated == nil
}

func (p *internalMessageProcessor) id() string {
	return p._id
}
