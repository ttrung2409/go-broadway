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
	defaultMinDemand = 10
	defaultMaxDemand = 100
)

type MessageProcessorConfig struct {
	Concurrency int              // Number of concurrent message processor workers (default: 1)
	Processor   MessageProcessor // The processor implementation that processes messages
	MinDemand   int              // Minimum number of messages to request from producers (default: 10)
	MaxDemand   int              // Maximum number of messages to request from producers (default: 100)
}

type MessageProcessor interface {
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

	Clone() MessageProcessor
}

type messageProcessor struct {
	id        string
	processor MessageProcessor
	config    MessageProcessorConfig

	_messages        *concurrentQueue[*Message]
	_pendingRequests *concurrentMap[string, *request]
	_producers       *concurrentMap[string, *producer]
	_batchers        *concurrentMap[string, *batcher]
	_paused          bool
	_terminated      bool
	_mu              sync.RWMutex
	_terminatedCh    chan any
}

func newMessageProcessor(
	processor MessageProcessor,
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
		id:               uuid.NewString(),
		processor:        processor.Clone(),
		config:           config,
		_messages:        newConcurrentQueue[*Message](),
		_pendingRequests: newConcurrentMap[string, *request](),
		_mu:              sync.RWMutex{},
		_producers:       newConcurrentMap(producers),
		_batchers:        newConcurrentMap(batchers),
		_terminatedCh:    make(chan any),
	}
}

// Run starts the message processor with the provided context.
// It processes messages from producers and forwards them to batchers.
//
// Parameters:
//   - ctx: The context provided when starting the pipeline.
func (p *messageProcessor) run(ctx context.Context) {

	go func() {
		ongoingMessages := []*Message{}

		defer func() {
			if len(ongoingMessages) > 0 {
				unackedMessages := lo.Filter(ongoingMessages, func(message *Message, _ int) bool {
					return !message.acked
				})

				p._messages.prepend(unackedMessages...)
			}

			r := recover()

			if r != nil {
				fmt.Printf("message processor panicked: %v\n", r)
				fmt.Println(string(debug.Stack()))

				p.flushAndFailAll(r)
				p.terminate()
			} else {
				p.flush(ctx)
			}

			for _, request := range p._pendingRequests.values() {
				request.close()
			}

			p._terminatedCh <- r
			close(p._terminatedCh)
			p._terminatedCh = nil
		}()

		for {
			ongoingMessages = []*Message{}

			if p._terminated {
				return
			}

			if p._paused {
				time.Sleep(time.Millisecond * 100)
				continue
			}

			if p._messages.len() < p.config.MinDemand {
				p.request(p._producers.values()...)
			}

			total := min(p.config.MaxDemand-p.config.MinDemand, p._messages.len())
			messages, ok := p._messages.dequeue(total)

			if !ok {
				time.Sleep(time.Millisecond * 100)
				continue
			}

			ongoingMessages = messages
			ongoingMessages = p.process(ongoingMessages, ctx)
		}
	}()

}

// terminate flushes all remaining messages in the queue and stop the message processor.
// After calling terminate, the processor will no longer process new messages.
func (p *messageProcessor) terminate() {
	p._mu.Lock()
	defer p._mu.Unlock()

	if p._terminated {
		return
	}

	p._terminated = true
}

// pause temporarily stops the message processor from accepting new messages.
func (p *messageProcessor) pause() {
	p._mu.Lock()
	defer p._mu.Unlock()

	if p._paused {
		return
	}

	for _, request := range p._pendingRequests.values() {
		request.close()
	}

	p._paused = true
}

// resume allows the message processor to accept new messages after being paused.
func (p *messageProcessor) resume() {
	p._mu.Lock()
	defer p._mu.Unlock()
	p._paused = false
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

	if messages, ok := p._messages.dequeueAll(); ok {
		p.process(messages, ctx)
	}
}

func (p *messageProcessor) flushAndFailAll(r any) {
	messages := p._messages.toSlice()
	if len(messages) == 0 {
		return
	}

	if ack := messages[0].ack; ack != nil {
		ack(messages, fmt.Errorf("message processor %s panicked %v", p.id, r))
	}
}

// process handles a batch of messages by passing messages to the configured processor.
// It then routes them to the appropriate batchers according to their batcher and batch key,
// or acknowledges them if no batcher is defined.
//
// Parameters:
//   - messages: A slice of messages to be processed.
//   - ctx: The context provided when starting the pipeline.
func (p *messageProcessor) process(messages []*Message, ctx context.Context) []*Message {
	hasBatchers := p._batchers.len() > 0

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
			processedMessage.acked = true
			processedMessage.error = err
		}

		return processedMessage
	})

	if hasBatchers {
		messages = p.sendToBatcher(
			lo.Filter(messages, func(message *Message, _ int) bool {
				return message.error == nil
			}),
		)
	}

	return messages
}

func (p *messageProcessor) sendToBatcher(messages []*Message) []*Message {
	messagesByBatchers := lo.GroupBy(messages, func(message *Message) string {
		return message.Batcher
	})

	const maxAttempts = 10

	for batcherName, messagesInBatcher := range messagesByBatchers {
		if batcher, ok := p._batchers.get(batcherName); ok {
			attempts := 0

			for {
				if attempts > maxAttempts {
					error := fmt.Errorf(
						"failed to send messages to batcher %s after %d attempts",
						batcherName,
						maxAttempts,
					)

					if ack := messagesInBatcher[0].ack; ack != nil {
						ack(messagesInBatcher, error)
					}

					for _, message := range messagesInBatcher {
						message.acked = true
						message.error = error
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

	return messages
}

// request sends requests to the producers to obtain more messages.
func (p *messageProcessor) request(producers ...*producer) {

	demand := p.config.MaxDemand - p.config.MinDemand

	for _, producer := range producers {
		if r, ok := p._pendingRequests.get(producer.id); ok {
			if r.isClosed() {
				p._pendingRequests.delete(producer.id)
			} else {
				continue
			}
		}

		r := newRequest(p.id, demand)

		if ok := producer.send(r); !ok {
			continue
		}

		p._pendingRequests.set(producer.id, r)

		go func(r *request, producerId string) {
			for messages := range r.response() {
				p._messages.enqueue(messages...)
			}

			p._pendingRequests.delete(producerId)
		}(r, producer.id)
	}
}

// setProducers assigns a map of producers to this message processor.
// The message processor will request messages from these producers
//
// Parameters:
//   - producers: A map of producer IDs to producer instances.
func (p *messageProcessor) setProducers(producers map[string]*producer) {
	p._producers.reset(producers)
}

// setBatchers assigns a map of batchers to this message processor.
// Processed messages will be sent to these batchers for further processing.
//
// Parameters:
//   - batchers: A map of batcher names to batcher instances.
func (p *messageProcessor) setBatchers(batchers map[string]*batcher) {
	p._batchers.reset(batchers)
}

// toString returns a string representation of this message processor.
func (p *messageProcessor) toString() string {
	return p.id
}

func (p *messageProcessor) terminated() <-chan any {
	return p._terminatedCh
}

func (p *messageProcessor) isTerminated() bool {
	return p._terminatedCh == nil
}
