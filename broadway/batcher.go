package broadway

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const defaultBatcherName = "default"
const defaultBatchKey = "default"

type BatcherConfig struct {
	Name         string
	BatchSize    int
	BatchTimeout time.Duration
	Concurrency  int
	Processor    BatchProcessor
}

type batcher struct {
	config     BatcherConfig
	messages   map[string]*concurrentQueue[*Message]
	hr         *hashRing[*batchProcessor]
	receiver   chan []*Message
	terminated bool
	mu         sync.Mutex
}

func newBatcher(config BatcherConfig) *batcher {
	if config.Name == "" {
		config.Name = defaultBatcherName
	}

	if config.BatchSize == 0 {
		config.BatchSize = 100
	}

	if config.BatchTimeout == 0 {
		config.BatchTimeout = time.Second
	}

	return &batcher{
		config:   config,
		messages: make(map[string]*concurrentQueue[*Message]),
		hr:       newHashRing[*batchProcessor](),
		receiver: make(chan []*Message),
		mu:       sync.Mutex{},
	}
}

func (b *batcher) Run(ctx context.Context) <-chan any {
	onTerminated := make(chan any)

	for i := 0; i < b.config.Concurrency; i++ {
		processor := newBatchProcessor(b.config.Processor)
		onTerminated := processor.Run(ctx)
		b.hr.AddNode(processor)

		go func(processor *batchProcessor, onTerminated <-chan any) {
			err := <-onTerminated

			if err != nil {
				b.handleProcessorPanic(processor, ctx)
			}
		}(processor, onTerminated)
	}

	go func() {
		defer func() {
			r := recover()

			if r != nil {
				fmt.Println("batcher panicked:", r)
				b.Terminate()
			} else {
				b.flush()
			}

			processors := b.hr.GetAllNodes()
			for _, processor := range processors {
				processor.Terminate()
			}

			onTerminated <- r
			close(onTerminated)
		}()

		for messages := range b.receiver {
			for _, message := range messages {
				if _, ok := b.messages[message.BatchKey]; !ok {
					b.messages[message.BatchKey] = newConcurrentQueue[*Message]()
				}

				b.messages[message.BatchKey].Enqueue(message)
			}
		}
	}()

	go b.processBatches(ctx)

	return onTerminated
}

func (b *batcher) Terminate() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.terminated {
		return
	}

	b.terminated = true
	close(b.receiver)
}

func (b *batcher) Send(messages []*Message) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.terminated {
		return false
	}

	b.receiver <- messages

	return true
}

func (b *batcher) flush() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("batcher panicked:", r)
		}
	}()

	for {
		for batchKey, messages := range b.messages {
			if batch, ok := messages.DequeueAll(); ok {
				if b.processBatch(batchKey, batch) {
					delete(b.messages, batchKey)
				} else {
					messages.Prepend(batch...)
				}
			}
		}

		if len(b.messages) == 0 {
			return
		}

		time.Sleep(time.Millisecond * 500)
	}
}

func (b *batcher) processBatches(ctx context.Context) {
	for {
		if b.terminated {
			return
		}

		select {
		case <-time.After(b.config.BatchTimeout):
			for batchKey, messages := range b.messages {
				if batch, ok := messages.DequeueMany(b.config.BatchSize); ok {
					if !b.processBatch(batchKey, batch) {
						messages.Prepend(batch...)
					}
				}
			}

		default:
			for batchKey, messages := range b.messages {
				if messages.Len() < b.config.BatchSize {
					continue
				}

				if batch, ok := messages.DequeueMany(b.config.BatchSize); ok {
					if !b.processBatch(batchKey, batch) {
						messages.Prepend(batch...)
					}
				}
			}
		}
	}
}

func (b *batcher) processBatch(batchKey string, batch []*Message) bool {
	if processor, ok := b.hr.GetNode(batchKey); ok {
		return processor.Send(batch)
	}

	return false
}

func (b *batcher) handleProcessorPanic(processor *batchProcessor, ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.hr.RemoveNode(processor)

	newProcessor := newBatchProcessor(b.config.Processor)
	onTerminated := newProcessor.Run(ctx)

	keySharingProcessor, _ := b.hr.GetNextNode(newProcessor)

	if keySharingProcessor != nil {
		keySharingProcessor.Pause()
	}

	b.hr.AddNode(newProcessor)

	if keySharingProcessor != nil {
		keySharingProcessor.Resume()
	}

	go func(processor *batchProcessor, onTerminated <-chan any) {
		err := <-onTerminated

		if err != nil {
			b.handleProcessorPanic(processor, ctx)
		}
	}(newProcessor, onTerminated)
}
