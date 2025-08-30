package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/ttrung2409/go-broadway/broadway"
)

type gracefulShutdownTestMessage struct {
}

type gracefulShutdownTestProducer struct {
	producedCount int
}

func (p *gracefulShutdownTestProducer) Clone() broadway.Producer {
	return &gracefulShutdownTestProducer{}
}

const gracefulShutdownTestTotalMessages = 100

func (p *gracefulShutdownTestProducer) HandleDemand(
	demand int,
	ctx context.Context,
) []broadway.MessagePayload {

	result := make([]broadway.MessagePayload, 0)

	if p.producedCount >= gracefulShutdownTestTotalMessages {
		return []broadway.MessagePayload{}
	}

	for i := 0; i < gracefulShutdownTestTotalMessages*5; i++ {
		result = append(
			result,
			gracefulShutdownTestMessage{},
		)
	}

	p.producedCount += gracefulShutdownTestTotalMessages * 5

	return result
}

type gracefulShutdownTestMessageProcessor struct{}

func (p *gracefulShutdownTestMessageProcessor) Handle(
	message *broadway.Message,
	ctx context.Context,
) (*broadway.Message, error) {
	return message, nil
}

func (p *gracefulShutdownTestMessageProcessor) Clone() broadway.MessageProcessor {
	return &gracefulShutdownTestMessageProcessor{}
}

type gracefulShutdownTestBatchProcessor struct{}

func (p *gracefulShutdownTestBatchProcessor) Clone() broadway.BatchProcessor {
	return &gracefulShutdownTestBatchProcessor{}
}

func (p *gracefulShutdownTestBatchProcessor) Handle(
	messages []*broadway.Message,
	ctx context.Context,
) ([]*broadway.Message, error) {
	return messages, nil
}

func TestGracefulShutdown(t *testing.T) {
	var (
		count int
		mu    sync.Mutex
	)

	acknowledger := func(messages []*broadway.Message, err error) {
		mu.Lock()
		defer mu.Unlock()

		count += len(messages)
	}

	pipeline := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{
			Producer:    &gracefulShutdownTestProducer{},
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &gracefulShutdownTestMessageProcessor{},
			Concurrency: 5,
			MinDemand:   1,
			MaxDemand:   10,
		},
		Acknowledger: acknowledger,
	})

	ctx, cancel := context.WithCancel(context.Background())

	pipeline.Run(ctx)

	<-pipeline.ProducerDrained()

	cancel()

	<-pipeline.Terminated()

	assert.Equal(t, gracefulShutdownTestTotalMessages*5, count, "not all messages were processed")
}

func TestGracefulShutdown_WithBatching(t *testing.T) {
	var (
		count int
		mu    sync.Mutex
	)

	acknowledger := func(messages []*broadway.Message, err error) {
		mu.Lock()
		defer mu.Unlock()

		count += len(messages)
	}

	pipeline := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{
			Producer:    &gracefulShutdownTestProducer{},
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &gracefulShutdownTestMessageProcessor{},
			Concurrency: 5,
			MinDemand:   1,
			MaxDemand:   10,
		},
		Batchers: []broadway.BatcherConfig{
			{
				Concurrency:  5,
				Processor:    &gracefulShutdownTestBatchProcessor{},
				BatchSize:    10,
				BatchTimeout: time.Second,
			},
		},
		Acknowledger: acknowledger,
	})

	ctx, cancel := context.WithCancel(context.Background())

	pipeline.Run(ctx)

	<-pipeline.ProducerDrained()

	cancel()

	<-pipeline.Terminated()

	assert.Equal(t, gracefulShutdownTestTotalMessages*5, count, "not all messages were processed")
}
