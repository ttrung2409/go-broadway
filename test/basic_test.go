package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/ttrung2409/go-broadway/broadway"
)

type basicTestMessage struct {
	Id         string
	UserId     string
	ShouldFail bool
}

type basicTestProducer struct {
	totalFailed   int
	producedCount int
	failedCount   int
	fixedDemand   int
}

func (p *basicTestProducer) New() broadway.Producer {
	return &basicTestProducer{totalFailed: p.totalFailed, fixedDemand: p.fixedDemand}
}

const basicTestTotalMessages = 100
const basicTestTotalUsers = 10

func (p *basicTestProducer) HandleDemand(demand int) []broadway.MessagePayload {

	result := make([]broadway.MessagePayload, 0)

	if p.producedCount >= basicTestTotalMessages {
		return []broadway.MessagePayload{}
	}

	demand = min(demand, basicTestTotalMessages-p.producedCount)
	if p.fixedDemand > 0 {
		demand = p.fixedDemand
	}

	userIds := make([]string, 0)
	for i := 0; i < basicTestTotalUsers; i++ {
		userIds = append(userIds, uuid.NewString())
	}

	for i := 0; i < demand; i++ {
		failed := p.producedCount%2 == 0

		result = append(
			result,
			basicTestMessage{
				Id:         fmt.Sprintf("message %d", p.producedCount),
				UserId:     userIds[p.producedCount%basicTestTotalUsers],
				ShouldFail: p.failedCount < p.totalFailed && failed,
			},
		)

		if p.failedCount < p.totalFailed && failed {
			p.failedCount++
		}

		p.producedCount++

	}

	return result
}

type basicTestMessageProcessor struct{}

func (p *basicTestMessageProcessor) New() broadway.MessageProcessor {
	return &basicTestMessageProcessor{}
}

func (p *basicTestMessageProcessor) Handle(
	message *broadway.Message,
	ctx context.Context,
) (*broadway.Message, error) {
	if message.Payload.(basicTestMessage).ShouldFail {
		return nil, fmt.Errorf(
			"processing failed for message: %s",
			message.Payload.(basicTestMessage).Id,
		)
	}

	message.BatchKey = message.Payload.(basicTestMessage).UserId

	return message, nil
}

type basicTestBatchProcessor struct{}

func (p *basicTestBatchProcessor) New() broadway.BatchProcessor {
	return &basicTestBatchProcessor{}
}

func (p *basicTestBatchProcessor) Handle(
	messages []*broadway.Message,
	ctx context.Context,
) ([]*broadway.Message, error) {
	for _, message := range messages {
		if message.Payload.(basicTestMessage).ShouldFail {
			return nil, fmt.Errorf(
				"batch processing failed for message: %s",
				message.Payload.(basicTestMessage).Id,
			)
		}
	}

	return messages, nil
}

func TestMessagesProperlyProcessedAndAcked(t *testing.T) {
	var (
		successfulCount int
		failedCount     int
		mu              sync.Mutex
	)

	acknowledger := func(messages []*broadway.Message, err error) {
		mu.Lock()
		defer mu.Unlock()

		if err != nil {
			failedCount += len(messages)
		} else {
			successfulCount += len(messages)
		}
	}

	pipeline := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{
			Producer:    &basicTestProducer{totalFailed: basicTestTotalMessages / 2},
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &basicTestMessageProcessor{},
			Concurrency: 5,
			MinDemand:   1,
			MaxDemand:   100,
		},
		Acknowledger: acknowledger,
	})

	ctx, cancel := context.WithCancel(context.Background())

	pipeline.Run(ctx)

	<-pipeline.OnProducerDrained()

	cancel()

	<-pipeline.OnTerminated()

	assert.Equal(t, basicTestTotalMessages/2, successfulCount)
	assert.Equal(t, basicTestTotalMessages/2, failedCount)
}

func TestMessagesProperlyProcessedAndAcked_WithBatching(t *testing.T) {
	var (
		successfulCount int
		failedCount     int
		mu              sync.Mutex
	)

	acknowledger := func(messages []*broadway.Message, err error) {
		mu.Lock()
		defer mu.Unlock()

		if err != nil {
			failedCount += len(messages)
		} else {
			successfulCount += len(messages)
		}
	}

	pipeline := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{
			Producer:    &basicTestProducer{totalFailed: basicTestTotalMessages / 2},
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &basicTestMessageProcessor{},
			Concurrency: 5,
			MinDemand:   1,
			MaxDemand:   100,
		},
		Batchers: []broadway.BatcherConfig{
			{
				Concurrency:  5,
				Processor:    &basicTestBatchProcessor{},
				BatchSize:    10,
				BatchTimeout: time.Second,
			},
		},
		Acknowledger: acknowledger,
	})

	ctx, cancel := context.WithCancel(context.Background())

	pipeline.Run(ctx)

	<-pipeline.OnProducerDrained()

	cancel()

	<-pipeline.OnTerminated()

	assert.Equal(t, basicTestTotalMessages/2, successfulCount)
	assert.Equal(t, basicTestTotalMessages/2, failedCount)
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
			Producer:    &basicTestProducer{fixedDemand: basicTestTotalMessages * 5},
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &basicTestMessageProcessor{},
			Concurrency: 5,
			MinDemand:   1,
			MaxDemand:   100,
		},
		Acknowledger: acknowledger,
	})

	ctx, cancel := context.WithCancel(context.Background())

	pipeline.Run(ctx)

	<-pipeline.OnProducerDrained()

	cancel()

	<-pipeline.OnTerminated()

	assert.Equal(t, basicTestTotalMessages*5, count)
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
			Producer:    &basicTestProducer{fixedDemand: basicTestTotalMessages * 5},
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &basicTestMessageProcessor{},
			Concurrency: 5,
			MinDemand:   1,
			MaxDemand:   100,
		},
		Batchers: []broadway.BatcherConfig{
			{
				Concurrency:  5,
				Processor:    &basicTestBatchProcessor{},
				BatchSize:    10,
				BatchTimeout: time.Second,
			},
		},
		Acknowledger: acknowledger,
	})

	ctx, cancel := context.WithCancel(context.Background())

	pipeline.Run(ctx)

	<-pipeline.OnProducerDrained()

	cancel()

	<-pipeline.OnTerminated()

	assert.Equal(t, basicTestTotalMessages*5, count)
}
