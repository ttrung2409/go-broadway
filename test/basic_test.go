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
}

func (p *basicTestProducer) Init(_ context.Context) {}

func (p *basicTestProducer) Clone() broadway.Producer {
	return &basicTestProducer{totalFailed: p.totalFailed}
}

const basicTestTotalMessages = 100
const basicTestTotalUsers = 10

func (p *basicTestProducer) HandleDemand(
	demand int,
	ctx context.Context,
) []*broadway.Message {

	result := make([]*broadway.Message, 0)

	if p.producedCount >= basicTestTotalMessages {
		return []*broadway.Message{}
	}

	demand = min(demand, basicTestTotalMessages-p.producedCount)

	userIds := make([]string, 0)
	for i := 0; i < basicTestTotalUsers; i++ {
		userIds = append(userIds, uuid.NewString())
	}

	for i := 0; i < demand; i++ {
		failed := p.producedCount%2 == 0

		result = append(
			result,
			broadway.NewMessage(basicTestMessage{
				Id:         fmt.Sprintf("message %d", p.producedCount),
				UserId:     userIds[p.producedCount%basicTestTotalUsers],
				ShouldFail: p.failedCount < p.totalFailed && failed,
			}, nil),
		)

		if p.failedCount < p.totalFailed && failed {
			p.failedCount++
		}

		p.producedCount++

	}

	return result
}

type basicTestMessageProcessor struct{}

func (p *basicTestMessageProcessor) Clone() broadway.MessageProcessor {
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

func (p *basicTestBatchProcessor) Clone() broadway.BatchProcessor {
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
			MaxDemand:   10,
		},
		Acknowledger: acknowledger,
	})

	ctx, cancel := context.WithCancel(context.Background())

	pipeline.Run(ctx)

	<-pipeline.ProducerIdle()

	cancel()

	<-pipeline.Terminated()

	assert.Equal(t, basicTestTotalMessages/2, successfulCount, "successful messages are missing")
	assert.Equal(t, basicTestTotalMessages/2, failedCount, "failed messages are missing")
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
			MaxDemand:   10,
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

	<-pipeline.ProducerIdle()

	cancel()

	<-pipeline.Terminated()

	assert.Equal(
		t,
		basicTestTotalMessages/2,
		successfulCount,
		"successful messages are missing",
	)
	assert.Equal(t, basicTestTotalMessages/2, failedCount, "failed messages are missing")
}

func TestMessagesProperlyProcessedAndAcked_WithPartitioning(t *testing.T) {
	var (
		processedCount int
		mu             sync.Mutex
	)

	acknowledger := func(messages []*broadway.Message, err error) {
		mu.Lock()
		defer mu.Unlock()

		processedCount += len(messages)
	}

	pipeline := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{
			Producer:    &basicTestProducer{},
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &basicTestMessageProcessor{},
			Concurrency: 5,
			MinDemand:   1,
			MaxDemand:   10,
		},
		Batchers: []broadway.BatcherConfig{
			{
				Concurrency:  5,
				Processor:    &basicTestBatchProcessor{},
				BatchSize:    10,
				BatchTimeout: time.Second,
			},
		},
		PartitionBy: func(payload broadway.MessagePayload) string {
			return payload.(basicTestMessage).UserId
		},
		Acknowledger: acknowledger,
	})

	ctx, cancel := context.WithCancel(context.Background())
	pipeline.Run(ctx)

	<-pipeline.ProducerIdle()

	cancel()

	<-pipeline.Terminated()

	assert.Equal(t, basicTestTotalMessages, processedCount, "not all messages were processed")
}
