package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/ttrung2409/go-broadway/broadway"
)

type partitioningTestMessage struct {
	UserId                      string
	Action                      string
	HandledByMessageProcessorId string
	HandledByBatchProcessorId   string
	Order                       int
}

type partitioningTestProducer struct {
	count int
}

func (p *partitioningTestProducer) New() broadway.Producer {
	return &partitioningTestProducer{}
}

const partitioningTestTotalMessages = 100
const partitioningTestTotalUsers = 10

func (p *partitioningTestProducer) HandleDemand(demand int) []broadway.MessagePayload {
	result := make([]broadway.MessagePayload, 0)
	actions := []string{"view", "click", "purchase"}

	if p.count > partitioningTestTotalMessages {
		return []broadway.MessagePayload{}
	}

	demand = min(demand, partitioningTestTotalMessages-p.count)
	userIds := make([]string, 0)
	for i := 0; i < partitioningTestTotalUsers; i++ {
		userIds = append(userIds, uuid.NewString())
	}

	for i := 0; i < demand; i++ {
		result = append(
			result,
			&partitioningTestMessage{
				UserId: userIds[p.count%partitioningTestTotalUsers],
				Action: actions[p.count%len(actions)],
				Order:  p.count,
			},
		)

		p.count++
	}

	return result
}

type partitioningTestMessageProcessor struct {
	Id string
}

func (p *partitioningTestMessageProcessor) New() broadway.MessageProcessor {
	return &partitioningTestMessageProcessor{Id: uuid.NewString()}
}

func (p *partitioningTestMessageProcessor) Handle(
	message *broadway.Message,
	ctx context.Context,
) (*broadway.Message, error) {
	message.Payload.(*partitioningTestMessage).HandledByMessageProcessorId = p.Id

	return message, nil
}

type partitioningTestBatchProcessor struct {
	Id string
}

func (p *partitioningTestBatchProcessor) New() broadway.BatchProcessor {
	return &partitioningTestBatchProcessor{
		Id: uuid.NewString(),
	}
}

func (p *partitioningTestBatchProcessor) Handle(
	messages []*broadway.Message,
	ctx context.Context,
) ([]*broadway.Message, error) {
	for _, message := range messages {
		message.Payload.(*partitioningTestMessage).HandledByBatchProcessorId = p.Id
	}

	return messages, nil
}

func TestMessagesWithSameParitionKeyRoutedToSameProcessors(t *testing.T) {
	messageProcessorsByPartitionKey := make(map[string]string)
	batchProcessorsByPartitionKey := make(map[string]string)
	mu := sync.Mutex{}

	acknowledger := func(messages []*broadway.Message, err error) {
		mu.Lock()
		defer mu.Unlock()

		for _, message := range messages {
			payload := message.Payload.(*partitioningTestMessage)
			partitionKey := payload.UserId

			if existingMessageProcessorId, ok := messageProcessorsByPartitionKey[partitionKey]; ok {
				assert.Equal(
					t,
					existingMessageProcessorId,
					payload.HandledByMessageProcessorId,
					"Partition key %s was handled by multiple message processors: %s and %s",
					partitionKey,
					existingMessageProcessorId,
					payload.HandledByMessageProcessorId,
				)

			} else {
				messageProcessorsByPartitionKey[partitionKey] = payload.HandledByMessageProcessorId
			}

			if existingBatchProcessorId, ok := batchProcessorsByPartitionKey[partitionKey]; ok {
				assert.Equal(
					t,
					existingBatchProcessorId,
					payload.HandledByBatchProcessorId,
					"Partition key %s was handled by multiple batch processors: %s and %s",
					partitionKey,
					existingBatchProcessorId,
					payload.HandledByBatchProcessorId)

			} else {
				batchProcessorsByPartitionKey[partitionKey] = payload.HandledByBatchProcessorId
			}
		}
	}

	pipeline := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{
			Producer:    &partitioningTestProducer{},
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &partitioningTestMessageProcessor{},
			Concurrency: 5,
			MinDemand:   1,
			MaxDemand:   100,
		},
		Batchers: []broadway.BatcherConfig{
			{
				Concurrency:  5,
				Processor:    &partitioningTestBatchProcessor{},
				BatchSize:    10,
				BatchTimeout: time.Second,
			},
		},
		PartitionBy: func(payload broadway.MessagePayload) string {
			return payload.(*partitioningTestMessage).UserId
		},
		Acknowledger: acknowledger,
	})

	ctx, cancel := context.WithCancel(context.Background())
	pipeline.Run(ctx)

	<-pipeline.OnProducerDrained()

	cancel()

	<-pipeline.OnTerminated()
}

func TestMessagesWithSamePartitionKeyProcessedInOrder(t *testing.T) {
	lastProcessedMessageByPartitionKey := make(map[string]*broadway.Message)
	mu := &sync.Mutex{}

	acknowledger := func(messages []*broadway.Message, err error) {
		mu.Lock()
		defer mu.Unlock()

		for _, message := range messages {
			payload := message.Payload.(*partitioningTestMessage)
			partitionKey := payload.UserId

			if lastMessage, ok := lastProcessedMessageByPartitionKey[partitionKey]; ok {
				assert.Less(
					t,
					lastMessage.Payload.(*partitioningTestMessage).Order,
					payload.Order,
					"messages with partition key %s were processed out of order: %d before %d",
					partitionKey,
					lastMessage.Payload.(*partitioningTestMessage).Order,
					payload.Order)
			}

			lastProcessedMessageByPartitionKey[partitionKey] = message
		}
	}

	pipeline := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{
			Producer:    &partitioningTestProducer{},
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &partitioningTestMessageProcessor{},
			Concurrency: 5,
			MinDemand:   1,
			MaxDemand:   100,
		},
		Batchers: []broadway.BatcherConfig{
			{
				Concurrency:  5,
				Processor:    &partitioningTestBatchProcessor{},
				BatchSize:    10,
				BatchTimeout: time.Second,
			},
		},
		PartitionBy: func(payload broadway.MessagePayload) string {
			return payload.(*partitioningTestMessage).UserId
		},
		Acknowledger: acknowledger,
	})

	ctx, cancel := context.WithCancel(context.Background())
	pipeline.Run(ctx)

	<-pipeline.OnProducerDrained()

	cancel()

	<-pipeline.OnTerminated()

}
