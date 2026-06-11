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

type batchingTestMessage struct {
	UserId                    string
	Action                    string
	BatchedBy                 string
	HandledByBatchProcessorId string
	Tenant                    string
	Order                     int
}

type batchingTestProducer struct {
	count int
}

func (p *batchingTestProducer) Init(_ context.Context) {}

func (p *batchingTestProducer) Clone() broadway.Producer {
	return &batchingTestProducer{}
}

const batchingTestTotalMessages = 100
const batchingTestTotalUsers = 10

func (p *batchingTestProducer) HandleDemand(
	demand int,
	ctx context.Context,
) []*broadway.Message {
	result := make([]*broadway.Message, 0)
	actions := []string{"view", "click", "purchase"}

	if p.count > batchingTestTotalMessages {
		return []*broadway.Message{}
	}

	demand = min(demand, batchingTestTotalMessages-p.count)

	userIds := make([]string, 0)
	for i := 0; i < batchingTestTotalUsers; i++ {
		userIds = append(userIds, uuid.NewString())
	}

	for i := 0; i < demand; i++ {
		result = append(
			result,
			broadway.NewMessage(&batchingTestMessage{
				UserId: userIds[p.count%batchingTestTotalUsers],
				Action: actions[p.count%len(actions)],
				Tenant: fmt.Sprintf("tenant %d", p.count%2),
				Order:  p.count,
			}),
		)

		p.count++
	}

	return result
}

type batchingTestMessageProcessor struct {
}

func (p *batchingTestMessageProcessor) Clone() broadway.MessageProcessor {
	return &batchingTestMessageProcessor{}
}

func (p *batchingTestMessageProcessor) Handle(
	message *broadway.Message,
	ctx context.Context,
) (*broadway.Message, error) {
	message.BatchKey = message.Payload.(*batchingTestMessage).UserId

	return message, nil
}

type batchingTestBatchProcessor struct {
	Id string
}

func (p *batchingTestBatchProcessor) Clone() broadway.BatchProcessor {
	return &batchingTestBatchProcessor{
		Id: uuid.NewString(),
	}
}

func (p *batchingTestBatchProcessor) Handle(
	messages []*broadway.Message,
	ctx context.Context,
) ([]*broadway.Message, error) {
	for _, message := range messages {
		message.Payload.(*batchingTestMessage).HandledByBatchProcessorId = p.Id
	}

	return messages, nil
}

func TestMessagesWithSameBatchKeyRoutedToSameProcessor(t *testing.T) {
	batchProcessorsByBatchKey := make(map[string]string)
	mu := &sync.Mutex{}
	processedCount := 0

	acknowledger := func(messages []*broadway.Message, err error) {
		mu.Lock()
		defer mu.Unlock()

		processedCount += len(messages)

		for _, message := range messages {
			payload := message.Payload.(*batchingTestMessage)
			batchKey := payload.UserId
			batchProcessorId := payload.HandledByBatchProcessorId

			if existingBatchProcessorId, ok := batchProcessorsByBatchKey[batchKey]; ok {
				assert.Equal(
					t,
					existingBatchProcessorId,
					batchProcessorId,
					"Batch key %s was handled by multiple batch processors: %s and %s",
					batchKey,
					existingBatchProcessorId,
					batchProcessorId,
				)
			} else {
				batchProcessorsByBatchKey[batchKey] = batchProcessorId
			}
		}
	}

	pipeline := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{
			Producer:    &batchingTestProducer{},
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &batchingTestMessageProcessor{},
			Concurrency: 1,
			MinDemand:   1,
			MaxDemand:   100,
		},
		Batchers: []broadway.BatcherConfig{
			{
				Concurrency:  5,
				Processor:    &batchingTestBatchProcessor{},
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

	assert.Equal(t, batchingTestTotalMessages, processedCount, "not all messages were processed")
}

func TestMessagesWithSameBatchKeyProcessedInOrder(t *testing.T) {
	lastProcessedMessageByBatchKey := make(map[string]*broadway.Message)
	mu := &sync.Mutex{}
	processedCount := 0

	acknowledger := func(messages []*broadway.Message, err error) {
		mu.Lock()
		defer mu.Unlock()

		processedCount += len(messages)

		for _, message := range messages {
			payload := message.Payload.(*batchingTestMessage)
			batchKey := payload.UserId

			if lastMessage, ok := lastProcessedMessageByBatchKey[batchKey]; ok {
				assert.Less(
					t,
					lastMessage.Payload.(*batchingTestMessage).Order,
					payload.Order,
					"messages with batch key %s were processed out of order: %d before %d",
					batchKey,
					lastMessage.Payload.(*batchingTestMessage).Order,
					payload.Order,
				)
			}

			lastProcessedMessageByBatchKey[batchKey] = message
		}
	}

	pipeline := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{
			Producer:    &batchingTestProducer{},
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &batchingTestMessageProcessor{},
			Concurrency: 1,
			MinDemand:   1,
			MaxDemand:   100,
		},
		Batchers: []broadway.BatcherConfig{
			{
				Concurrency:  5,
				Processor:    &batchingTestBatchProcessor{},
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

	assert.Equal(t, batchingTestTotalMessages, processedCount, "not all messages were processed")
}

type multiBatcherMessageProcessor struct {
}

func (p *multiBatcherMessageProcessor) Clone() broadway.MessageProcessor {
	return &multiBatcherMessageProcessor{}
}

func (p *multiBatcherMessageProcessor) Handle(
	message *broadway.Message,
	ctx context.Context,
) (*broadway.Message, error) {
	message.BatchKey = message.Payload.(*batchingTestMessage).UserId
	message.Batcher = message.Payload.(*batchingTestMessage).Tenant

	return message, nil
}

type multiBatcherBatchProcessor struct {
	Id string
}

func (p *multiBatcherBatchProcessor) Clone() broadway.BatchProcessor {
	return &multiBatcherBatchProcessor{Id: uuid.NewString()}
}

func (p *multiBatcherBatchProcessor) Handle(
	messages []*broadway.Message,
	ctx context.Context,
) ([]*broadway.Message, error) {
	for _, message := range messages {
		payload := message.Payload.(*batchingTestMessage)
		payload.HandledByBatchProcessorId = p.Id
		payload.BatchedBy = ctx.Value(broadway.BatcherContextKey).(string)
	}

	return messages, nil
}

func TestMultiBatcher_MessagesRoutedToSameBatcher(t *testing.T) {
	mu := &sync.Mutex{}
	processedCount := 0

	acknowledger := func(messages []*broadway.Message, err error) {
		mu.Lock()
		defer mu.Unlock()

		processedCount += len(messages)

		for _, message := range messages {
			assert.Equal(
				t,
				message.Batcher,
				message.Payload.(*batchingTestMessage).BatchedBy,
				"message of %s is supposed to be routed to batcher %s, but was routed to %s",
				message.Payload.(*batchingTestMessage).Tenant,
				message.Batcher,
				message.Payload.(*batchingTestMessage).BatchedBy,
			)
		}
	}

	pipeline := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{
			Producer:    &batchingTestProducer{},
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &multiBatcherMessageProcessor{},
			Concurrency: 1,
			MinDemand:   1,
			MaxDemand:   100,
		},
		Batchers: []broadway.BatcherConfig{
			{
				Name:         "tenant 0",
				Concurrency:  5,
				Processor:    &multiBatcherBatchProcessor{},
				BatchSize:    10,
				BatchTimeout: time.Second,
			},
			{
				Name:         "tenant 1",
				Concurrency:  5,
				Processor:    &multiBatcherBatchProcessor{},
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

	assert.Equal(t, batchingTestTotalMessages, processedCount, "not all messages were processed")
}
