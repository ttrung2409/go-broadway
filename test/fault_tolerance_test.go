package test

import (
	"context"
	"hash/fnv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/ttrung2409/go-broadway/broadway"
)

const faultToleranceTestTotalMessages = 100
const faultToleranceTestTotalUsers = 10

var (
	producedCount int
)

type faultToleranceTestMessage struct {
	UserId string
	Order  int
}

type faultToleranceTestProducer struct {
	shouldPanic bool
	panicked    bool
}

func newFaultTolerantTestProducer(shouldPanic bool) *faultToleranceTestProducer {
	producedCount = 0

	return &faultToleranceTestProducer{
		shouldPanic: shouldPanic,
	}
}

func (p *faultToleranceTestProducer) Clone() broadway.Producer {
	return &faultToleranceTestProducer{shouldPanic: p.shouldPanic && !p.panicked}
}

func (p *faultToleranceTestProducer) HandleDemand(
	demand int,
	ctx context.Context,
) []broadway.MessagePayload {

	if producedCount > faultToleranceTestTotalMessages {
		return []broadway.MessagePayload{}
	}

	if p.shouldPanic && producedCount > faultToleranceTestTotalMessages/2 {
		p.panicked = true
		panic("test producer recovery")
	}

	demand = min(demand, faultToleranceTestTotalMessages-producedCount)
	userIds := make([]string, 0)
	for i := 0; i < faultToleranceTestTotalUsers; i++ {
		userIds = append(userIds, uuid.NewString())
	}

	result := []broadway.MessagePayload{}

	for i := 0; i < demand; i++ {
		result = append(
			result,
			&faultToleranceTestMessage{
				UserId: userIds[producedCount%faultToleranceTestTotalUsers],
				Order:  producedCount,
			},
		)

		producedCount++
	}

	return result
}

type faultToleranceTestMessageProcessor struct {
	id          string
	shouldPanic bool
	panicked    bool
}

func (p *faultToleranceTestMessageProcessor) Clone() broadway.MessageProcessor {
	return &faultToleranceTestMessageProcessor{
		id:          uuid.NewString(),
		shouldPanic: p.shouldPanic && !p.panicked,
	}
}

func (p *faultToleranceTestMessageProcessor) Handle(
	message *broadway.Message,
	ctx context.Context,
) (*broadway.Message, error) {

	if p.shouldPanic && hashString(p.id)%2 == 1 {
		p.panicked = true
		panic("test message processor recovery")
	}

	return message, nil
}

type faultToleranceTestBatchProcessor struct {
	id          string
	shouldPanic bool
	panicked    bool
}

func (p *faultToleranceTestBatchProcessor) Clone() broadway.BatchProcessor {
	return &faultToleranceTestBatchProcessor{
		id:          uuid.NewString(),
		shouldPanic: p.shouldPanic,
		panicked:    p.panicked,
	}
}

func (p *faultToleranceTestBatchProcessor) Handle(
	messages []*broadway.Message,
	ctx context.Context,
) ([]*broadway.Message, error) {
	if p.shouldPanic && hashString(p.id)%2 == 1 {
		p.panicked = true
		panic("test batch processor recovery")
	}

	return messages, nil
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func TestProducerRecovery_MessagesProperlyAckedDespiteFailure(t *testing.T) {
	mu := sync.Mutex{}
	count := 0

	acknowledger := func(messages []*broadway.Message, err error) {
		mu.Lock()
		defer mu.Unlock()

		count += len(messages)
	}

	pipeline := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{
			Producer:    newFaultTolerantTestProducer(true),
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &faultToleranceTestMessageProcessor{},
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

	assert.Equal(t, faultToleranceTestTotalMessages, count, "not all messages were processed")
}

func TestMessageProcessorRecovery_MessagesProperlyAckedDespiteFailure(t *testing.T) {
	mu := sync.Mutex{}
	count := 0

	acknowledger := func(messages []*broadway.Message, err error) {
		mu.Lock()
		defer mu.Unlock()

		count += len(messages)
	}

	pipeline := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{
			Producer:    newFaultTolerantTestProducer(false),
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &faultToleranceTestMessageProcessor{shouldPanic: true},
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

	assert.Equal(t, faultToleranceTestTotalMessages, count, "not all messages were processed")
}

func TestMessageProcessorRecovery_PartitionedMessagesProcessedInOrderDespiteFailure(
	t *testing.T,
) {
	lastProcessedMessageByPartitionKey := make(map[string]*broadway.Message)
	mu := &sync.Mutex{}
	processedCount := 0

	acknowledger := func(messages []*broadway.Message, err error) {
		mu.Lock()
		defer mu.Unlock()

		processedCount += len(messages)

		for _, message := range messages {
			payload := message.Payload.(*faultToleranceTestMessage)
			partitionKey := payload.UserId

			if lastMessage, ok := lastProcessedMessageByPartitionKey[partitionKey]; ok {
				lastOrder := lastMessage.Payload.(*faultToleranceTestMessage).Order
				currentOrder := payload.Order

				assert.Less(
					t,
					lastOrder,
					currentOrder,
					"messages with partition key %s were processed out of order: %d before %d",
					partitionKey,
					lastOrder,
					currentOrder)

			}

			lastProcessedMessageByPartitionKey[partitionKey] = message
		}
	}

	pipeline := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{
			Producer:    newFaultTolerantTestProducer(false),
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &faultToleranceTestMessageProcessor{shouldPanic: true},
			Concurrency: 5,
			MinDemand:   1,
			MaxDemand:   10,
		},
		PartitionBy: func(payload broadway.MessagePayload) string {
			return payload.(*faultToleranceTestMessage).UserId
		},
		Acknowledger: acknowledger,
	})

	ctx, cancel := context.WithCancel(context.Background())
	pipeline.Run(ctx)

	<-pipeline.ProducerDrained()

	cancel()

	<-pipeline.Terminated()

	assert.Equal(
		t,
		faultToleranceTestTotalMessages,
		processedCount,
		"not all messages were processed",
	)
}

func TestBatchProcessorRecovery_MessagesProperlyAckedDespiteFailure(t *testing.T) {
	mu := sync.Mutex{}
	count := 0

	acknowledger := func(messages []*broadway.Message, err error) {
		mu.Lock()
		defer mu.Unlock()

		count += len(messages)
	}

	pipeline := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{
			Producer:    newFaultTolerantTestProducer(false),
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &faultToleranceTestMessageProcessor{id: uuid.NewString()},
			Concurrency: 5,
			MinDemand:   1,
			MaxDemand:   10,
		},
		Batchers: []broadway.BatcherConfig{
			{
				Concurrency:  2,
				Processor:    &faultToleranceTestBatchProcessor{shouldPanic: true},
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

	select {
	case <-pipeline.Terminated():
	case <-time.After(time.Second * 2):
	}

	assert.Equal(t, faultToleranceTestTotalMessages, count, "not all messages were processed")
}
