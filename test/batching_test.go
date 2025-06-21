package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ttrung2409/go-broadway/broadway"
)

type batchingTestMessage struct {
	UserId                    string
	Action                    string
	HandledByBatchProcessorId string
}

type batchingTestProducer struct {
	Count int
}

func (p *batchingTestProducer) New() broadway.Producer {
	return &batchingTestProducer{}
}

func (p *batchingTestProducer) HandleDemand(demand int) []broadway.MessagePayload {
	const numOfMessages = 100

	result := make([]broadway.MessagePayload, 0)
	actions := []string{"view", "click", "purchase"}

	if p.Count > numOfMessages {
		return []broadway.MessagePayload{}
	}

	for i := 0; i < min(numOfMessages-p.Count, demand); i++ {
		result = append(result, &batchingTestMessage{UserId: fmt.Sprintf("User%d", (i+1+p.Count)%5), Action: actions[(i+1+p.Count)%len(actions)]})
	}

	p.Count += min(numOfMessages-p.Count, demand)

	return result
}

type batchingTestMessageProcessor struct {
}

func (p *batchingTestMessageProcessor) New() broadway.MessageProcessor {
	return &batchingTestMessageProcessor{}
}

func (p *batchingTestMessageProcessor) Handle(message *broadway.Message, ctx context.Context) (*broadway.Message, error) {
	message.BatchKey = message.Payload.(*batchingTestMessage).UserId

	return message, nil
}

type batchingTestBatchProcessor struct {
	Id string
}

func (p *batchingTestBatchProcessor) New() broadway.BatchProcessor {
	return &batchingTestBatchProcessor{
		Id: uuid.NewString(),
	}
}

func (p *batchingTestBatchProcessor) Handle(messages []*broadway.Message, ctx context.Context) ([]*broadway.Message, error) {
	for _, message := range messages {
		message.Payload.(*batchingTestMessage).HandledByBatchProcessorId = p.Id
	}

	return messages, nil
}

func TestMessagesWithSameBatchKeyRoutedToSameProcessor(t *testing.T) {
	batchProcessorsByUser := make(map[string]string)

	acknowledger := func(messages []*broadway.Message, err error) {
		for _, message := range messages {
			payload := message.Payload.(*batchingTestMessage)
			userId := payload.UserId
			batchProcessorId := payload.HandledByBatchProcessorId

			if existingBatchProcessorId, ok := batchProcessorsByUser[userId]; ok {
				if existingBatchProcessorId != batchProcessorId {
					t.Errorf("Batch key %s was handled by multiple batch processors: %s and %s",
						userId, existingBatchProcessorId, batchProcessorId)
				}
			} else {
				batchProcessorsByUser[userId] = batchProcessorId
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
			Concurrency: 5,
			MinDemand:   5,
			MaxDemand:   10,
		},
		Batchers: []broadway.BatcherConfig{
			{Concurrency: 5, Processor: &batchingTestBatchProcessor{}, BatchSize: 10, BatchTimeout: 1 * time.Second},
		},
		Acknowledger: acknowledger,
	})

	ctx, cancel := context.WithCancel(context.Background())
	pipeline.Run(ctx)

	<-pipeline.OnProducerDrained()

	cancel()

	<-pipeline.OnTerminated()
}
