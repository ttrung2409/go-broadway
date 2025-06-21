package test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/ttrung2409/go-broadway/broadway"
)

type basicTestMessage struct {
	Id         string
	ShouldFail bool
}

type basicTestProducer struct {
	Count int
}

func (p *basicTestProducer) New() broadway.Producer {
	return &basicTestProducer{}
}

const numOfMessages = 100

func (p *basicTestProducer) HandleDemand(demand int) []broadway.MessagePayload {
	result := make([]broadway.MessagePayload, 0)

	if p.Count > numOfMessages {
		return []broadway.MessagePayload{}
	}

	for i := 0; i < min(numOfMessages-p.Count, demand); i++ {
		result = append(result, basicTestMessage{Id: fmt.Sprintf("Message %d", i+1+p.Count), ShouldFail: (i+1+p.Count)%2 == 0})
	}

	p.Count += min(numOfMessages-p.Count, demand)

	return result
}

type basicTestProcessor struct{}

func (p *basicTestProcessor) New() broadway.MessageProcessor {
	return &basicTestProcessor{}
}

func (p *basicTestProcessor) Handle(message *broadway.Message, ctx context.Context) (*broadway.Message, error) {
	if message.Payload.(basicTestMessage).ShouldFail {
		return nil, fmt.Errorf("processing failed for message: %s", message.Payload.(basicTestMessage).Id)
	}

	return message, nil
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
			Producer:    &basicTestProducer{},
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &basicTestProcessor{},
			Concurrency: 5,
			MinDemand:   5,
			MaxDemand:   10,
		},
		Acknowledger: acknowledger,
	})

	ctx, cancel := context.WithCancel(context.Background())

	pipeline.Run(ctx)

	<-pipeline.OnProducerDrained()

	cancel()

	<-pipeline.OnTerminated()

	expectedSuccess := numOfMessages / 2
	expectedFailures := numOfMessages / 2

	if successfulCount != expectedSuccess {
		t.Errorf("Expected %d successful messages, got %d", expectedSuccess, successfulCount)
	}

	if failedCount != expectedFailures {
		t.Errorf("Expected %d failed messages, got %d", expectedFailures, failedCount)
	}
}
