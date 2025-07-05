package test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ttrung2409/go-broadway/broadway"
)

const faultTolerantTestTotalMessages = 100

var (
	producedCount                  int
	panickedMessageProcessorsCount int32
)

type faultTolerantTestProducer struct {
	shouldPanic bool
	panicked    bool
}

func (p *faultTolerantTestProducer) Clone() broadway.Producer {
	return &faultTolerantTestProducer{shouldPanic: p.shouldPanic && !p.panicked}
}

func (p *faultTolerantTestProducer) HandleDemand(
	demand int,
	ctx context.Context,
) []broadway.MessagePayload {

	if producedCount > faultTolerantTestTotalMessages {
		return []broadway.MessagePayload{}
	}

	if p.shouldPanic && producedCount > faultTolerantTestTotalMessages/2 {
		p.panicked = true
		panic("test producer recovery")
	}

	demand = min(demand, faultTolerantTestTotalMessages-producedCount)

	result := []broadway.MessagePayload{}

	for i := 0; i < demand; i++ {
		result = append(
			result,
			fmt.Sprintf("message %d", producedCount),
		)

		producedCount++
	}

	return result
}

type faultTolerantTestMessageProcessor struct {
	shouldPanic bool
	panicked    bool
}

func (p *faultTolerantTestMessageProcessor) Clone() broadway.MessageProcessor {
	return &faultTolerantTestMessageProcessor{shouldPanic: p.shouldPanic && !p.panicked}
}

func (p *faultTolerantTestMessageProcessor) Handle(
	message *broadway.Message,
	ctx context.Context,
) (*broadway.Message, error) {
	if p.shouldPanic && atomic.LoadInt32(&panickedMessageProcessorsCount) < 2 {
		p.panicked = true
		atomic.AddInt32(&panickedMessageProcessorsCount, 1)
		panic("test message processor recovery")
	}

	return message, nil
}

func TestProducerRecovery_MessagesProperlyAckedDespiteProducerFails(t *testing.T) {
	mu := sync.Mutex{}
	count := 0

	acknowledger := func(messages []*broadway.Message, err error) {
		mu.Lock()
		defer mu.Unlock()

		count += len(messages)
	}

	pipeline := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{
			Producer:    &faultTolerantTestProducer{shouldPanic: true},
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &faultTolerantTestMessageProcessor{},
			Concurrency: 5,
			MinDemand:   1,
			MaxDemand:   10,
		},
		Acknowledger: acknowledger,
	})

	ctx, cancel := context.WithCancel(context.Background())
	pipeline.Run(ctx)
	<-pipeline.OnProducerDrained()

	cancel()

	<-pipeline.OnTerminated()

	assert.Equal(t, faultTolerantTestTotalMessages, count)
}

func TestMessageProcessorRecovery_MessagesProperlyAckedDespiteMessageProcessorFails(t *testing.T) {
	producedCount = 0
	mu := sync.Mutex{}
	count := 0

	acknowledger := func(messages []*broadway.Message, err error) {
		mu.Lock()
		defer mu.Unlock()

		count += len(messages)
	}

	pipeline := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{
			Producer:    &faultTolerantTestProducer{},
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &faultTolerantTestMessageProcessor{shouldPanic: true},
			Concurrency: 5,
			MinDemand:   1,
			MaxDemand:   10,
		},
		Acknowledger: acknowledger,
	})

	ctx, cancel := context.WithCancel(context.Background())
	pipeline.Run(ctx)
	<-pipeline.OnProducerDrained()

	cancel()

	<-pipeline.OnTerminated()

	assert.Equal(t, faultTolerantTestTotalMessages, count)
}
