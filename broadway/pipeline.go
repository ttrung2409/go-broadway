package broadway

import (
	"context"
	"sync/atomic"
)

// PartitionKeyResolver is a function that extracts a partition key from a message.
// The partition key is used to ensure that messages with the same key are always processed
// by the same message processor and batch processor (if batching is enabled),
// which guarantees ordering of related messages.
//
// Parameters:
//   - payload: The message payload to extract a partition key from
//
// Returns:
//   - The partition key
type PartitionKeyResolver func(payload MessagePayload) string

// PipelineConfig defines the configuration for a Broadway pipeline.
type PipelineConfig struct {
	Producer         ProducerConfig
	MessageProcessor MessageProcessorConfig
	Batchers         []BatcherConfig
	PartitionBy      PartitionKeyResolver
	Acknowledger     Acknowledger
}

// Pipeline represents a Broadway processing pipeline that manages the
// flow of messages from producers through message processors to batchers.
// It orchestrates the interaction between these components to ensure efficient
// message processing with proper partitioning, batching, and acknowledgment.
type Pipeline struct {
	config         PipelineConfig
	terminated     atomic.Bool
	producerIdleCh chan bool
	terminatedCh   chan bool
}

// NewPipeline creates a new Broadway pipeline with the given configuration.
//
// Parameters:
//   - config: The configuration for the pipeline
//
// Returns:
//   - A new Pipeline instance
func NewPipeline(config PipelineConfig) *Pipeline {
	if connector, ok := config.Producer.Producer.(Connector); ok {
		config.Acknowledger = connector.Acknowledger()
	}

	return &Pipeline{
		config:         config,
		producerIdleCh: make(chan bool, 1),
		terminatedCh:   make(chan bool, 1),
	}
}

// Run starts the pipeline in a goroutine with the provided context.
// The pipeline will continue running until the context is canceled.
//
// Parameters:
//   - ctx: the cancellable context, possibly with value.
func (p *Pipeline) Run(ctx context.Context) {
	go func() {
		var messageProcessorSupervisor *messageProcessorSupervisor

		producerConfig := p.config.Producer
		producerConfig.partitionKeyResolver = p.config.PartitionBy
		producerSupervisor := newProducerSupervisor(
			producerConfig,
			func(partitionKey string) (string, bool) {
				return messageProcessorSupervisor.resolve(partitionKey)
			},
			p.config.Acknowledger,
		)

		producers := producerSupervisor.run(ctx)

		batcherSupervisor := newBatcherSupervisor(p.config.Batchers)
		batchers := batcherSupervisor.run(ctx)

		messageProcessorSupervisor = newMessageProcessorSupervisor(
			p.config.MessageProcessor,
			producers,
			batchers,
		)

		messageProcessorSupervisor.run(ctx)

		go func() {
			defer func() {
				close(p.producerIdleCh)
			}()

			for {
				if p.terminated.Load() {
					return
				}

				select {
				case producers := <-producerSupervisor.producersChanged():
					messageProcessorSupervisor.setProducers(producers)
				case <-producerSupervisor.allProducersIdle():
					select {
					case p.producerIdleCh <- true:
					default:
					}
				}
			}
		}()

		<-ctx.Done()

		producerSupervisor.terminate()
		<-producerSupervisor.allProducersTerminated()

		messageProcessorSupervisor.terminate()
		<-messageProcessorSupervisor.allProcessorsTerminated()

		if batchers != nil {
			batcherSupervisor.terminate()
			<-batcherSupervisor.allBatchersTerminated()
		}

		p.terminate()
	}()
}

func (p *Pipeline) terminate() {
	p.terminated.Store(true)
	p.terminatedCh <- true
	close(p.terminatedCh)
}

func (p *Pipeline) ProducerIdle() <-chan bool {
	return p.producerIdleCh
}

func (p *Pipeline) Terminated() <-chan bool {
	return p.terminatedCh
}
