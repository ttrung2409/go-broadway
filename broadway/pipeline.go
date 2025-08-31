package broadway

import (
	"context"
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
	terminated     bool
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
	return &Pipeline{
		config:         config,
		producerIdleCh: make(chan bool),
		terminatedCh:   make(chan bool),
	}
}

// Run starts the pipeline in a goroutine with the provided context.
// The pipeline will continue running until the context is canceled.
//
// Parameters:
//   - ctx: the cancellable context, possibly with value.
func (p *Pipeline) Run(ctx context.Context) {
	go func() {
		messageProcessorSupervisor := newMessageProcessorSupervisor(p.config.MessageProcessor)

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

		messageProcessorSupervisor.run(ctx, producers, batchers)

		go func() {
			defer func() {
				close(p.producerIdleCh)
			}()

			for {
				if p.terminated {
					return
				}

				select {
				case producers := <-producerSupervisor.producersChanged():
					messageProcessorSupervisor.setProducers(producers)
				case batchers := <-batcherSupervisor.batchersChanged():
					messageProcessorSupervisor.setBatchers(batchers)
				case <-producerSupervisor.allProducersIdle():
					p.producerIdleCh <- true
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
	p.terminated = true
	p.terminatedCh <- true
	close(p.terminatedCh)
}

func (p *Pipeline) ProducerIdle() <-chan bool {
	return p.producerIdleCh
}

func (p *Pipeline) Terminated() <-chan bool {
	return p.terminatedCh
}
