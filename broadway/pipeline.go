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
	config            PipelineConfig
	terminated        bool
	onProducerDrained chan bool
	onTerminated      chan bool
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
		config:            config,
		onProducerDrained: make(chan bool),
		onTerminated:      make(chan bool),
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
				return messageProcessorSupervisor.Resolve(partitionKey)
			},
			p.config.Acknowledger,
		)

		producers := producerSupervisor.Run(ctx)

		batcherSupervisor := newBatcherSupervisor(p.config.Batchers)
		batchers := batcherSupervisor.Run(ctx)

		messageProcessorSupervisor.Run(ctx, producers, batchers)

		go func() {
			defer func() {
				close(p.onProducerDrained)
			}()

			for {
				if p.terminated {
					return
				}

				select {
				case producers := <-producerSupervisor.OnProducersChange():
					messageProcessorSupervisor.SetProducers(producers)
				case batchers := <-batcherSupervisor.OnBatchersChange():
					messageProcessorSupervisor.SetBatchers(batchers)
				case <-producerSupervisor.OnAllProducersDrained():
					p.onProducerDrained <- true
				}
			}
		}()

		<-ctx.Done()

		producerSupervisor.Terminate()
		messageProcessorSupervisor.Terminate()
		<-messageProcessorSupervisor.OnAllProcessorsTerminated()

		if batchers != nil {
			batcherSupervisor.Terminate()
			<-batcherSupervisor.OnAllBatchersTerminated()
		}

		p.terminate()
	}()
}

func (p *Pipeline) terminate() {
	p.terminated = true
	p.onTerminated <- true
	close(p.onTerminated)
}

func (p *Pipeline) OnProducerDrained() <-chan bool {
	return p.onProducerDrained
}

func (p *Pipeline) OnTerminated() <-chan bool {
	return p.onTerminated
}
