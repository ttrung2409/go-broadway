package broadway

import (
	"context"
)

type PartitionKeyResolver func(payload MessagePayload) string

// PipelineConfig defines the configuration for a Broadway pipeline,
// consisting of a producer, message processor, and batchers.
type PipelineConfig struct {
	Producer             ProducerConfig
	MessageProcessor     MessageProcessorConfig
	Batchers             []BatcherConfig
	PartitionKeyResolver PartitionKeyResolver
}

// Pipeline represents a Broadway processing pipeline that manages the
// flow of messages from producers through batchers to message processors.
type Pipeline struct {
	config PipelineConfig
}

// NewPipeline creates a new Broadway pipeline with the given configuration.
//
// Parameters:
//   - config: The configuration for the pipeline, specifying the producers, message processors,
//     and batchers to be used.
//
// Returns:
//   - A new Pipeline instance ready to be started with Run.
func NewPipeline(config PipelineConfig) *Pipeline {
	return &Pipeline{config: config}
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
		producerConfig.partitionKeyResolver = p.config.PartitionKeyResolver
		producerSupervisor := newProducerSupervisor(producerConfig, func(partitionKey string) (string, bool) {
			return messageProcessorSupervisor.Resolve(partitionKey)
		})

		producers, onProducersChange := producerSupervisor.Run(ctx)

		batcherSupervisor := newBatcherSupervisor(p.config.Batchers)
		batcherInstances, onBatchersChange := batcherSupervisor.Run(ctx)

		messageProcessorSupervisor.Run(ctx, producers, batcherInstances)

		go func() {
			for {
				select {
				case producers := <-onProducersChange:
					messageProcessorSupervisor.SetProducers(producers)
				case batcherInstances := <-onBatchersChange:
					messageProcessorSupervisor.SetBatchers(batcherInstances)
				}
			}
		}()

		<-ctx.Done()

		producerSupervisor.Terminate()
		messageProcessorSupervisor.Terminate()
		batcherSupervisor.Terminate()
	}()
}
