package broadway

import (
	"context"
)

type PipelineConfig struct {
	Producer         ProducerConfig
	MessageProcessor MessageProcessorConfig
	Batchers         []BatcherConfig
}

type Pipeline struct {
	config PipelineConfig
}

func NewPipeline(config PipelineConfig) *Pipeline {
	return &Pipeline{config: config}
}

func (p *Pipeline) Run(ctx context.Context) {
	go func() {
		producerSupervisor := newProducerSupervisor(p.config.Producer)
		producers, onProducersChange := producerSupervisor.Run(ctx)

		batcherSupervisor := newBatcherSupervisor(p.config.Batchers)
		batchers, onBatchersChange := batcherSupervisor.Run(ctx)

		messageProcessorSupervisor := newMessageProcessorSupervisor(p.config.MessageProcessor)
		messageProcessorSupervisor.Run(ctx, producers, batchers)

		go func() {
			for {
				select {
				case producers := <-onProducersChange:
					messageProcessorSupervisor.SetProducers(producers)
				case batchers := <-onBatchersChange:
					messageProcessorSupervisor.SetBatchers(batchers)
				}
			}
		}()

		<-ctx.Done()

		producerSupervisor.Terminate()
		messageProcessorSupervisor.Terminate()
		batcherSupervisor.Terminate()
	}()
}
