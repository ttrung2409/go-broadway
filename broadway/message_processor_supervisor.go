package broadway

import "context"

type messageProcessorSupervisor struct {
	config     MessageProcessorConfig
	processors map[string]*messageProcessor
}

func newMessageProcessorSupervisor(config MessageProcessorConfig) *messageProcessorSupervisor {
	return &messageProcessorSupervisor{
		config:     config,
		processors: make(map[string]*messageProcessor),
	}
}

func (s *messageProcessorSupervisor) Run(
	ctx context.Context,
	producers map[string]*producer,
	batchers map[string]*batcher,
) {
	for i := 0; i < s.config.Concurrency; i++ {
		messageProcessor := newMessageProcessor(s.config, producers, batchers)
		messageProcessor.Run(ctx)
	}
}

func (s *messageProcessorSupervisor) Terminate() {
	for _, p := range s.processors {
		p.Terminate()
	}
}

func (s *messageProcessorSupervisor) SetProducers(producers map[string]*producer) {
	for _, p := range s.processors {
		p.SetProducers(producers)
	}
}

func (s *messageProcessorSupervisor) SetBatchers(batchers map[string]*batcher) {
	for _, p := range s.processors {
		p.SetBatchers(batchers)
	}
}
