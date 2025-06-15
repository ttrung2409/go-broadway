package broadway

import "context"

// producerSupervisor manages a pool of producer instances, handling their lifecycle
// and ensuring fault tolerance by restarting producers that panic during execution.
type producerSupervisor struct {
	config                   ProducerConfig
	producers                map[string]*producer
	messageProcessorResolver messageProcessorResolver
	messageAck               Acknowledger
	onProducersChange        chan map[string]*producer
}

func newProducerSupervisor(config ProducerConfig, messageProcessorResolver messageProcessorResolver, messageAck Acknowledger) *producerSupervisor {
	return &producerSupervisor{
		config:                   config,
		producers:                make(map[string]*producer),
		messageProcessorResolver: messageProcessorResolver,
		messageAck:               messageAck,
		onProducersChange:        make(chan map[string]*producer),
	}
}

// Run starts the producer supervisor with the provided context. It in turn starts
// the configured number of producers, and monitors them for failures.
//
// Parameters:
//   - ctx: The context provided when starting the pipeline.
//
// Returns:
//   - A map of producer IDs to producer instances that are currently active.
//   - A channel that will receive updates to the producers, i.e. when a producer failed and is replaced
func (s *producerSupervisor) Run(
	ctx context.Context,
) (map[string]*producer, <-chan map[string]*producer) {

	for i := 0; i < s.config.Concurrency; i++ {
		p := newProducer(s.config, s.messageProcessorResolver, s.messageAck)
		s.producers[p.Id] = p
		onTerminated := p.Run(ctx)

		go func(p *producer, onTerminated <-chan any) {
			err, ok := <-onTerminated

			if ok && err != nil {
				s.handleProducerPanic(p, ctx)
			}
		}(p, onTerminated)
	}

	return s.producers, s.onProducersChange
}

// Terminate stops all producers managed by this supervisor and releases all resources.
// This should be called when shutting down the pipeline to ensure proper cleanup.
func (s *producerSupervisor) Terminate() {
	for _, p := range s.producers {
		p.Terminate()
	}

	close(s.onProducersChange)
}

// handleProducerPanic is called when a producer panics. It removes the failed producer,
// and creates a new one with the same configuration.
// This provides automatic recovery and fault tolerance for the pipeline.
//
// Parameters:
//   - p: The producer that panicked and needs to be replaced.
//   - ctx: The context provided when starting the pipeline.
func (s *producerSupervisor) handleProducerPanic(p *producer, ctx context.Context) {
	delete(s.producers, p.Id)
	newProducer := newProducer(s.config, s.messageProcessorResolver, s.messageAck)
	s.producers[newProducer.Id] = newProducer
	onTerminated := newProducer.Run(ctx)

	go func(newProducer *producer, onTerminated <-chan any) {
		err, ok := <-onTerminated

		if ok && err != nil {
			s.handleProducerPanic(newProducer, ctx)
		}
	}(newProducer, onTerminated)

	s.onProducersChange <- s.producers
}
