package broadway

import "context"

// producerSupervisor manages a pool of producer instances, handling their lifecycle
// and ensuring fault tolerance by restarting producers that panic during execution.
type producerSupervisor struct {
	config                   ProducerConfig
	producers                map[string]*producer
	messageProcessorResolver messageProcessorResolver
	onProducersChange        chan map[string]*producer
}

// newProducerSupervisor creates a new producer supervisor with the given configuration.
// It initializes empty maps and channels that will be populated when Run is called.
func newProducerSupervisor(config ProducerConfig, messageProcessorResolver messageProcessorResolver) *producerSupervisor {
	return &producerSupervisor{
		config:            config,
		producers:         make(map[string]*producer),
		onProducersChange: make(chan map[string]*producer),
	}
}

// Run starts the producer supervisor with the provided context. It creates and starts
// the configured number of producers, and sets up monitoring for each producer to handle panics.
// Returns a map of active producers and a channel that will receive updates when producers change.
//
// Parameters:
//   - ctx: The context that controls the lifecycle of the producer supervisor and its producers.
//     When cancelled, all producers will shut down gracefully.
//
// Returns:
//   - A map of producer IDs to producer instances that are currently active.
//   - A channel that will receive updates to the producers map when producers are replaced due to panics.
func (s *producerSupervisor) Run(
	ctx context.Context,
) (map[string]*producer, <-chan map[string]*producer) {

	for i := 0; i < s.config.Concurrency; i++ {
		p := newProducer(s.config, s.messageProcessorResolver)
		s.producers[p.Id] = p
		onTerminated := p.Run(ctx)

		go func(p *producer, onTerminated <-chan any) {
			err := <-onTerminated

			if err != nil {
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
// creates a new one with the same configuration, and notifies listeners about the change.
// This provides automatic recovery and fault tolerance for the pipeline.
//
// Parameters:
//   - p: The producer that panicked and needs to be replaced.
//   - ctx: The context to pass to the new producer.
func (s *producerSupervisor) handleProducerPanic(p *producer, ctx context.Context) {
	delete(s.producers, p.Id)
	newProducer := newProducer(s.config, s.messageProcessorResolver)
	s.producers[newProducer.Id] = newProducer
	onTerminated := newProducer.Run(ctx)

	go func(newProducer *producer, onTerminated <-chan any) {
		err := <-onTerminated

		if err != nil {
			s.handleProducerPanic(newProducer, ctx)
		}
	}(newProducer, onTerminated)

	s.onProducersChange <- s.producers
}
