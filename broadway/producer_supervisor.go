package broadway

import (
	"context"
)

// producerSupervisor manages a pool of producer instances, handling their lifecycle
// and ensuring fault tolerance by restarting producers that panic during execution.
type producerSupervisor struct {
	config                   ProducerConfig
	producers                *concurrentMap[string, *producer]
	messageProcessorResolver messageProcessorResolver
	messageAck               Acknowledger
	onProducersChange        chan map[string]*producer
	onAllProducersDrained    chan bool
	onAllProducersTerminated chan bool
}

func newProducerSupervisor(
	config ProducerConfig,
	messageProcessorResolver messageProcessorResolver,
	messageAck Acknowledger,
) *producerSupervisor {
	return &producerSupervisor{
		config:                   config,
		producers:                newConcurrentMap[string, *producer](),
		messageProcessorResolver: messageProcessorResolver,
		messageAck:               messageAck,
		onProducersChange:        make(chan map[string]*producer),
		onAllProducersDrained:    make(chan bool),
		onAllProducersTerminated: make(chan bool),
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
func (s *producerSupervisor) Run(
	ctx context.Context,
) map[string]*producer {

	for i := 0; i < s.config.Concurrency; i++ {
		p := newProducer(s.config, s.messageProcessorResolver, s.messageAck)
		s.producers.Set(p.Id, p)
		p.Run(ctx)

		go func(p *producer) {
			s.watchProducerStatus(p, ctx)
		}(p)
	}

	return s.producers.ToMap()
}

// Terminate stops all producers managed by this supervisor.
// This should be called when shutting down the pipeline to ensure proper cleanup.
func (s *producerSupervisor) Terminate() {
	for _, p := range s.producers.ToMap() {
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
	s.producers.Delete(p.Id)
	newProducer := newProducer(s.config, s.messageProcessorResolver, s.messageAck)
	s.producers.Set(newProducer.Id, newProducer)
	newProducer.Run(ctx)

	go func(p *producer) {
		s.watchProducerStatus(p, ctx)
	}(newProducer)

	s.onProducersChange <- s.producers.ToMap()
}

func (s *producerSupervisor) watchProducerStatus(p *producer, ctx context.Context) {
	for {
		select {
		case <-p.OnDrained():
			s.checkIfAllProducersDrained()
		case panic, ok := <-p.OnTerminated():
			if ok {
				if panic != nil {
					s.handleProducerPanic(p, ctx)
				} else {
					s.checkIfAllProducersTerminated()
				}
			}

			return
		}
	}
}

func (s *producerSupervisor) checkIfAllProducersDrained() {
	allDrained := true

	for _, producer := range s.producers.Values() {
		if !producer.IsDrained() {
			allDrained = false
		}
	}

	if allDrained {
		select {
		case s.onAllProducersDrained <- true: // sent successfully
		default: // no receiver, ignore
		}
	}
}

func (s *producerSupervisor) checkIfAllProducersTerminated() {
	allTerminated := true

	for _, producer := range s.producers.Values() {
		if !producer.IsTerminated() {
			allTerminated = false
		}
	}

	if allTerminated {
		select {
		case s.onAllProducersTerminated <- true: // sent successfully
		default: // no receiver, ignore
		}
	}
}

func (s *producerSupervisor) OnAllProducersDrained() <-chan bool {
	return s.onAllProducersDrained
}

func (s *producerSupervisor) OnProducersChange() <-chan map[string]*producer {
	return s.onProducersChange
}
