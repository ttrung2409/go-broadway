package broadway

import (
	"context"
)

// producerSupervisor manages a pool of producer instances, handling their lifecycle
// and ensuring fault tolerance by restarting producers that panic during execution.
type producerSupervisor interface {
	// run starts the producer supervisor with the provided context. It in turn starts
	// the configured number of producers, and monitors them for failures.
	//
	// Parameters:
	//   - ctx: The context provided when starting the pipeline.
	//
	// Returns:
	//   - A map of producer IDs to producer instances that are currently active.
	run(ctx context.Context) map[string]producer

	// terminate stops all producers managed by this supervisor.
	// This should be called when shutting down the pipeline to ensure proper cleanup.
	terminate()

	producersChanged() <-chan map[string]producer
	allProducersDrained() <-chan bool
	allProducersTerminated() <-chan bool
}

type internalProducerSupervisor struct {
	config                   ProducerConfig
	producers                *concurrentMap[string, producer]
	messageProcessorResolver messageProcessorResolver
	messageAck               Acknowledger
	producersChangeCh        chan map[string]producer
	allProducersDrainedCh    chan bool
	allProducersTerminatedCh chan bool
}

func newProducerSupervisor(
	config ProducerConfig,
	messageProcessorResolver messageProcessorResolver,
	messageAck Acknowledger,
) producerSupervisor {
	return &internalProducerSupervisor{
		config:                   config,
		producers:                newConcurrentMap[string, producer](),
		messageProcessorResolver: messageProcessorResolver,
		messageAck:               messageAck,
		producersChangeCh:        make(chan map[string]producer),
		allProducersDrainedCh:    make(chan bool),
		allProducersTerminatedCh: make(chan bool),
	}
}

func (s *internalProducerSupervisor) run(
	ctx context.Context,
) map[string]producer {

	for i := 0; i < s.config.Concurrency; i++ {
		p := newProducer(s.config.Producer, s.config, s.messageProcessorResolver, s.messageAck)
		s.producers.set(p.id(), p)
		p.run(ctx)

		go func(p producer) {
			s.watchProducerStatus(p, ctx)
		}(p)
	}

	return s.producers.toMap()
}

func (s *internalProducerSupervisor) terminate() {
	for _, p := range s.producers.toMap() {
		p.terminate()
	}

	close(s.producersChangeCh)
}

// handleProducerPanic is called when a producer panics. It removes the failed producer,
// and creates a new one with the same configuration.
// This provides automatic recovery and fault tolerance for the pipeline.
//
// Parameters:
//   - p: The producer that panicked and needs to be replaced.
//   - ctx: The context provided when starting the pipeline.
func (s *internalProducerSupervisor) handleProducerPanic(p producer, ctx context.Context) {
	s.producers.delete(p.id())
	newProducer := newProducer(p.producer(), s.config, s.messageProcessorResolver, s.messageAck)
	s.producers.set(newProducer.id(), newProducer)
	newProducer.run(ctx)

	go func(p producer) {
		s.watchProducerStatus(p, ctx)
	}(newProducer)

	s.producersChangeCh <- s.producers.toMap()
}

func (s *internalProducerSupervisor) watchProducerStatus(p producer, ctx context.Context) {
	for {
		select {
		case <-p.drained():
			s.checkIfAllProducersDrained()
		case panic, ok := <-p.terminated():
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

func (s *internalProducerSupervisor) checkIfAllProducersDrained() {
	allDrained := true

	for _, producer := range s.producers.values() {
		if !producer.isDrained() {
			allDrained = false
		}
	}

	if allDrained {
		select {
		case s.allProducersDrainedCh <- true: // sent successfully
		default: // no receiver, ignore
		}
	}
}

func (s *internalProducerSupervisor) checkIfAllProducersTerminated() {
	allTerminated := true

	for _, producer := range s.producers.values() {
		if !producer.isTerminated() {
			allTerminated = false
		}
	}

	if allTerminated {
		select {
		case s.allProducersTerminatedCh <- true: // sent successfully
		default: // no receiver, ignore
		}
	}
}

func (s *internalProducerSupervisor) allProducersDrained() <-chan bool {
	return s.allProducersDrainedCh
}

func (s *internalProducerSupervisor) producersChanged() <-chan map[string]producer {
	return s.producersChangeCh
}

func (s *internalProducerSupervisor) allProducersTerminated() <-chan bool {
	return s.allProducersTerminatedCh
}
