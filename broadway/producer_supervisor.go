package broadway

import (
	"context"
	"sync"
)

// producerSupervisor manages a pool of producer instances, handling their lifecycle
// and ensuring fault tolerance by restarting producers that panic during execution.
type producerSupervisor struct {
	config ProducerConfig

	_producers                *concurrentMap[string, *producer]
	_messageProcessorResolver messageProcessorResolver
	_messageAck               Acknowledger
	_producersChangeCh        chan map[string]*producer
	_allProducersIdleCh       chan bool
	_allProducersTerminatedCh chan bool
	_terminatedOnce           sync.Once
}

func newProducerSupervisor(
	config ProducerConfig,
	messageProcessorResolver messageProcessorResolver,
	messageAck Acknowledger,
) *producerSupervisor {
	return &producerSupervisor{
		config:                    config,
		_producers:                newConcurrentMap[string, *producer](),
		_messageProcessorResolver: messageProcessorResolver,
		_messageAck:               messageAck,
		_producersChangeCh:        make(chan map[string]*producer),
		_allProducersIdleCh:       make(chan bool, 1),
		_allProducersTerminatedCh: make(chan bool, 1),
	}
}

// run starts the producer supervisor with the provided context. It in turn starts
// the configured number of producers, and monitors them for failures.
//
// Parameters:
//   - ctx: The context provided when starting the pipeline.
//
// Returns:
//   - A map of producer IDs to producer instances that are currently active.
func (s *producerSupervisor) run(ctx context.Context) map[string]*producer {

	for i := 0; i < s.config.Concurrency; i++ {
		p := newProducer(s.config.Producer, s.config, s._messageProcessorResolver, s._messageAck)
		s._producers.set(p.id, p)
		p.run(ctx)

		go func(p *producer) {
			s.watchProducerStatus(p, ctx)
		}(p)
	}

	return s._producers.toMap()
}

// terminate stops all producers managed by this supervisor.
// This should be called when shutting down the pipeline to ensure proper cleanup.
func (s *producerSupervisor) terminate() {
	for _, p := range s._producers.toMap() {
		p.terminate()
	}

	close(s._producersChangeCh)
}

// handleProducerPanic is called when a producer panics. It removes the failed producer,
// and creates a new one with the same configuration.
// This provides automatic recovery and fault tolerance for the pipeline.
//
// Parameters:
//   - p: The producer that panicked and needs to be replaced.
//   - ctx: The context provided when starting the pipeline.
func (s *producerSupervisor) handleProducerPanic(p *producer, ctx context.Context) {
	s._producers.delete(p.id)
	newProducer := newProducer(p.producer, s.config, s._messageProcessorResolver, s._messageAck)
	s._producers.set(newProducer.id, newProducer)
	newProducer.run(ctx)

	go func(p *producer) {
		s.watchProducerStatus(p, ctx)
	}(newProducer)

	s._producersChangeCh <- s._producers.toMap()
}

func (s *producerSupervisor) watchProducerStatus(p *producer, ctx context.Context) {
	for {
		select {
		case <-p.idle():
			s.checkIfAllProducersIdle()
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

func (s *producerSupervisor) checkIfAllProducersIdle() {
	allIdle := true

	for _, producer := range s._producers.values() {
		if !producer.isIdle() {
			allIdle = false
		}
	}

	if allIdle {
		select {
		case s._allProducersIdleCh <- true: // sent successfully
		default: // no receiver, ignore
		}
	}
}

func (s *producerSupervisor) checkIfAllProducersTerminated() {
	allTerminated := true

	for _, producer := range s._producers.values() {
		if !producer.isTerminated() {
			allTerminated = false
			break
		}
	}

	if allTerminated {
		s._terminatedOnce.Do(func() {
			s._allProducersTerminatedCh <- true
		})
	}
}

func (s *producerSupervisor) allProducersIdle() <-chan bool {
	return s._allProducersIdleCh
}

func (s *producerSupervisor) producersChanged() <-chan map[string]*producer {
	return s._producersChangeCh
}

func (s *producerSupervisor) allProducersTerminated() <-chan bool {
	return s._allProducersTerminatedCh
}
