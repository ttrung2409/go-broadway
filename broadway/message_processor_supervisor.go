package broadway

import (
	"context"
	"sync"
)

// messageProcessorSupervisor manages a pool of message processors, handling their
// lifecycle and coordination with producers and batchers.
type messageProcessorSupervisor struct {
	config MessageProcessorConfig

	_hr                        *hashRing[*messageProcessor]
	_mu                        sync.Mutex
	_allProcessorsTerminatedCh chan bool
	_terminatedOnce            sync.Once
	_producers                 *concurrentMap[string, *producer]
	_batchers                  *concurrentMap[string, *batcher]
}

func newMessageProcessorSupervisor(
	config MessageProcessorConfig,
	producers map[string]*producer,
	batchers map[string]*batcher,
) *messageProcessorSupervisor {
	return &messageProcessorSupervisor{
		config: config,

		_hr:                        newHashRing[*messageProcessor](),
		_mu:                        sync.Mutex{},
		_allProcessorsTerminatedCh: make(chan bool),
		_producers:                 newConcurrentMap(producers),
		_batchers:                  newConcurrentMap(batchers),
	}
}

// run starts the message processor supervisor with the provided context.
// It in turn starts the configured number of message processors,
// connecting them to the provided producers and batchers.
//
// Parameters:
//   - ctx: The provided context when starting the pipeline
//   - producers: A map of producer IDs to producer instances that the processors will receive messages from.
//   - batchers: A map of batcher names to batcher instances that the processors will send processed messages to.
func (s *messageProcessorSupervisor) run(ctx context.Context) {
	for i := 0; i < s.config.Concurrency; i++ {
		mp := newMessageProcessor(
			s.config.Processor,
			s.config,
			s._producers.toMap(),
			s._batchers.toMap(),
		)
		mp.run(ctx)

		go func(mp *messageProcessor) {
			if panic, ok := <-mp.terminated(); ok {
				if panic != nil {
					s.handleProcessorPanic(mp, ctx)
				} else {
					s.checkIfAllProcessorsTerminated()
				}
			}

		}(mp)

		s._hr.addNode(mp)
	}
}

// terminate stops all message processors managed by this supervisor.
// This should be called when shutting down the pipeline to ensure proper cleanup.
func (s *messageProcessorSupervisor) terminate() {
	s._mu.Lock()
	defer s._mu.Unlock()

	processors := s._hr.getAllNodes()
	for _, p := range processors {
		p.terminate()
	}
}

func (s *messageProcessorSupervisor) handleProcessorPanic(
	processor *messageProcessor,
	ctx context.Context,
) {
	s._mu.Lock()
	defer s._mu.Unlock()

	s._hr.removeNode(processor)

	newProcessor := newMessageProcessor(
		processor.processor,
		s.config,
		s._producers.toMap(),
		s._batchers.toMap(),
	)
	newProcessor.run(ctx)

	keySharingProcessor, _ := s._hr.getNextNode(newProcessor)

	if keySharingProcessor != nil {
		keySharingProcessor.pause()
	}

	s._hr.addNode(newProcessor)

	if keySharingProcessor != nil {
		keySharingProcessor.resume()
	}

	go func(p *messageProcessor) {
		if panic, ok := <-p.terminated(); ok {
			if panic != nil {
				s.handleProcessorPanic(p, ctx)
			} else {
				s.checkIfAllProcessorsTerminated()
			}
		}
	}(newProcessor)
}

// setProducers updates the producers for all message processors managed by this supervisor.
// This is called when the set of available producers changes, such as when a producer
// fails and is replaced.
//
// Parameters:
//   - producers: A map of producer IDs to producer instances that will replace
//     the current set of producers for all managed message processors.
func (s *messageProcessorSupervisor) setProducers(producers map[string]*producer) {
	s._producers.reset(producers)

	processors := s._hr.getAllNodes()

	for _, p := range processors {
		p.setProducers(s._producers.toMap())
	}
}

// resolve attempts to find the message processor responsible for a given partition key.
//
// Parameters:
//   - partitionKey: The partition key for which to find the responsible processor.
//
// Returns:
//   - The ID of the message processor responsible for the partition key, or an empty string if not found.
//   - A boolean indicating whether a processor was found for the given partition key.
func (s *messageProcessorSupervisor) resolve(partitionKey string) (string, bool) {
	if processor, ok := s._hr.getNode(partitionKey); ok {
		return processor.id, true
	}

	return "", false
}

func (s *messageProcessorSupervisor) allProcessorsTerminated() <-chan bool {
	return s._allProcessorsTerminatedCh
}

func (s *messageProcessorSupervisor) checkIfAllProcessorsTerminated() {
	s._mu.Lock()
	defer s._mu.Unlock()

	allTerminated := true
	for _, processor := range s._hr.getAllNodes() {
		if !processor.isTerminated() {
			allTerminated = false
			break
		}
	}

	if allTerminated {
		s._terminatedOnce.Do(func() {
			s._allProcessorsTerminatedCh <- true
		})
	}
}
