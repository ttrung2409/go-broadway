package broadway

import (
	"context"
	"sync"
)

// messageProcessorSupervisor manages a pool of message processors, handling their
// lifecycle and coordination with producers and batchers.
type messageProcessorSupervisor interface {
	// run starts the message processor supervisor with the provided context.
	// It in turn starts the configured number of message processors,
	// connecting them to the provided producers and batchers.
	//
	// Parameters:
	//   - ctx: The provided context when starting the pipeline
	//   - producers: A map of producer IDs to producer instances that the processors will receive messages from.
	//   - batchers: A map of batcher names to batcher instances that the processors will send processed messages to.
	run(ctx context.Context, producers map[string]producer, batchers map[string]batcher)

	// terminate stops all message processors managed by this supervisor.
	// This should be called when shutting down the pipeline to ensure proper cleanup.
	terminate()

	// setProducers updates the producers for all message processors managed by this supervisor.
	// This is called when the set of available producers changes, such as when a producer
	// fails and is replaced.
	//
	// Parameters:
	//   - producers: A map of producer IDs to producer instances that will replace
	//     the current set of producers for all managed message processors.
	setProducers(producers map[string]producer)

	// setBatchers updates the batchers for all message processors managed by this supervisor.
	// This is called when the set of available batchers changes, such as when a batcher
	// fails and is replaced.
	//
	// Parameters:
	//   - batchers: A map of batcher names to batcher instances that will replace
	//     the current set of batchers for all managed message processors.
	setBatchers(batchers map[string]batcher)

	// resolve attempts to find the message processor responsible for a given partition key.
	//
	// Parameters:
	//   - partitionKey: The partition key for which to find the responsible processor.
	//
	// Returns:
	//   - The ID of the message processor responsible for the partition key, or an empty string if not found.
	//   - A boolean indicating whether a processor was found for the given partition key.
	resolve(partitionKey string) (string, bool)

	allProcessorsTerminated() <-chan bool
}

type internalMessageProcessorSupervisor struct {
	config                    MessageProcessorConfig
	hr                        *hashRing[messageProcessor]
	mu                        sync.Mutex
	allProcessorsTerminatedCh chan bool
}

func newMessageProcessorSupervisor(
	config MessageProcessorConfig,
) messageProcessorSupervisor {
	return &internalMessageProcessorSupervisor{
		config:                    config,
		hr:                        newHashRing[messageProcessor](),
		mu:                        sync.Mutex{},
		allProcessorsTerminatedCh: make(chan bool),
	}
}

func (s *internalMessageProcessorSupervisor) run(
	ctx context.Context,
	producers map[string]producer,
	batchers map[string]batcher,
) {
	for i := 0; i < s.config.Concurrency; i++ {
		mp := newMessageProcessor(s.config.Processor, s.config, producers, batchers)
		mp.run(context.WithValue(ctx, MessageProcessorIdContextKey, mp.id()))

		go func(mp messageProcessor) {
			if panic, ok := <-mp.terminated(); ok {
				if panic != nil {
					s.handleProcessorPanic(mp, producers, batchers, ctx)
				} else {
					s.checkIfAllProcessorsTerminated()
				}
			}

		}(mp)

		s.hr.addNode(mp)
	}
}

func (s *internalMessageProcessorSupervisor) terminate() {
	s.mu.Lock()
	defer s.mu.Unlock()

	processors := s.hr.getAllNodes()
	for _, p := range processors {
		p.terminate()
	}
}

func (s *internalMessageProcessorSupervisor) handleProcessorPanic(
	processor messageProcessor,
	producers map[string]producer,
	batchers map[string]batcher,
	ctx context.Context,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.hr.removeNode(processor)

	newProcessor := newMessageProcessor(processor.processor(), s.config, producers, batchers)
	newProcessor.run(context.WithValue(ctx, MessageProcessorIdContextKey, newProcessor.id()))

	keySharingProcessor, _ := s.hr.getNextNode(newProcessor)

	if keySharingProcessor != nil {
		keySharingProcessor.pause()
	}

	s.hr.addNode(newProcessor)

	if keySharingProcessor != nil {
		keySharingProcessor.resume()
	}

	go func(p messageProcessor) {
		if panic, ok := <-p.terminated(); ok {
			if panic != nil {
				s.handleProcessorPanic(p, producers, batchers, ctx)
			} else {
				s.checkIfAllProcessorsTerminated()
			}
		}
	}(newProcessor)
}

func (s *internalMessageProcessorSupervisor) setProducers(producers map[string]producer) {
	processors := s.hr.getAllNodes()

	for _, p := range processors {
		p.setProducers(producers)
	}
}

func (s *internalMessageProcessorSupervisor) setBatchers(batchers map[string]batcher) {
	processors := s.hr.getAllNodes()

	for _, p := range processors {
		p.setBatchers(batchers)
	}
}

func (s *internalMessageProcessorSupervisor) resolve(partitionKey string) (string, bool) {
	if processor, ok := s.hr.getNode(partitionKey); ok {
		return processor.id(), true
	}

	return "", false
}

func (s *internalMessageProcessorSupervisor) allProcessorsTerminated() <-chan bool {
	return s.allProcessorsTerminatedCh
}

func (s *internalMessageProcessorSupervisor) checkIfAllProcessorsTerminated() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.allProcessorsTerminatedCh == nil {
		return
	}

	processors := s.hr.getAllNodes()

	allTerminated := true

	for _, processor := range processors {
		if !processor.isTerminated() {
			allTerminated = false
		}
	}

	if allTerminated {
		s.allProcessorsTerminatedCh <- true
		close(s.allProcessorsTerminatedCh)
		s.allProcessorsTerminatedCh = nil
	}
}
