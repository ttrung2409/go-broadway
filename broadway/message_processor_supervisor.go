package broadway

import (
	"context"
	"sync"
)

// messageProcessorSupervisor manages a pool of message processors, handling their
// lifecycle and coordination with producers and batchers.
type messageProcessorSupervisor struct {
	config                    MessageProcessorConfig
	hr                        *hashRing[*messageProcessor]
	mu                        sync.Mutex
	allProcessorsTerminatedCh chan bool
}

func newMessageProcessorSupervisor(
	config MessageProcessorConfig,
) *messageProcessorSupervisor {
	return &messageProcessorSupervisor{
		config:                    config,
		hr:                        newHashRing[*messageProcessor](),
		mu:                        sync.Mutex{},
		allProcessorsTerminatedCh: make(chan bool),
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
func (s *messageProcessorSupervisor) run(
	ctx context.Context,
	producers map[string]*producer,
	batchers map[string]*batcher,
) {
	for i := 0; i < s.config.Concurrency; i++ {
		mp := newMessageProcessor(s.config.Processor, s.config, producers, batchers)
		mp.run(ctx)

		go func(mp *messageProcessor) {
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

// terminate stops all message processors managed by this supervisor.
// This should be called when shutting down the pipeline to ensure proper cleanup.
func (s *messageProcessorSupervisor) terminate() {
	s.mu.Lock()
	defer s.mu.Unlock()

	processors := s.hr.getAllNodes()
	for _, p := range processors {
		p.terminate()
	}
}

func (s *messageProcessorSupervisor) handleProcessorPanic(
	processor *messageProcessor,
	producers map[string]*producer,
	batchers map[string]*batcher,
	ctx context.Context,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.hr.removeNode(processor)

	newProcessor := newMessageProcessor(processor.processor, s.config, producers, batchers)
	newProcessor.run(ctx)

	keySharingProcessor, _ := s.hr.getNextNode(newProcessor)

	if keySharingProcessor != nil {
		keySharingProcessor.pause()
	}

	s.hr.addNode(newProcessor)

	if keySharingProcessor != nil {
		keySharingProcessor.resume()
	}

	go func(p *messageProcessor) {
		if panic, ok := <-p.terminated(); ok {
			if panic != nil {
				s.handleProcessorPanic(p, producers, batchers, ctx)
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
	processors := s.hr.getAllNodes()

	for _, p := range processors {
		p.setProducers(producers)
	}
}

// setBatchers updates the batchers for all message processors managed by this supervisor.
// This is called when the set of available batchers changes, such as when a batcher
// fails and is replaced.
//
// Parameters:
//   - batchers: A map of batcher names to batcher instances that will replace
//     the current set of batchers for all managed message processors.
func (s *messageProcessorSupervisor) setBatchers(batchers map[string]*batcher) {
	processors := s.hr.getAllNodes()

	for _, p := range processors {
		p.setBatchers(batchers)
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
	if processor, ok := s.hr.getNode(partitionKey); ok {
		return processor.id, true
	}

	return "", false
}

func (s *messageProcessorSupervisor) allProcessorsTerminated() <-chan bool {
	return s.allProcessorsTerminatedCh
}

func (s *messageProcessorSupervisor) checkIfAllProcessorsTerminated() {
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
