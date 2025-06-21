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
	onAllProcessorsTerminated chan bool
}

func newMessageProcessorSupervisor(config MessageProcessorConfig) *messageProcessorSupervisor {
	return &messageProcessorSupervisor{
		config:                    config,
		hr:                        newHashRing[*messageProcessor](),
		mu:                        sync.Mutex{},
		onAllProcessorsTerminated: make(chan bool),
	}
}

// Run starts the message processor supervisor with the provided context.
// It in turn starts the configured number of message processors,
// connecting them to the provided producers and batchers.
//
// Parameters:
//   - ctx: The provided context when starting the pipeline
//   - producers: A map of producer IDs to producer instances that the processors will receive messages from.
//   - batchers: A map of batcher names to batcher instances that the processors will send processed messages to.
func (s *messageProcessorSupervisor) Run(
	ctx context.Context,
	producers map[string]*producer,
	batchers map[string]*batcher,
) {
	for i := 0; i < s.config.Concurrency; i++ {
		mp := newMessageProcessor(s.config, producers, batchers)
		mp.Run(ctx)

		go func(mp *messageProcessor) {
			if panic, ok := <-mp.OnTerminated(); ok {
				if panic != nil {
					s.handleProcessorPanic(mp, producers, batchers, ctx)
				} else {
					s.checkIfAllProcessorsTerminated()
				}
			}

		}(mp)

		s.hr.AddNode(mp)
	}
}

// Terminate stops all message processors managed by this supervisor.
// This should be called when shutting down the pipeline to ensure proper cleanup.
func (s *messageProcessorSupervisor) Terminate() {
	processors := s.hr.GetAllNodes()
	for _, p := range processors {
		p.Terminate()
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

	s.hr.RemoveNode(processor)

	newProcessor := newMessageProcessor(s.config, producers, batchers)
	newProcessor.Run(ctx)

	keySharingProcessor, _ := s.hr.GetNextNode(newProcessor)

	if keySharingProcessor != nil {
		keySharingProcessor.Pause()
	}

	s.hr.AddNode(newProcessor)

	if keySharingProcessor != nil {
		keySharingProcessor.Resume()
	}

	go func(p *messageProcessor) {
		if panic, ok := <-p.OnTerminated(); ok {
			if panic != nil {
				s.handleProcessorPanic(p, producers, batchers, ctx)
			} else {
				s.checkIfAllProcessorsTerminated()
			}
		}
	}(newProcessor)
}

// SetProducers updates the producers for all message processors managed by this supervisor.
// This is called when the set of available producers changes, such as when a producer
// fails and is replaced.
//
// Parameters:
//   - producers: A map of producer IDs to producer instances that will replace
//     the current set of producers for all managed message processors.
func (s *messageProcessorSupervisor) SetProducers(producers map[string]*producer) {
	processors := s.hr.GetAllNodes()

	for _, p := range processors {
		p.SetProducers(producers)
	}
}

// SetBatchers updates the batchers for all message processors managed by this supervisor.
// This is called when the set of available batchers changes, such as when a batcher
// fails and is replaced.
//
// Parameters:
//   - batchers: A map of batcher names to batcher instances that will replace
//     the current set of batchers for all managed message processors.
func (s *messageProcessorSupervisor) SetBatchers(batchers map[string]*batcher) {
	processors := s.hr.GetAllNodes()

	for _, p := range processors {
		p.SetBatchers(batchers)
	}
}

// Resolve attempts to find the message processor responsible for a given partition key.
//
// Parameters:
//   - partitionKey: The partition key for which to find the responsible processor.
//
// Returns:
//   - The ID of the message processor responsible for the partition key, or an empty string if not found.
//   - A boolean indicating whether a processor was found for the given partition key.
func (s *messageProcessorSupervisor) Resolve(partitionKey string) (string, bool) {
	if processor, ok := s.hr.GetNode(partitionKey); ok {
		return processor.Id, true
	}

	return "", false
}

func (s *messageProcessorSupervisor) OnAllProcessorsTerminated() <-chan bool {
	return s.onAllProcessorsTerminated
}

func (s *messageProcessorSupervisor) checkIfAllProcessorsTerminated() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.onAllProcessorsTerminated == nil {
		return
	}

	processors := s.hr.GetAllNodes()

	allTerminated := true

	for _, processor := range processors {
		if !processor.IsTerminated() {
			allTerminated = false
		}
	}

	if allTerminated {
		s.onAllProcessorsTerminated <- true
		close(s.onAllProcessorsTerminated)
		s.onAllProcessorsTerminated = nil
	}
}
