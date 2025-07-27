package broadway

import (
	"context"
	"sync"
)

// batcherSupervisor manages a collection of batchers, handling their lifecycle
// and ensuring fault tolerance by restarting batchers that panic during execution.
type batcherSupervisor interface {
	// run starts the batcher supervisor with the provided context, which starts the batchers
	// based on its config.
	//
	// Parameters:
	//   - ctx: The context provided when starting the pipeline
	//
	// Returns:
	//   - A map of batcher ids to batcher instances that are currently active.
	run(ctx context.Context) map[string]batcher

	// terminate stops all batchers managed by this supervisor.
	terminate()

	batchersChanged() <-chan map[string]batcher
	allBatchersTerminated() <-chan bool
}

type internalBatcherSupervisor struct {
	config                  []BatcherConfig
	batchers                *concurrentMap[string, batcher]
	mu                      sync.Mutex
	batchersChangeCh        chan map[string]batcher
	allBatchersTerminatedCh chan bool
}

func newBatcherSupervisor(config []BatcherConfig) batcherSupervisor {
	return &internalBatcherSupervisor{
		config:                  config,
		batchers:                newConcurrentMap[string, batcher](),
		mu:                      sync.Mutex{},
		batchersChangeCh:        make(chan map[string]batcher),
		allBatchersTerminatedCh: make(chan bool),
	}
}

func (s *internalBatcherSupervisor) run(
	ctx context.Context,
) map[string]batcher {

	if s.config == nil {
		return nil
	}

	for _, batcherConfig := range s.config {
		b := newBatcher(batcherConfig)
		s.batchers.set(b.config().Name, b)
		b.run(ctx)

		go func(b batcher) {
			if panic, ok := <-b.terminated(); ok {
				if panic != nil {
					s.handleBatcherPanic(b, ctx)
				} else {
					s.checkIfAllBatchersTerminated()
				}
			}

		}(b)
	}

	return s.batchers.toMap()
}

func (s *internalBatcherSupervisor) terminate() {
	for _, b := range s.batchers.toMap() {
		b.terminate()
	}

	close(s.batchersChangeCh)
}

func (s *internalBatcherSupervisor) handleBatcherPanic(b batcher, ctx context.Context) {
	newBatcher := newBatcher(b.config())
	s.batchers.set(b.config().Name, newBatcher)
	newBatcher.run(ctx)

	go func(b batcher) {
		if panic, ok := <-b.terminated(); ok {
			if panic != nil {
				s.handleBatcherPanic(b, ctx)
			} else {
				s.checkIfAllBatchersTerminated()
			}
		}
	}(newBatcher)

	s.batchersChangeCh <- s.batchers.toMap()
}

func (s *internalBatcherSupervisor) allBatchersTerminated() <-chan bool {
	return s.allBatchersTerminatedCh
}

func (s *internalBatcherSupervisor) checkIfAllBatchersTerminated() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.allBatchersTerminatedCh == nil {
		return
	}

	allTerminated := true

	for _, b := range s.batchers.toMap() {
		if !b.isTerminated() {
			allTerminated = false
		}
	}

	if allTerminated {
		s.allBatchersTerminatedCh <- true
		close(s.allBatchersTerminatedCh)
		s.allBatchersTerminatedCh = nil
	}
}

func (s *internalBatcherSupervisor) batchersChanged() <-chan map[string]batcher {
	return s.batchersChangeCh
}
