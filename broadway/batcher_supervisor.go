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

	onBatchersChange() <-chan map[string]batcher
	onAllBatchersTerminated() <-chan bool
}

type internalBatcherSupervisor struct {
	config                   []BatcherConfig
	batchers                 *concurrentMap[string, batcher]
	mu                       sync.Mutex
	_onBatchersChange        chan map[string]batcher
	_onAllBatchersTerminated chan bool
}

func newBatcherSupervisor(config []BatcherConfig) batcherSupervisor {
	return &internalBatcherSupervisor{
		config:                   config,
		batchers:                 newConcurrentMap[string, batcher](),
		mu:                       sync.Mutex{},
		_onBatchersChange:        make(chan map[string]batcher),
		_onAllBatchersTerminated: make(chan bool),
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
			if panic, ok := <-b.onTerminated(); ok {
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

	close(s._onBatchersChange)
}

func (s *internalBatcherSupervisor) handleBatcherPanic(b batcher, ctx context.Context) {
	newBatcher := newBatcher(b.config())
	s.batchers.set(b.config().Name, newBatcher)
	newBatcher.run(ctx)

	go func(b batcher) {
		if panic, ok := <-b.onTerminated(); ok {
			if panic != nil {
				s.handleBatcherPanic(b, ctx)
			} else {
				s.checkIfAllBatchersTerminated()
			}
		}
	}(newBatcher)

	s._onBatchersChange <- s.batchers.toMap()
}

func (s *internalBatcherSupervisor) onAllBatchersTerminated() <-chan bool {
	return s._onAllBatchersTerminated
}

func (s *internalBatcherSupervisor) checkIfAllBatchersTerminated() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s._onAllBatchersTerminated == nil {
		return
	}

	allTerminated := true

	for _, b := range s.batchers.toMap() {
		if !b.isTerminated() {
			allTerminated = false
		}
	}

	if allTerminated {
		s._onAllBatchersTerminated <- true
		close(s._onAllBatchersTerminated)
		s._onAllBatchersTerminated = nil
	}
}

func (s *internalBatcherSupervisor) onBatchersChange() <-chan map[string]batcher {
	return s._onBatchersChange
}
