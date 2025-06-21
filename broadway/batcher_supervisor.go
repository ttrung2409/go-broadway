package broadway

import (
	"context"
	"sync"
)

// batcherSupervisor manages a collection of batchers, handling their lifecycle
// and ensuring fault tolerance by restarting batchers that panic during execution.
type batcherSupervisor struct {
	config                  []BatcherConfig
	batchers                *concurrentMap[string, *batcher]
	mu                      sync.Mutex
	onBatchersChange        chan map[string]*batcher
	onAllBatchersTerminated chan bool
}

func newBatcherSupervisor(config []BatcherConfig) *batcherSupervisor {
	return &batcherSupervisor{
		config:                  config,
		batchers:                newConcurrentMap[string, *batcher](),
		mu:                      sync.Mutex{},
		onBatchersChange:        make(chan map[string]*batcher),
		onAllBatchersTerminated: make(chan bool),
	}
}

// Run starts the batcher supervisor with the provided context, which starts the batchers
// based on its config.
//
// Parameters:
//   - ctx: The context provided when starting the pipeline
//
// Returns:
//   - A map of batcher ids to batcher instances that are currently active.
func (s *batcherSupervisor) Run(
	ctx context.Context,
) map[string]*batcher {

	if s.config == nil {
		return nil
	}

	for _, batcherConfig := range s.config {
		b := newBatcher(batcherConfig)
		s.batchers.Set(b.Name(), b)
		b.Run(ctx)

		go func(b *batcher) {
			if panic, ok := <-b.OnTerminated(); ok {
				if panic != nil {
					s.handleBatcherPanic(b, ctx)
				} else {
					s.checkIfAllBatchersTerminated()
				}
			}

		}(b)
	}

	return s.batchers.ToMap()
}

// Terminate stops all batchers managed by this supervisor.
func (s *batcherSupervisor) Terminate() {
	for _, b := range s.batchers.ToMap() {
		b.Terminate()
	}

	close(s.onBatchersChange)
}

func (s *batcherSupervisor) handleBatcherPanic(b *batcher, ctx context.Context) {
	newBatcher := newBatcher(b.config)
	s.batchers.Set(b.config.Name, newBatcher)
	newBatcher.Run(ctx)

	go func(b *batcher) {
		if panic, ok := <-b.OnTerminated(); ok {
			if panic != nil {
				s.handleBatcherPanic(b, ctx)
			} else {
				s.checkIfAllBatchersTerminated()
			}
		}
	}(newBatcher)

	s.onBatchersChange <- s.batchers.ToMap()
}

func (s *batcherSupervisor) OnAllBatchersTerminated() <-chan bool {
	return s.onAllBatchersTerminated
}

func (s *batcherSupervisor) checkIfAllBatchersTerminated() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.onAllBatchersTerminated == nil {
		return
	}

	allTerminated := true

	for _, b := range s.batchers.ToMap() {
		if !b.IsTerminated() {
			allTerminated = false
		}
	}

	if allTerminated {
		s.onAllBatchersTerminated <- true
		close(s.onAllBatchersTerminated)
		s.onAllBatchersTerminated = nil
	}
}

func (s *batcherSupervisor) OnBatchersChange() <-chan map[string]*batcher {
	return s.onBatchersChange
}
