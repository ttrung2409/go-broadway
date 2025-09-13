package broadway

import (
	"context"
	"sync"
)

// batcherSupervisor manages a collection of batchers, handling their lifecycle
// and ensuring fault tolerance by restarting batchers that panic during execution.
type batcherSupervisor struct {
	config []BatcherConfig

	_batchers                *concurrentMap[string, *batcher]
	_mu                      sync.Mutex
	_allBatchersTerminatedCh chan bool
}

func newBatcherSupervisor(config []BatcherConfig) *batcherSupervisor {
	return &batcherSupervisor{
		config: config,

		_batchers:                newConcurrentMap[string, *batcher](),
		_mu:                      sync.Mutex{},
		_allBatchersTerminatedCh: make(chan bool),
	}
}

// run starts the batcher supervisor with the provided context, which starts the batchers
// based on its config.
//
// Parameters:
//   - ctx: The context provided when starting the pipeline
//
// Returns:
//   - A map of batcher ids to batcher instances that are currently active.
func (s *batcherSupervisor) run(
	ctx context.Context,
) map[string]*batcher {

	if s.config == nil {
		return nil
	}

	for _, batcherConfig := range s.config {
		b := newBatcher(batcherConfig)
		s._batchers.set(b.config.Name, b)
		b.run(ctx)

		go func(b *batcher) {
			<-b.terminated()
			s.checkIfAllBatchersTerminated()
		}(b)
	}

	return s._batchers.toMap()
}

// terminate stops all batchers managed by this supervisor.
func (s *batcherSupervisor) terminate() {
	for _, b := range s._batchers.toMap() {
		b.terminate()
	}

}

func (s *batcherSupervisor) allBatchersTerminated() <-chan bool {
	return s._allBatchersTerminatedCh
}

func (s *batcherSupervisor) checkIfAllBatchersTerminated() {
	s._mu.Lock()
	defer s._mu.Unlock()

	if s._allBatchersTerminatedCh == nil {
		return
	}

	allTerminated := true

	for _, b := range s._batchers.toMap() {
		if !b.isTerminated() {
			allTerminated = false
		}
	}

	if allTerminated {
		s._allBatchersTerminatedCh <- true
		close(s._allBatchersTerminatedCh)
		s._allBatchersTerminatedCh = nil
	}
}
