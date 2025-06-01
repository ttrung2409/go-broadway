package broadway

import "context"

// batcherSupervisor manages a collection of batchers, handling their lifecycle
// and ensuring fault tolerance by restarting batchers that panic during execution.
type batcherSupervisor struct {
	config           []BatcherConfig
	batchers         map[string]*batcher
	onBatchersChange chan map[string]*batcher
}

func newBatcherSupervisor(config []BatcherConfig) *batcherSupervisor {
	return &batcherSupervisor{
		config:           config,
		batchers:         make(map[string]*batcher),
		onBatchersChange: make(chan map[string]*batcher),
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
//   - A channel that will receive updates to the batchers when a batcher panics
//     and a new batcher is created.
func (s *batcherSupervisor) Run(
	ctx context.Context,
) (map[string]*batcher, <-chan map[string]*batcher) {

	if s.config == nil {
		return nil, nil
	}

	for _, batcherConfig := range s.config {
		b := newBatcher(batcherConfig)
		s.batchers[batcherConfig.Name] = b
		onTerminated := b.Run(ctx)

		go func(b *batcher, onTerminated <-chan any) {
			err, ok := <-onTerminated

			if ok && err != nil {
				s.handleBatcherPanic(b, ctx)
			}
		}(b, onTerminated)
	}

	return s.batchers, s.onBatchersChange
}

// Terminate stops all batchers managed by this supervisor.
func (s *batcherSupervisor) Terminate() {
	for _, b := range s.batchers {
		b.Terminate()
	}

	close(s.onBatchersChange)
}

func (s *batcherSupervisor) handleBatcherPanic(b *batcher, ctx context.Context) {

	newBatcher := newBatcher(b.config)
	s.batchers[b.config.Name] = newBatcher
	onTerminated := newBatcher.Run(ctx)

	go func(newBatcher *batcher, onTerminated <-chan any) {
		err, ok := <-onTerminated

		if ok && err != nil {
			s.handleBatcherPanic(newBatcher, ctx)
		}
	}(newBatcher, onTerminated)

	s.onBatchersChange <- s.batchers
}
