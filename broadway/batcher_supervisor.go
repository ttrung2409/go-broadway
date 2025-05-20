package broadway

import "context"

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

func (s *batcherSupervisor) Run(
	ctx context.Context,
) (map[string]*batcher, <-chan map[string]*batcher) {

	if s.config == nil {
		return nil, nil
	}

	for _, batcherConfig := range s.config {
		batcher := newBatcher(batcherConfig)
		s.batchers[batcherConfig.Name] = batcher
		batcher.Run(ctx)
	}

	return s.batchers, s.onBatchersChange
}

func (s *batcherSupervisor) Terminate() {
	for _, b := range s.batchers {
		b.Terminate()
	}
}

func (s *batcherSupervisor) handleBatcherPanic(b *batcher, ctx context.Context) {

	newBatcher := newBatcher(b.config)
	s.batchers[b.config.Name] = newBatcher
	onTerminated := newBatcher.Run(ctx)

	go func(newBatcher *batcher, onTerminated <-chan any) {
		err := <-onTerminated

		if err != nil {
			s.handleBatcherPanic(newBatcher, ctx)
		}
	}(newBatcher, onTerminated)

	s.onBatchersChange <- s.batchers
}
