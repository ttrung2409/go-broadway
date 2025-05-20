package broadway

import "context"

type producerSupervisor struct {
	config            ProducerConfig
	producers         map[string]*producer
	onProducersChange chan map[string]*producer
}

func newProducerSupervisor(config ProducerConfig) *producerSupervisor {
	return &producerSupervisor{
		config:            config,
		producers:         make(map[string]*producer),
		onProducersChange: make(chan map[string]*producer),
	}
}

func (s *producerSupervisor) Run(
	ctx context.Context,
) (map[string]*producer, <-chan map[string]*producer) {

	for i := 0; i < s.config.Concurrency; i++ {
		p := newProducer(s.config)
		s.producers[p.Id] = p
		onTerminated := p.Run(ctx)

		go func(p *producer, onTerminated <-chan any) {
			err := <-onTerminated

			if err != nil {
				s.handleProducerPanic(p, ctx)
			}
		}(p, onTerminated)
	}

	return s.producers, s.onProducersChange
}

func (s *producerSupervisor) Terminate() {
	for _, p := range s.producers {
		p.Terminate()
	}
}

func (s *producerSupervisor) handleProducerPanic(p *producer, ctx context.Context) {
	delete(s.producers, p.Id)
	newProducer := newProducer(s.config)
	s.producers[newProducer.Id] = newProducer
	onTerminated := newProducer.Run(ctx)

	go func(newProducer *producer, onTerminated <-chan any) {
		err := <-onTerminated

		if err != nil {
			s.handleProducerPanic(newProducer, ctx)
		}
	}(newProducer, onTerminated)

	s.onProducersChange <- s.producers
}
