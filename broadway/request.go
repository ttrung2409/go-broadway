package broadway

import "sync"

type request struct {
	Demand   int
	Response chan []*Message
	closed   bool
	mu       sync.Mutex
}

func newRequest(demand int) *request {
	return &request{
		Demand:   demand,
		Response: make(chan []*Message),
		mu:       sync.Mutex{},
	}
}

func (r *request) Closed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

func (r *request) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return
	}

	r.closed = true
	close(r.Response)
}

func (r *request) Send(messages []*Message) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return false
	}

	r.Response <- messages

	return true
}
