package broadway

import (
	"sync"
)

type request struct {
	Demand             int
	MessageProcessorId string

	response chan []*Message
	closed   bool
	mu       sync.Mutex
}

func newRequest(messageProcessorId string, demand int) *request {
	return &request{
		Demand:             demand,
		MessageProcessorId: messageProcessorId,
		response:           make(chan []*Message),
		mu:                 sync.Mutex{},
	}
}

func (r *request) IsClosed() bool {
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
	close(r.response)
}

func (r *request) Reply(messages []*Message) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return false
	}

	r.response <- messages

	return true
}

func (r *request) Response() <-chan []*Message {
	return r.response
}
