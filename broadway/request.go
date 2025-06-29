package broadway

import (
	"sync"
)

type request struct {
	demand             int
	messageProcessorId string
	responseChan       chan []*Message
	closed             bool
	mu                 sync.Mutex
}

func newRequest(messageProcessorId string, demand int) *request {
	return &request{
		demand:             demand,
		messageProcessorId: messageProcessorId,
		responseChan:       make(chan []*Message),
		mu:                 sync.Mutex{},
	}
}

func (r *request) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

func (r *request) close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return
	}

	r.closed = true
	close(r.responseChan)
}

func (r *request) reply(messages []*Message) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return false
	}

	r.responseChan <- messages

	return true
}

func (r *request) response() <-chan []*Message {
	return r.responseChan
}
