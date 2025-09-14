package broadway

import (
	"sync"
)

type request struct {
	demand             int
	messageProcessorId string

	_responseChan chan []*Message
	_closed       bool
	_mu           sync.Mutex
}

func newRequest(messageProcessorId string, demand int) *request {
	return &request{
		demand:             demand,
		messageProcessorId: messageProcessorId,
		_responseChan:      make(chan []*Message),
		_mu:                sync.Mutex{},
	}
}

func (r *request) isClosed() bool {
	r._mu.Lock()
	defer r._mu.Unlock()
	return r._closed
}

func (r *request) close() {
	r._mu.Lock()
	defer r._mu.Unlock()

	if r._closed {
		return
	}

	r._closed = true
	close(r._responseChan)
}

func (r *request) reply(messages []*Message) bool {
	r._mu.Lock()
	defer r._mu.Unlock()

	if r._closed {
		return false
	}

	r._responseChan <- messages

	return true
}

func (r *request) response() <-chan []*Message {
	return r._responseChan
}
