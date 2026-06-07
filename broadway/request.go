package broadway

import "sync/atomic"

type request struct {
	demand             int
	messageProcessorId string

	_responseChan chan []*Message
	_closedChan   chan struct{}
	_closed       atomic.Bool
}

func newRequest(messageProcessorId string, demand int) *request {
	return &request{
		demand:             demand,
		messageProcessorId: messageProcessorId,
		_responseChan:      make(chan []*Message),
		_closedChan:        make(chan struct{}),
	}
}

func (r *request) isClosed() bool {
	return r._closed.Load()
}

func (r *request) close() {
	if r._closed.Swap(true) {
		return
	}
	close(r._closedChan)
}

// reply sends messages to the consumer. Returns false if the request is closed.
func (r *request) reply(messages []*Message) bool {
	if r._closed.Load() {
		return false
	}

	select {
	case r._responseChan <- messages:
		return true
	case <-r._closedChan:
		return false
	}
}

func (r *request) response() <-chan []*Message {
	return r._responseChan
}
