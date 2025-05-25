package broadway

type Acknowledger interface {
	// Ack acknowledges the processing of a batch of messages. It receives
	// the processed messages and any error that occurred during processing.
	//
	// Parameters:
	//   - messages: The processed messages to acknowledge.
	//   - err: Any error that occurred during processing, or nil if processing was successful.
	Ack(messages []*Message, err error)
}

type defaultAcknowledger struct{}

func (a *defaultAcknowledger) Ack(messages []*Message, err error) {
}

func defaultAck() Acknowledger {
	return &defaultAcknowledger{}
}
