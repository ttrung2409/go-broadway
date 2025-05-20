package broadway

type Acknowledger interface {
	Ack(messages []*Message, err error)
}

type defaultAcknowledger struct{}

func (a *defaultAcknowledger) Ack(messages []*Message, err error) {
}

func defaultAck() Acknowledger {
	return &defaultAcknowledger{}
}
