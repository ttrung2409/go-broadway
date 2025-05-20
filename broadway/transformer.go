package broadway

type Transformer func(data MessagePayload) *Message

func defaultTransformer() Transformer {
	return func(payload MessagePayload) *Message {
		return &Message{Payload: payload, Acknowledger: defaultAck()}
	}
}
