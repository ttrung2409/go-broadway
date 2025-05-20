package broadway

type MessagePayload any

type Message struct {
	Payload      MessagePayload
	Acknowledger Acknowledger
	Batcher      string
	BatchKey     string
}
