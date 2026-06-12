package broadway

type MessagePayload any
type MessageMetadata any

// Message represents a unit of data flowing through the Broadway pipeline.
// It contains the actual payload and metadata for routing and acknowledgment.
type Message struct {
	Payload  MessagePayload
	Batcher  string
	BatchKey string

	metadata     MessageMetadata
	partitionKey string
	ack          Acknowledger
	acked        bool
	error        error
}

// NewMessage creates a Message with the given payload and optional metadata.
func NewMessage(payload MessagePayload, metadata MessageMetadata) *Message {
	return &Message{Payload: payload, metadata: metadata}
}

func (m *Message) Metadata() MessageMetadata { return m.metadata }
