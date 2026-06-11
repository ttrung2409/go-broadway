package broadway

type MessagePayload any

// Message represents a unit of data flowing through the Broadway pipeline.
// It contains the actual payload and metadata for routing and acknowledgment.
type Message struct {
	Payload  MessagePayload
	Batcher  string
	BatchKey string

	// Metadata is an optional value set by the producer when creating a message.
	// The framework never inspects or modifies it, so it is preserved across all
	// processor transformations.
	Metadata any

	partitionKey string
	ack          Acknowledger
	acked        bool
	error        error
}

// NewMessage creates a Message with the given payload.  Producers use this in
// HandleDemand to build messages they want to enqueue; they may set Metadata,
// Batcher, and BatchKey before returning.
func NewMessage(payload MessagePayload) *Message {
	return &Message{Payload: payload}
}
