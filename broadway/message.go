package broadway

type MessagePayload any

// Message represents a unit of data flowing through the Broadway pipeline.
// It contains the actual payload and metadata for routing and acknowledgment.
type Message struct {
	Payload  MessagePayload // The actual data being processed
	Batcher  string         // Name of the batcher to handle this message
	BatchKey string         // Key for grouping messages in batches

	ack          Acknowledger // Handles acknowledgment after processing
	partitionKey string       // Key for partitioning messages
}

type messageArgs struct {
	payload      MessagePayload
	ack          Acknowledger
	partitionKey string
}

func newMessage(args messageArgs) *Message {
	if args.ack == nil {
		args.ack = defaultAck()
	}

	return &Message{
		Payload:      args.payload,
		ack:          args.ack,
		partitionKey: args.partitionKey,
	}
}

func (m *Message) PartitionKey() string {
	return m.partitionKey
}

func (m *Message) Ack() Acknowledger {
	return m.ack
}
