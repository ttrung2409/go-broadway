package broadway

type MessagePayload any

// Message represents a unit of data flowing through the Broadway pipeline.
// It contains the actual payload and metadata for routing and acknowledgment.
type Message struct {
	Payload      MessagePayload
	Batcher      string
	BatchKey     string
	PartitionKey string

	ack Acknowledger
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
		PartitionKey: args.partitionKey,

		ack: args.ack,
	}
}
