package broadway

type MessagePayload any

// Message represents a unit of data flowing through the Broadway pipeline.
// It contains the actual payload and metadata for routing and acknowledgment.
type Message struct {
	Payload      MessagePayload // The actual data being processed
	Acknowledger Acknowledger   // Handles acknowledgment after processing
	Batcher      string         // Name of the batcher to handle this message
	BatchKey     string         // Key for grouping messages in batches
	PartitionKey string         // Key for partitioning messages to specific processors
}
