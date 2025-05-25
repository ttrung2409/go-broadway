package broadway

// Transformer is a function type that transforms raw message payloads into
// complete Message objects. This allows for customization of how message
// metadata (such as batcher routing and acknowledgment) is assigned.
//
// Parameters:
//   - data: The raw message payload to transform.
//
// Returns:
//   - A fully formed Message with the payload and appropriate metadata.
type Transformer func(data MessagePayload) *Message

// defaultTransformer creates a basic transformer that wraps payloads into
// Messages with a default acknowledger. This is used when no custom
// transformer is provided in the configuration.
func defaultTransformer(partitionKeyResolver PartitionKeyResolver) Transformer {
	if partitionKeyResolver == nil {
		partitionKeyResolver = func(payload MessagePayload) string {
			return ""
		}
	}

	return func(payload MessagePayload) *Message {
		return &Message{
			Payload:      payload,
			Acknowledger: defaultAck(),
			Batcher:      defaultBatcherName,
			BatchKey:     defaultBatchKey,
			PartitionKey: partitionKeyResolver(payload),
		}
	}
}
