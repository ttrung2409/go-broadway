package broadway

type ContextKey string

const (
	BatcherContextKey            ContextKey = "batcher"
	MessageProcessorIdContextKey ContextKey = "messageProcessorId"
)
