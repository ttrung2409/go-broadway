package broadway

// Connector is a stateful Producer that manages its own delivery offset.
// The paired Acknowledger advances that offset only after downstream
// acknowledgment, giving the source at-least-once delivery guarantees.
type Connector interface {
	Producer
	Acknowledger() Acknowledger
}
