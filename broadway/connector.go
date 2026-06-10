package broadway

// A `Connector` is a `Producer` that streams data from an external source into the Broadway pipeline.
// Each connector is optionally paired with an Acknowledger that can be used to track the message offset
// and provide at-least-once delivery guarantee.
type Connector interface {
	Producer
	Acknowledger() Acknowledger
}
