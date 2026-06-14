# go-broadway

A concurrent and multi-stage data ingestion and processing framework for Go applications, inspired by the Broadway library from Elixir. go-broadway simplifies building concurrent, resilient data processing pipelines with built-in support for batching, partitioning, and fault tolerance.

- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage Examples](#usage-examples)
- [Advanced Topics](#advanced-topics)
- [Configuration Options](#configuration-options)
- [Connectors](#connectors)
- [Usage Considerations](#usage-considerations)
- [Caveats](#caveats)
- [Real-World Example](#real-world-example)
- [Testing](#testing)
- [License](#license)
- [Acknowledgments](#acknowledgments)

## Features

- **Back-pressure** - The pipeline only requests the amount of messages necessary from producers, never flooding the system
- **Batching** - Group related messages into batches for efficient processing, based on size and/or time
- **Partitioning** - Ensure messages with the same key are processed by the same processor, guaranteeing ordering of related messages
- **Fault Tolerance** - Handle failures gracefully with automatic error handling and isolation
- **Graceful Shutdown** - Properly flush all events when the system is shutting down

## Architecture

See the [Architecture Documentation](./docs/architecture.md) for a detailed explanation of the architecture. 

## Installation

```bash
go get github.com/ttrung2409/go-broadway
```

## Quick Start

Below is a simple example showing how to set up a basic pipeline:

```go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ttrung2409/go-broadway/broadway"
)

func main() {
	// Create and configure the pipeline
	pipeline := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{
			Producer:    &MyProducer{},
			Concurrency: 2,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &MyMessageProcessor{},
			Concurrency: 5,
			MinDemand:   10,
			MaxDemand:   100,
		},
		Batchers: []broadway.BatcherConfig{
			{
				Name:         "my_batcher",
				BatchSize:    50,
				BatchTimeout: 5 * time.Second,
				Concurrency:  3,
				Processor:    &MyBatchProcessor{},
			},
		},
		PartitionBy: func(payload broadway.MessagePayload) string {
			// Extract partition key from message payload
			return myPayload.UserID
		},
		Acknowledger: myAcknowledger,
	})

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle termination signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		fmt.Printf("Received signal %s, shutting down...\n", sig)
		cancel()
	}()

	// Start the pipeline
	pipeline.Run(ctx)

	// Wait for pipeline to terminate
	<-ctx.Done()
}
```

## Usage Examples

### Implementing a Producer

```go
type MyProducer struct {
	// state variables
}

// Init is called once when the pipeline starts. Use it for one-time setup
// that requires a context, such as opening connections.
func (p *MyProducer) Init(ctx context.Context) {}

// Clone creates a new instance of MyProducer for concurrent processing.
func (p *MyProducer) Clone() broadway.Producer {
	return &MyProducer{}
}

// HandleDemand fetches or generates messages based on demand.
func (p *MyProducer) HandleDemand(demand int, ctx context.Context) []broadway.MessagePayload {
	payloads := make([]broadway.MessagePayload, 0, demand)
	// fetch or generate up to demand messages
	return payloads
}
```

### Implementing a Message Processor

```go
type MyMessageProcessor struct {
	// state variables
}

// Clone creates a new instance of MyMessageProcessor for concurrent processing
func (p *MyMessageProcessor) Clone() broadway.MessageProcessor {
	return &MyMessageProcessor{}
}

// Handle processes a single message
func (p *MyMessageProcessor) Handle(message *broadway.Message, ctx context.Context) (*broadway.Message, error) {
	// Process the message
	
	// Set batch key for grouping in batchers
	message.BatchKey = "some-batch-key"
	
	return message, nil
}
```

### Implementing a Batch Processor

```go
type MyBatchProcessor struct {
	// state variables
}

// Clone creates a new instance of MyBatchProcessor for concurrent processing
func (p *MyBatchProcessor) Clone() broadway.BatchProcessor {
	return &MyBatchProcessor{}
}

// Handle processes a batch of messages
func (p *MyBatchProcessor) Handle(batch []*broadway.Message, ctx context.Context) error {
	// Process the batch of messages
	
	return nil
}
```

### Implementing an Acknowledger

```go
func myAcknowledger(messages []*broadway.Message, err error) {
	if err != nil {
		// Handle failed messages (e.g., retry or dead-letter queue)
		fmt.Printf("%d messages failed with error: %v\n", len(messages), err)
	} else {
		// Acknowledge successful processing (e.g., remove from queue)
		fmt.Printf("%d messages processed successfully\n", len(messages))
	}
}
```

## Advanced Topics

### Batching

Batching groups messages for more efficient processing. Configure batch size and timeout to balance throughput and latency:

```go
Batchers: []broadway.BatcherConfig{
    {
        Name:         "my_batcher",
        BatchSize:    100,       // Process in batches of 100
        BatchTimeout: 5 * time.Second, // Or after 5 seconds
        Concurrency:  3,         // Run 3 batch processors
        Processor:    &MyBatchProcessor{},
    },
},
```

### Partitioning

Partitioning ensures that messages with the same partition key are always processed by the same message processor and batch processor. This guarantees that related messages are processed in order:

```go
PartitionBy: func(payload broadway.MessagePayload) string {
    // Return a string that will be used to partition messages
    return payload.(MyMessage).UserID
},
```

## Configuration Options

### Pipeline Configuration

```go
type PipelineConfig struct {
    Producer         ProducerConfig
    MessageProcessor MessageProcessorConfig
    Batchers         []BatcherConfig
    PartitionBy      PartitionKeyResolver
    Acknowledger     Acknowledger
}
```

When configuring a pipeline:
- `Producer` and `MessageProcessor` are required components
- `Batchers` is optional - if not provided, messages will be acknowledged immediately after processing by the message processor
- `PartitionBy` is optional - if not provided, messages will be distributed across processors in a round-robin fashion
- `Acknowledger` is optional - if not provided, a no-op acknowledger will be used

### Producer Configuration

```go
type ProducerConfig struct {
    Producer    Producer // Your producer implementation
    Concurrency int      // Number of concurrent producers
}
```

### Message Processor Configuration

```go
type MessageProcessorConfig struct {
    Processor   MessageProcessor // Your message processor implementation
    Concurrency int              // Number of concurrent message processors
    MinDemand   int              // Minimum number of messages to request
    MaxDemand   int              // Maximum number of messages to request
}
```

### Batcher Configuration

```go
type BatcherConfig struct {
    Name         string         // Unique name for this batcher
    BatchSize    int            // Maximum number of messages per batch
    BatchTimeout time.Duration  // Maximum time to wait before processing
    Concurrency  int            // Number of concurrent batch processors
    Processor    BatchProcessor // Your batch processor implementation
}
```

## Connectors

A `Connector` is a `Producer` that streams data from an external source into the Broadway pipeline.

Each connector is optionally paired with an Acknowledger that can be used to track the message offset and provide at-least-once delivery guarantee for the source.

#### Built-in Connectors

##### [PostgresConnector](./docs/connectors/postgres.md)

Postgres CDC via WAL logical replication (`pg` package).

```go
connector := pg.New(pg.Config{
    ConnectionString: "postgres://user:pass@host/db",
    SlotName:         "broadway_cdc",
    Publication:      "broadway_pub",
    Tables:           []string{"public.orders", "public.users"},
})

pipeline := broadway.NewPipeline(broadway.PipelineConfig{
    Producer: broadway.ProducerConfig{
        Producer:    connector,
        Concurrency: 1, // must be 1; WAL is a single sequential stream
    },
    MessageProcessor: broadway.MessageProcessorConfig{
        Processor:   &MyCDCProcessor{},
        Concurrency: 4,
        MinDemand:   10,
        MaxDemand:   100,
    },
    PartitionBy: func(payload broadway.MessagePayload) string {
        event := payload.(pg.CDCEvent)
        return fmt.Sprintf("%s.%d", event.Table, event.PK.Value())
    },
    // Acknowledger is auto-wired from the connector
})
```

## Usage Considerations
- **Concurrency Settings**: Adjust based on workload and available resources
- **Batch Sizing**: Balance between throughput (larger batches) and latency (smaller batches)
- **Partition Keys**: Choose keys that distribute load evenly
- **Error Handling**: Implement custom acknowledgers for special failure cases

## Caveats

go-broadway is designed for lightweight ETL pipelines. If you need to stream changes out of a single data source, particularly Postgres, and process them through a pipeline without pulling in a heavy infrastructure dependency, this library gives you that in pure Go. 

Consider a dedicated message broker instead if any of the following caveats apply.

### Heterogeneous Sources & Targets

If your workload involves multiple heterogeneous sources and/or targets, you may want an interface sitting in between to decouple sources and targets, so both of them can scale independently: adding a new source does not require implementation for every target, and vice versa.

### Durability

If your use case requires durable queuing, for example to replay historical changes for debugging, backfill, or onboarding late consumers without re-snapshotting from the source.

## Real-World Example

Check out the [examples](./examples) directory for a complete example of processing user activity events:

```go
// See examples/main.go for the full implementation
```

## Testing

The library includes tests demonstrating various scenarios:

- Basic processing: [basic_test.go](./test/basic_test.go)
- Batching: [batching_test.go](./test/batching_test.go)
- Fault tolerance: [fault_tolerance_test.go](./test/fault_tolerance_test.go)
- Graceful shutdown: [graceful_shutdown_test.go](./test/graceful_shutdown_test.go)
- Partitioning: [partitioning_test.go](./test/partitioning_test.go)

To run the tests:

```bash
go test ./test/... -v -race
```

## License

MIT License - see LICENSE file for details.

## Acknowledgments

- Inspired by the [Broadway](https://github.com/dashbitco/broadway) library from Elixir
- Built with ❤️ by the Go community