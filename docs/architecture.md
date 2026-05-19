# Architecture

## Overview

```
                     [Producer_1]
                         / \
                        /   \
                       /     \
                      /       \
             [Processor_1] [Processor_2]   <- Process each message
                      /\     /\
                     /  \   /  \
                    /    \ /    \
                   /      X      \
                  /      / \      \
                 /      /   \      \
                /      /     \      \
           [Batcher_1]    [Batcher_2]  <- Collect & group messages
                /\              \
               /  \              \
              /    \              \
             /      \              \
   [BatchProcessor_1] [BatchProcessor_2]  [BatchProcessor_3]  <- Process batches
                      |
                      |
                      v
                 [Acknowledger]  <- Confirm message processing results
```

- **Producers**: Generate or fetch messages from external sources (APIs, databases, message queues).

- **Message Processors**: Process individual messages, applying business logic and transformations.

- **Batchers**: Collect related messages into batches based on configurable criteria.

- **Batch Processors**: Process batches of messages.

- **Acknowledger**: Messages are acked either by the message processor or the batch processor (if batching is enabled). This allows for tracking message status, implementing retry logic, or reporting errors to external systems.

## Key Mechanisms

### Back-pressure

Back-pressure regulates message flow through a demand-driven model: processors signal how many messages they can handle, and producers only fetch up to that amount. As processors get busy, demand drops and producers slow down; as they free up, demand rises again. This self-regulating loop keeps the pipeline stable under varying loads.

#### Message Buffering

*The producer generates more messages than immediate demand.*

To mitigate this, the message processor implement an internal buffer to manage incoming messages. When messages arrive they are temporarily stored in the buffer, and the processor continuously consumes messages from the buffer. When the buffer level drops below a configurable threshold (default `MinDemand`: 10), the processor automatically requests additional messages from upstream producers, with the request size of `MaxDemand - MinDemand`. This allows the processor to operate at a stable pace even when the producer generate messages faster than the processor can handle.

#### Demand Buffering

*The message processor requests more messages than the producer can generate / fetch.*

To mitigate this, the producer employs an internal request queue. When a request with a specific demand comes from the message processor, it temporarily stored in the queue. The producer then continuously ful-fill these requests from the queue.

### Batching

Batching groups related messages together for more efficient processing, especially when interacting with external systems that perform better with bulk operations. Batchers in the pipeline collect messages until either reaching a configured threshold (for example, 100 messages) or when a batch times out (such as waiting 5 seconds). Once either condition is met, the accumulated batch is forwarded to a batch processor.


#### Multiple Batchers

The system supports multiple batchers, each with its own unique name, allowing messages to be routed to different batchers based on their requirements. During message processing, each message can be assigned a specific batcher name to control which batcher it goes to.

```go
message.Batcher = "my_batcher"
```

#### Batch Key

Each message carries a batch key that determines how messages are grouped within a batcher. Messages sharing the same batch key are collected into the same batch and always processed by the same batch processor, guaranteeing ordered processing for related messages.

```go
// all messages for the same user are grouped into one batch and processed in order
message.BatchKey = user.ID
```

#### Default Batcher

If batching is enabled (by configuring at least one batcher in the pipeline) but a message doesn't explicitly specify a batcher name, the system automatically routes it to a "default" batcher. This ensures that all messages are properly handled even when the developer doesn't explicitly set the batcher name:

```go
// The following two approaches are equivalent when the default batcher is used
message1.Batcher = "default"  // Explicitly using the default batcher
message2.Batcher = ""         // Implicitly using the default batcher
```

### Partitioning

Partitioning guarantees that messages sharing the same partition key are always routed to the same processor, preserving their processing order. Messages with different keys are distributed across processors and handled concurrently, maximizing throughput without sacrificing consistency.

Implementing partitioning is straightforward with a simple function that extracts the appropriate key from each message:

```go
PartitionBy: func(payload broadway.MessagePayload) string {
    return payload.(MyMessage).UserID
}
```

#### Consistent Hashing

Under the hood, the pipeline uses consistent hashing to translate a partition key into a stable processor index. Each partition key is run through a hash function and mapped to a position on a logical ring. Processors are also placed at fixed positions on that ring. A key is routed to the first processor it encounters moving clockwise — so the same key always resolves to the same processor. This determinism is what makes the ordering guarantee possible.

```
┌──────────────────────── Hash Ring ────────────────────────────┐
│                                                               │
0            200              500                800          (MAX, wraps to 0)
│             │                │                  │
●─────────────●────────────────●──────────────────●────────────●
              │                │                  │
           [Proc_0]         [Proc_1]           [Proc_2]

  key_A (hash=250) ──→ [Proc_1]   (first processor clockwise from 250)
  key_B (hash=530) ──→ [Proc_2]   (first processor clockwise from 530)
  key_C (hash=820) ──→ [Proc_0]   (wraps around the ring back to Proc_0)
```

This also means that the partition key space is distributed evenly across processors. A well-chosen key (e.g., a UUID or numeric ID) produces a uniform hash distribution, keeping load balanced across all processor instances without any explicit coordination.

### Fault Tolerance

The fault tolerance model is built on three properties that together keep the pipeline resilient against unexpected failures.

#### Failure Isolation

Each component — producer, message processor, and batch processor — runs in its own goroutine. A panic or unhandled error in one goroutine is caught and contained there; it does not propagate to sibling goroutines or adjacent pipeline stages. A crashing processor does not stall the producer, and a failing batch processor does not affect other batch processors running concurrently. This containment ensures that a single bad message or transient external error cannot cascade into a full pipeline outage.

#### Automatic Component Restart

When a component crashes, the pipeline immediately spins up a replacement using the component's `Clone()` method, which produces a fresh instance with clean state. Concurrency is restored without manual intervention: if one of three message processors panics, a new processor is launched in its place, keeping throughput close to the configured level. The same applies to producers and batch processors — any failed component is transparently replaced, making individual component failures invisible to the rest of the pipeline.

#### Explicit Acknowledgment of Failed Messages

Messages that fail during processing are never silently dropped. Whether a failure occurs at the message processor or batch processor stage, the failed messages are always forwarded to the `Acknowledger` together with the error — following the same path as successfully processed messages. This guarantees that every message the pipeline accepts has a known, explicit outcome. The acknowledger can then take the action appropriate for the application: logging, retrying through a message queue, routing to a dead-letter store, or emitting metrics.

### Graceful Shutdown

When a shutdown signal is received (via context cancellation), the pipeline drains cleanly without dropping messages. Producers stop fetching new messages immediately, batchers flush any partial batches, and in-flight messages are allowed to complete processing and be acknowledged. Components then terminate in dependency order, releasing resources cleanly.

