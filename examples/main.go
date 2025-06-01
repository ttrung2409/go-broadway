package examples

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ttrung2409/go-broadway/broadway"
)

func Run() {

	// Create and configure the pipeline
	pipeline := broadway.NewPipeline(broadway.PipelineConfig{
		Producer: broadway.ProducerConfig{
			Producer:    &ActivityProducer{},
			Concurrency: 1,
		},
		MessageProcessor: broadway.MessageProcessorConfig{
			Processor:   &ActivityProcessor{},
			Concurrency: 1,
			MinDemand:   5,
			MaxDemand:   10,
		},
		Batchers: []broadway.BatcherConfig{
			{
				Name:         "session_tracker",
				BatchSize:    10,
				BatchTimeout: 3 * time.Second,
				Concurrency:  3,
				Processor:    &UserSessionTracker{},
			},
		},
		PartitionKeyResolver: func(payload broadway.MessagePayload) string {
			if activity, ok := payload.(UserActivity); ok {
				return activity.UserID
			}
			return ""
		},
		Acknowledger: ack,
	})

	// Create a context with cancellation
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

	// Print header
	fmt.Println("==================================")
	fmt.Println("User Activity Tracking with Broadway")
	fmt.Println("==================================")
	fmt.Println()
	fmt.Println("Press Ctrl+C to stop")
	fmt.Println("==================================")

	// Start the pipeline
	pipeline.Run(ctx)

	// Wait for termination
	<-ctx.Done()
	time.Sleep(500 * time.Millisecond) // Allow final logs to print
	fmt.Println("==================================")
	fmt.Println("Pipeline shut down")
}
