package main

import (
	"context"
	"fmt"
	"log"

	ferricstore "github.com/ferricstore/ferricstore-go"
)

func main() {
	ctx := context.Background()
	client := ferricstore.NewClient("127.0.0.1:6379", ferricstore.WithCodec(ferricstore.JSONCodec{}))
	defer client.Close()

	queue := ferricstore.NewQueueClient(client).Queue("email")
	_, err := queue.Enqueue(ctx, "email:1", map[string]any{"to": "user@example.com"}, ferricstore.CreateOptions{
		PartitionKey: "tenant:1",
		Idempotent:   ferricstore.Bool(true),
	})
	if err != nil {
		log.Fatal(err)
	}

	result, err := queue.Worker("email-worker-1", func(ctx context.Context, job ferricstore.FlowRecord) error {
		fmt.Printf("send email job=%s payload=%v\n", job.ID, job.Payload)
		return nil
	}, ferricstore.WorkerOptions{
		BatchSize:    10,
		Concurrency:  4,
		ClaimPayload: true,
	}).RunOnce(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("claimed=%d completed=%d retried=%d failed=%d\n", result.Claimed, result.Completed, result.Retried, result.Failed)
}
