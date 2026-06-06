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
	defer func() { _ = client.Close() }()

	orders := ferricstore.NewWorkflowClient(client).Workflow("order", "validate")
	orders.State("validate", func(ctx context.Context, w ferricstore.WorkflowContext) (ferricstore.Outcome, error) {
		fmt.Printf("validate %s payload=%v\n", w.ID(), w.Payload())
		return ferricstore.TransitionTo("charge", map[string]any{"validated": true}), nil
	})
	orders.State("charge", func(ctx context.Context, w ferricstore.WorkflowContext) (ferricstore.Outcome, error) {
		fmt.Printf("charge %s\n", w.ID())
		return ferricstore.CompleteWith(map[string]any{"status": "paid"}), nil
	})

	_, err := orders.Start(ctx, "order:1", map[string]any{"amount": 42}, ferricstore.CreateOptions{
		PartitionKey: "tenant:1",
		Idempotent:   ferricstore.Bool(true),
	})
	if err != nil {
		log.Fatal(err)
	}

	worker := orders.Worker("order-worker-1", nil, ferricstore.WorkerOptions{
		BatchSize:   10,
		Concurrency: 4,
	})
	result, err := worker.RunOnce(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("claimed=%d applied=%d\n", result.Claimed, result.Applied)
}
