package main

import (
	"context"
	"fmt"
	"os"

	ferricstore "github.com/ferricstore/ferricstore-go"
)

func main() {
	ctx := context.Background()
	client := ferricstore.NewClient("127.0.0.1:6388", ferricstore.WithCodec(ferricstore.JSONCodec{}))
	defer func() { _ = client.Close() }()

	put, err := client.ValuePut(ctx, map[string]any{"score": 98}, ferricstore.ValuePutOptions{
		PartitionKey: "tenant:1",
		TTLMS:        ferricstore.Int64(3600000),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "value write failed")
		os.Exit(1)
	}
	fields, ok := put.(map[string]any)
	if !ok {
		fmt.Fprintln(os.Stderr, "value write returned an unexpected response")
		os.Exit(1)
	}
	ref, ok := fields["ref"].(string)
	if !ok || ref == "" {
		fmt.Fprintln(os.Stderr, "value write response has no reference")
		os.Exit(1)
	}

	_, err = client.Create(ctx, ferricstore.CreateOptions{
		ID:           "analysis:1",
		Type:         "analysis",
		State:        "ready",
		PartitionKey: "tenant:1",
		ValueRefs:    map[string]string{"analysis": ref},
		Idempotent:   ferricstore.Bool(true),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "flow creation failed")
		os.Exit(1)
	}

	values, err := client.ValueMGet(ctx, []string{ref}, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "value read failed")
		os.Exit(1)
	}
	fmt.Printf("value refs=%v\n", values)
}
