package main

import (
	"context"
	"fmt"
	"log"

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
		log.Fatal(err)
	}
	fields, ok := put.(map[string]any)
	if !ok {
		log.Fatalf("unexpected FLOW.VALUE.PUT response %T", put)
	}
	ref, ok := fields["ref"].(string)
	if !ok || ref == "" {
		log.Fatalf("FLOW.VALUE.PUT response has no ref: %#v", fields)
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
		log.Fatal(err)
	}

	values, err := client.ValueMGet(ctx, []string{ref}, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("value refs=%v\n", values)
}
