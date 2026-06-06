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

	ref, err := client.PutValue(ctx, "analysis", map[string]any{"score": 98}, ferricstore.ValuePutOptions{
		OwnerFlowID:  "analysis:1",
		PartitionKey: "tenant:1",
		TTLMS:        ferricstore.Int64(3600000),
	})
	if err != nil {
		log.Fatal(err)
	}

	_, err = client.Create(ctx, ferricstore.CreateOptions{
		ID:           "analysis:1",
		Type:         "analysis",
		State:        "ready",
		PartitionKey: "tenant:1",
		ValueRefs:    map[string]string{"analysis": fmt.Sprint(ref)},
		Idempotent:   ferricstore.Bool(true),
	})
	if err != nil {
		log.Fatal(err)
	}

	values, err := client.ValueMGet(ctx, []string{fmt.Sprint(ref)}, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("value refs=%v\n", values)
}
