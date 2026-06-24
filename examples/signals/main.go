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

	_, err := client.Create(ctx, ferricstore.CreateOptions{
		ID:           "review:1",
		Type:         "review",
		State:        "waiting",
		PartitionKey: "tenant:1",
		Idempotent:   ferricstore.Bool(true),
	})
	if err != nil {
		log.Fatal(err)
	}

	_, err = client.Signal(ctx, ferricstore.SignalOptions{
		ID:           "review:1",
		Signal:       "approved",
		PartitionKey: "tenant:1",
		IfStates:     []string{"waiting"},
		TransitionTo: "approved",
		NamedValues: ferricstore.NamedValues{
			Values: map[string]any{"approved_by": "alice"},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("review approved")
}
