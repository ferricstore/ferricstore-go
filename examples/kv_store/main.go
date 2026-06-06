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

	if err := client.KV().Set(ctx, "tenant:1:profile", map[string]any{"plan": "pro"}); err != nil {
		log.Fatal(err)
	}
	profile, err := client.KV().Get(ctx, "tenant:1:profile")
	if err != nil {
		log.Fatal(err)
	}

	_, err = client.Hash().Set(ctx, "tenant:1:stats", "orders", 12)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("profile=%v\n", profile)
}
