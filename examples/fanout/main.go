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

	_, err := client.Create(ctx, ferricstore.CreateOptions{
		ID:           "report:1",
		Type:         "report",
		State:        "split",
		PartitionKey: "tenant:1",
		Idempotent:   ferricstore.Bool(true),
	})
	if err != nil {
		log.Fatal(err)
	}

	jobs, err := client.ClaimDue(ctx, ferricstore.ClaimDueOptions{
		Type:         "report",
		State:        "split",
		Worker:       "fanout-worker-1",
		PartitionKey: "tenant:1",
		Limit:        1,
	})
	if err != nil {
		log.Fatal(err)
	}
	if len(jobs) == 0 {
		fmt.Println("no due report jobs")
		return
	}

	job := jobs[0]
	_, err = client.SpawnChildren(ctx, ferricstore.SpawnChildrenOptions{
		ParentID:     job.ID,
		PartitionKey: job.PartitionKey,
		LeaseToken:   job.LeaseToken,
		FencingToken: ferricstore.Int64(job.FencingToken),
		Wait:         "all",
		Success:      "join",
		Failure:      "failed",
		Children: []ferricstore.ChildSpec{
			{ID: "report:1:part:1", Type: "report-part", Payload: map[string]any{"part": 1}, PartitionKey: job.PartitionKey},
			{ID: "report:1:part:2", Type: "report-part", Payload: map[string]any{"part": 2}, PartitionKey: job.PartitionKey},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("spawned report children")
}
