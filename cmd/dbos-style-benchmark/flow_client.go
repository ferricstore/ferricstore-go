package main

import (
	"context"

	ferricstore "github.com/ferricstore/ferricstore-go"
)

type benchFlowClient struct {
	transport string
	read      *ferricstore.Client
	client    *ferricstore.Client
	buffered  *ferricstore.BufferedExecutor
}

func newBenchFlowClient(addr, transport string) *benchFlowClient {
	read := ferricstore.NewClient(addr)
	if transport == "pipeline" {
		buffered := ferricstore.NewBufferedExecutor(read)
		return &benchFlowClient{
			transport: transport,
			read:      read,
			client:    ferricstore.NewClientWithExecutor(buffered),
			buffered:  buffered,
		}
	}
	return &benchFlowClient{transport: transport, read: read, client: read}
}

func (c *benchFlowClient) enqueueMany(ctx context.Context, runID string, indices []int, partitions int, payload []byte) (int, error) {
	if c.transport == "pipeline" || len(indices) == 1 {
		for _, index := range indices {
			_, err := c.client.Create(ctx, ferricstore.CreateOptions{
				ID:           flowID(runID, index),
				Type:         flowType,
				State:        queueState,
				PartitionKey: partitionFor(index, partitions, runID),
				Payload:      payload,
				ReturnRecord: false,
			})
			if err != nil {
				return 0, err
			}
		}
		return len(indices), c.flush(ctx)
	}

	items := make([]ferricstore.CreateItem, 0, len(indices))
	for _, index := range indices {
		items = append(items, ferricstore.CreateItem{
			ID:           flowID(runID, index),
			Payload:      payload,
			PartitionKey: partitionFor(index, partitions, runID),
		})
	}
	independent := true
	_, err := c.client.CreateMany(ctx, ferricstore.CreateManyOptions{
		Items:       items,
		Type:        flowType,
		State:       queueState,
		Independent: &independent,
	})
	if err != nil {
		return 0, err
	}
	return len(items), nil
}

func (c *benchFlowClient) claimDue(ctx context.Context, worker string, partitionKeys []string, limit int) ([]ferricstore.ClaimedItem, error) {
	includeAttributes := false
	opt := ferricstore.ClaimDueOptions{
		Type:              flowType,
		State:             queueState,
		Worker:            worker,
		Limit:             limit,
		IncludeAttributes: &includeAttributes,
	}
	if len(partitionKeys) == 1 {
		opt.PartitionKey = partitionKeys[0]
	} else if len(partitionKeys) > 1 {
		opt.PartitionKeys = partitionKeys
	}
	return c.read.ClaimJobs(ctx, opt)
}

func (c *benchFlowClient) doWork(ctx context.Context, command, runID string, jobs []ferricstore.ClaimedItem) error {
	if command != "incr" {
		return nil
	}
	for range jobs {
		if c.transport == "pipeline" {
			if _, err := c.client.Command(ctx, "INCR", runID+":counter"); err != nil {
				return err
			}
			continue
		}
		if _, err := c.client.KV().Incr(ctx, runID+":counter"); err != nil {
			return err
		}
	}
	return nil
}

func (c *benchFlowClient) completeClaimed(ctx context.Context, jobs []ferricstore.ClaimedItem, partitionKey string, useMany bool) error {
	if c.transport == "pipeline" {
		for _, job := range jobs {
			if _, err := c.client.Complete(ctx, ferricstore.CompleteOptions{
				ID:           job.ID,
				LeaseToken:   job.LeaseToken,
				FencingToken: job.FencingToken,
				PartitionKey: job.PartitionKey,
				ReturnRecord: false,
			}); err != nil {
				return err
			}
		}
		return c.flush(ctx)
	}
	if useMany && len(jobs) > 1 {
		independent := true
		_, err := c.client.CompleteMany(ctx, ferricstore.CompleteManyOptions{
			PartitionKey: partitionKey,
			Items:        jobs,
			Independent:  &independent,
		})
		return err
	}
	for _, job := range jobs {
		if _, err := c.client.Complete(ctx, ferricstore.CompleteOptions{
			ID:           job.ID,
			LeaseToken:   job.LeaseToken,
			FencingToken: job.FencingToken,
			PartitionKey: job.PartitionKey,
			ReturnRecord: false,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (c *benchFlowClient) flush(ctx context.Context) error {
	if c.buffered == nil {
		return nil
	}
	_, err := c.buffered.Flush(ctx)
	return err
}
