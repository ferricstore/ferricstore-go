package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	ferricstore "github.com/ferricstore/ferricstore-go"
)

func runSerialLatency(ctx context.Context, cfg config) (map[string]any, error) {
	client := ferricstore.NewClient(cfg.addr)
	runtimes := make([]float64, 0, cfg.iterations)
	for i := 0; i < cfg.iterations; i++ {
		runID := fmt.Sprintf("go-sdk-latency-%d-%d", time.Now().UnixNano(), i)
		partition := partitionFor(0, 1, runID)
		started := time.Now()
		if _, err := client.Create(ctx, ferricstore.CreateOptions{
			ID:           runID + ":flow",
			Type:         flowType,
			State:        "step_1",
			PartitionKey: partition,
			ReturnRecord: false,
		}); err != nil {
			return nil, err
		}
		for step := 1; step <= cfg.steps; step++ {
			jobs, err := client.ClaimDue(ctx, ferricstore.ClaimDueOptions{
				Type:         flowType,
				State:        fmt.Sprintf("step_%d", step),
				Worker:       runID + ":worker",
				PartitionKey: partition,
				Limit:        1,
			})
			if err != nil {
				return nil, err
			}
			if len(jobs) == 0 {
				return nil, fmt.Errorf("no job claimed for step %d", step)
			}
			job := jobs[0]
			if _, err := client.KV().Incr(ctx, runID+":counter"); err != nil {
				return nil, err
			}
			if step == cfg.steps {
				_, err = client.Complete(ctx, ferricstore.CompleteOptions{
					ID:           job.ID,
					LeaseToken:   job.LeaseToken,
					FencingToken: job.FencingToken,
					PartitionKey: job.PartitionKey,
					ReturnRecord: false,
				})
			} else {
				_, err = client.Transition(ctx, ferricstore.TransitionOptions{
					ID:           job.ID,
					FromState:    job.State,
					ToState:      fmt.Sprintf("step_%d", step+1),
					LeaseToken:   job.LeaseToken,
					FencingToken: job.FencingToken,
					PartitionKey: job.PartitionKey,
					ReturnRecord: false,
				})
			}
			if err != nil {
				return nil, err
			}
		}
		runtimes = append(runtimes, float64(time.Since(started).Microseconds())/1000.0)
	}
	sort.Float64s(runtimes)
	return map[string]any{
		"mode":       "serial-latency",
		"steps":      cfg.steps,
		"iterations": cfg.iterations,
		"avg_ms":     avgFloat(runtimes),
		"p50_ms":     percentile(runtimes, 50),
		"p95_ms":     percentile(runtimes, 95),
		"p99_ms":     percentile(runtimes, 99),
		"min_ms":     runtimes[0],
		"max_ms":     runtimes[len(runtimes)-1],
	}, nil
}
