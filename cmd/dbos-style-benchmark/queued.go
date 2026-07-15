package main

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	ferricstore "github.com/ferricstore/ferricstore-go"
)

func createFlows(ctx context.Context, cfg config, runID string, indices []int, payload []byte, wake *partitionWakeCoordinator) (phaseStats, error) {
	flow := newBenchFlowClient(cfg.addr, cfg.transport)
	var created int64
	for _, batch := range chunks(indices, cfg.createBatchSize) {
		count, err := flow.enqueueMany(ctx, runID, batch, cfg.partitions, payload)
		if err != nil {
			return phaseStats{}, err
		}
		created += int64(count)
		if wake != nil {
			seen := map[int]struct{}{}
			for _, index := range batch {
				p := index % cfg.partitions
				if _, ok := seen[p]; !ok {
					seen[p] = struct{}{}
					wake.notifyPartition(p)
				}
			}
		}
	}
	stats := phaseStats{Created: created}
	if flow.buffered != nil {
		bufferedStats := flow.buffered.Stats()
		stats.CreatePipelineFlushes = bufferedStats.Flushes
		stats.CreatePipelineCommands = bufferedStats.CommandsSent
		stats.CreatePipelineMaxDepth = bufferedStats.MaxDepth
	}
	return stats, nil
}

func runClaimWorker(ctx context.Context, cfg config, runID string, workerIndex int, producersDone *atomic.Bool, completed *atomic.Int64, wake *partitionWakeCoordinator) (phaseStats, error) {
	flow := newBenchFlowClient(cfg.addr, cfg.transport)
	worker := workerID(runID, workerIndex)
	var localCompleted, claimCalls, emptyClaims, claimedItems, maxClaimBatch int64
	claimRound := 0
	baseIdle := time.Duration(cfg.idleSleepMS * float64(time.Millisecond))
	maxIdle := time.Duration(cfg.maxIdleSleepMS * float64(time.Millisecond))
	if maxIdle < baseIdle {
		maxIdle = baseIdle
	}
	idle := baseIdle
	wakeCoalesce := time.Duration(cfg.wakeCoalesceMS * float64(time.Millisecond))
	owned := make([]int, 0)
	for p := 0; p < cfg.partitions; p++ {
		if p%cfg.workers == workerIndex {
			owned = append(owned, p)
		}
	}
	activeOwned := append([]int(nil), owned...)
	fallbackRound := 0

	finish := func() phaseStats {
		stats := phaseStats{
			Completed:     localCompleted,
			ClaimCalls:    claimCalls,
			EmptyClaims:   emptyClaims,
			ClaimedItems:  claimedItems,
			MaxClaimBatch: maxClaimBatch,
		}
		if flow.buffered != nil {
			bufferedStats := flow.buffered.Stats()
			stats.ProcessPipelineFlushes = bufferedStats.Flushes
			stats.ProcessPipelineCommands = bufferedStats.CommandsSent
			stats.ProcessPipelineMaxDepth = bufferedStats.MaxDepth
		}
		return stats
	}

	handleJobs := func(jobs []ferricstore.ClaimedItem, partitionKey string) error {
		if int64(len(jobs)) > maxClaimBatch {
			maxClaimBatch = int64(len(jobs))
		}
		claimedItems += int64(len(jobs))
		if err := flow.doWork(ctx, cfg.workCommand, runID, jobs); err != nil {
			return err
		}
		if err := flow.completeClaimed(ctx, jobs, partitionKey, cfg.completeBatch); err != nil {
			return err
		}
		localCompleted += int64(len(jobs))
		completed.Add(int64(len(jobs)))
		return nil
	}

	for completed.Load() < int64(cfg.flows) {
		partitionKeys := []string(nil)
		if wake != nil && !cfg.claimAny {
			partitions, ok := wake.nextPartitions(workerIndex, idle, cfg.claimPartitionBatchSize)
			if !ok {
				if producersDone.Load() && len(activeOwned) > 0 {
					limit := cfg.claimPartitionBatchSize
					if limit <= 0 {
						limit = 1
					}
					if limit > len(activeOwned) {
						limit = len(activeOwned)
					}
					partitions = make([]int, 0, limit)
					for i := 0; i < limit; i++ {
						partitions = append(partitions, activeOwned[(fallbackRound+i)%len(activeOwned)])
					}
					fallbackRound += limit
				} else if producersDone.Load() {
					return finish(), nil
				} else {
					continue
				}
			}
			if wakeCoalesce > 0 && !producersDone.Load() {
				time.Sleep(wakeCoalesce)
			}
			partitionKeys = partitionKeysFor(partitions, cfg.partitions, runID)
			completePartitionKey := ""
			if len(partitionKeys) == 1 {
				completePartitionKey = partitionKeys[0]
			}
			for completed.Load() < int64(cfg.flows) {
				claimCalls++
				jobs, err := flow.claimDue(ctx, worker, partitionKeys, cfg.claimBatchSize)
				if err != nil {
					return phaseStats{}, err
				}
				if len(jobs) == 0 {
					emptyClaims++
					if producersDone.Load() && len(partitions) > 0 {
						activeOwned = removeInts(activeOwned, partitions)
						if len(activeOwned) == 0 {
							return finish(), nil
						}
						fallbackRound = 0
					}
					break
				}
				if err := handleJobs(jobs, completePartitionKey); err != nil {
					return phaseStats{}, err
				}
				if len(jobs) < cfg.claimBatchSize {
					break
				}
			}
			continue
		}

		if !cfg.claimAny {
			partition := partitionIndexForClaim(workerIndex, cfg.workers, cfg.partitions, claimRound)
			claimRound++
			partitionKeys = []string{partitionFor(partition, cfg.partitions, runID)}
		}
		claimCalls++
		jobs, err := flow.claimDue(ctx, worker, partitionKeys, cfg.claimBatchSize)
		if err != nil {
			return phaseStats{}, err
		}
		if len(jobs) == 0 {
			emptyClaims++
			if idle > 0 {
				time.Sleep(idle)
				idle *= 2
				if idle > maxIdle {
					idle = maxIdle
				}
			}
			continue
		}
		idle = baseIdle
		completePartitionKey := ""
		if len(partitionKeys) == 1 {
			completePartitionKey = partitionKeys[0]
		}
		if err := handleJobs(jobs, completePartitionKey); err != nil {
			return phaseStats{}, err
		}
	}
	return finish(), nil
}

func runQueued(ctx context.Context, cfg config) (map[string]any, error) {
	runID := "go-sdk-bench-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	payload := make([]byte, cfg.payloadBytes)
	for i := range payload {
		payload[i] = 'x'
	}
	indices := make([]int, cfg.flows)
	for i := range indices {
		indices[i] = i
	}

	effectiveWorkerMode := cfg.workerMode
	if cfg.claimAny {
		effectiveWorkerMode = "polling"
	}
	var wake *partitionWakeCoordinator
	if effectiveWorkerMode == "owner-wakeup" {
		wake = newPartitionWakeCoordinator(cfg.workers, cfg.partitions)
	}

	createRanges := make([][]int, cfg.producers)
	for i, index := range indices {
		createRanges[i%cfg.producers] = append(createRanges[i%cfg.producers], index)
	}

	var completed atomic.Int64
	var producersDone atomic.Bool
	started := time.Now()
	processStarted := time.Now()
	createStarted := time.Now()

	createStatsCh := make(chan phaseStats, cfg.producers)
	workerStatsCh := make(chan phaseStats, cfg.workers)
	errCh := make(chan error, cfg.producers+cfg.workers)

	for w := 0; w < cfg.workers; w++ {
		go func(worker int) {
			stats, err := runClaimWorker(ctx, cfg, runID, worker, &producersDone, &completed, wake)
			if err != nil {
				errCh <- err
				return
			}
			workerStatsCh <- stats
		}(w)
	}
	for _, batch := range createRanges {
		go func(batch []int) {
			stats, err := createFlows(ctx, cfg, runID, batch, payload, wake)
			if err != nil {
				errCh <- err
				return
			}
			createStatsCh <- stats
		}(batch)
	}

	createStats := make([]phaseStats, 0, cfg.producers)
	for len(createStats) < cfg.producers {
		select {
		case err := <-errCh:
			return nil, err
		case stats := <-createStatsCh:
			createStats = append(createStats, stats)
		}
	}
	createFinished := time.Now()
	producersDone.Store(true)

	workerStats := make([]phaseStats, 0, cfg.workers)
	for len(workerStats) < cfg.workers {
		select {
		case err := <-errCh:
			return nil, err
		case stats := <-workerStatsCh:
			workerStats = append(workerStats, stats)
		}
	}
	processFinished := time.Now()

	created := sumStats(createStats, func(s phaseStats) int64 { return s.Created })
	processed := sumStats(workerStats, func(s phaseStats) int64 { return s.Completed })
	claimCalls := sumStats(workerStats, func(s phaseStats) int64 { return s.ClaimCalls })
	claimedItems := sumStats(workerStats, func(s phaseStats) int64 { return s.ClaimedItems })
	createSeconds := createFinished.Sub(createStarted).Seconds()
	processSeconds := processFinished.Sub(processStarted).Seconds()
	totalSeconds := processFinished.Sub(started).Seconds()

	return map[string]any{
		"mode":                       "queued",
		"queued_shape":               "live",
		"flows":                      cfg.flows,
		"created":                    created,
		"completed":                  processed,
		"workers":                    cfg.workers,
		"producers":                  cfg.producers,
		"partitions":                 cfg.partitions,
		"claim_any":                  cfg.claimAny,
		"worker_mode":                effectiveWorkerMode,
		"claim_batch_size":           cfg.claimBatchSize,
		"claim_partition_batch_size": cfg.claimPartitionBatchSize,
		"create_batch_size":          cfg.createBatchSize,
		"complete_batch":             cfg.completeBatch,
		"transport":                  cfg.transport,
		"payload_bytes":              cfg.payloadBytes,
		"work_command":               cfg.workCommand,
		"idle_sleep_ms":              cfg.idleSleepMS,
		"max_idle_sleep_ms":          cfg.maxIdleSleepMS,
		"wake_coalesce_ms":           cfg.wakeCoalesceMS,
		"wake_notifications":         wakeNotifications(wake),
		"process_claim_calls":        claimCalls,
		"process_empty_claims":       sumStats(workerStats, func(s phaseStats) int64 { return s.EmptyClaims }),
		"process_avg_claim_batch":    avg(claimedItems, claimCalls),
		"process_max_claim_batch":    maxStats(workerStats, func(s phaseStats) int64 { return s.MaxClaimBatch }),
		"create_pipeline_flushes":    sumStats(createStats, func(s phaseStats) int64 { return s.CreatePipelineFlushes }),
		"create_pipeline_commands":   sumStats(createStats, func(s phaseStats) int64 { return s.CreatePipelineCommands }),
		"create_pipeline_max_depth":  maxStats(createStats, func(s phaseStats) int64 { return s.CreatePipelineMaxDepth }),
		"process_pipeline_flushes":   sumStats(workerStats, func(s phaseStats) int64 { return s.ProcessPipelineFlushes }),
		"process_pipeline_commands":  sumStats(workerStats, func(s phaseStats) int64 { return s.ProcessPipelineCommands }),
		"process_pipeline_max_depth": maxStats(workerStats, func(s phaseStats) int64 {
			return s.ProcessPipelineMaxDepth
		}),
		"create_seconds":           createSeconds,
		"process_seconds":          processSeconds,
		"total_seconds":            totalSeconds,
		"create_flows_per_sec":     rate(created, createSeconds),
		"process_flows_per_sec":    rate(processed, processSeconds),
		"end_to_end_flows_per_sec": rate(processed, totalSeconds),
	}, nil
}
