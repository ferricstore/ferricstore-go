package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	ferricstore "github.com/ferricstore/ferricstore-go"
)

const (
	flowType   = "dbos_go_sdk_bench"
	queueState = "queued"
)

type config struct {
	addr                    string
	mode                    string
	flows                   int
	workers                 int
	producers               int
	partitions              int
	claimBatchSize          int
	claimPartitionBatchSize int
	createBatchSize         int
	transport               string
	payloadBytes            int
	workCommand             string
	idleSleepMS             float64
	maxIdleSleepMS          float64
	workerMode              string
	wakeCoalesceMS          float64
	claimAny                bool
	completeBatch           bool
	steps                   int
	iterations              int
	cpuProfile              string
	memProfile              string
}

type phaseStats struct {
	Created                 int64 `json:"created,omitempty"`
	Completed               int64 `json:"completed,omitempty"`
	ClaimCalls              int64 `json:"claim_calls,omitempty"`
	EmptyClaims             int64 `json:"empty_claims,omitempty"`
	ClaimedItems            int64 `json:"claimed_items,omitempty"`
	MaxClaimBatch           int64 `json:"max_claim_batch,omitempty"`
	CreatePipelineFlushes   int64 `json:"create_pipeline_flushes,omitempty"`
	CreatePipelineCommands  int64 `json:"create_pipeline_commands,omitempty"`
	CreatePipelineMaxDepth  int64 `json:"create_pipeline_max_depth,omitempty"`
	ProcessPipelineFlushes  int64 `json:"process_pipeline_flushes,omitempty"`
	ProcessPipelineCommands int64 `json:"process_pipeline_commands,omitempty"`
	ProcessPipelineMaxDepth int64 `json:"process_pipeline_max_depth,omitempty"`
}

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

type partitionWakeCoordinator struct {
	workers       int
	partitions    int
	chans         []chan int
	pending       []map[int]struct{}
	locks         []sync.Mutex
	notifications atomic.Int64
}

func newPartitionWakeCoordinator(workers, partitions int) *partitionWakeCoordinator {
	c := &partitionWakeCoordinator{
		workers:    workers,
		partitions: partitions,
		chans:      make([]chan int, workers),
		pending:    make([]map[int]struct{}, workers),
		locks:      make([]sync.Mutex, workers),
	}
	for i := range c.chans {
		c.chans[i] = make(chan int, partitions)
		c.pending[i] = make(map[int]struct{})
	}
	return c
}

func (c *partitionWakeCoordinator) ownerFor(partition int) int {
	return partition % c.workers
}

func (c *partitionWakeCoordinator) notifyPartition(partition int) {
	owner := c.ownerFor(partition)
	c.locks[owner].Lock()
	if _, ok := c.pending[owner][partition]; ok {
		c.locks[owner].Unlock()
		return
	}
	c.pending[owner][partition] = struct{}{}
	c.notifications.Add(1)
	c.locks[owner].Unlock()
	c.chans[owner] <- partition
}

func (c *partitionWakeCoordinator) nextPartitions(worker int, timeout time.Duration, limit int) ([]int, bool) {
	if limit <= 0 {
		limit = 1
	}
	select {
	case partition := <-c.chans[worker]:
		c.locks[worker].Lock()
		delete(c.pending[worker], partition)
		c.locks[worker].Unlock()
		partitions := []int{partition}
		for len(partitions) < limit {
			select {
			case partition := <-c.chans[worker]:
				c.locks[worker].Lock()
				delete(c.pending[worker], partition)
				c.locks[worker].Unlock()
				partitions = append(partitions, partition)
			default:
				return partitions, true
			}
		}
		return partitions, true
	case <-time.After(timeout):
		return nil, false
	}
}

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
		stats.CreatePipelineFlushes = flow.buffered.Flushes
		stats.CreatePipelineCommands = flow.buffered.CommandsSent
		stats.CreatePipelineMaxDepth = flow.buffered.MaxDepth
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
			stats.ProcessPipelineFlushes = flow.buffered.Flushes
			stats.ProcessPipelineCommands = flow.buffered.CommandsSent
			stats.ProcessPipelineMaxDepth = flow.buffered.MaxDepth
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

func main() {
	var cfg config
	flag.StringVar(&cfg.addr, "addr", "127.0.0.1:6388", "FerricStore native address")
	flag.StringVar(&cfg.mode, "mode", "queued", "queued or serial-latency")
	flag.IntVar(&cfg.flows, "flows", 10000, "flows to create")
	flag.IntVar(&cfg.workers, "workers", 16, "worker goroutines")
	flag.IntVar(&cfg.producers, "producers", 4, "producer goroutines")
	flag.IntVar(&cfg.partitions, "partitions", 16, "partition keys")
	flag.IntVar(&cfg.claimBatchSize, "claim-batch-size", 250, "FLOW.CLAIM_DUE limit")
	flag.IntVar(&cfg.claimPartitionBatchSize, "claim-partition-batch-size", 64, "partition keys to include in one FLOW.CLAIM_DUE")
	flag.IntVar(&cfg.createBatchSize, "create-batch-size", 500, "create batch size")
	flag.StringVar(&cfg.transport, "transport", "many", "many or pipeline/buffered")
	flag.IntVar(&cfg.payloadBytes, "payload-bytes", 0, "payload bytes per flow")
	flag.StringVar(&cfg.workCommand, "work-command", "none", "none or incr")
	flag.Float64Var(&cfg.idleSleepMS, "idle-sleep-ms", 10, "idle sleep milliseconds")
	flag.Float64Var(&cfg.maxIdleSleepMS, "max-idle-sleep-ms", 50, "max idle sleep milliseconds")
	flag.StringVar(&cfg.workerMode, "worker-mode", "owner-wakeup", "owner-wakeup or polling")
	flag.Float64Var(&cfg.wakeCoalesceMS, "wake-coalesce-ms", 0, "wake coalesce milliseconds")
	flag.BoolVar(&cfg.claimAny, "claim-any", false, "claim globally")
	flag.BoolVar(&cfg.completeBatch, "complete-batch", true, "use COMPLETE_MANY in many transport")
	flag.IntVar(&cfg.steps, "steps", 10, "serial latency steps")
	flag.IntVar(&cfg.iterations, "iterations", 100, "serial latency iterations")
	flag.StringVar(&cfg.cpuProfile, "cpu-profile", "", "write Go CPU profile to file")
	flag.StringVar(&cfg.memProfile, "mem-profile", "", "write Go heap profile to file after benchmark")
	flag.Parse()

	if err := validate(cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	ctx := context.Background()
	if cfg.cpuProfile != "" {
		file, err := os.Create(cfg.cpuProfile)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := pprof.StartCPUProfile(file); err != nil {
			_ = file.Close()
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer func() {
			pprof.StopCPUProfile()
			_ = file.Close()
		}()
	}
	var result map[string]any
	var err error
	if cfg.mode == "queued" {
		result, err = runQueued(ctx, cfg)
	} else {
		result, err = runSerialLatency(ctx, cfg)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if cfg.memProfile != "" {
		file, err := os.Create(cfg.memProfile)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		runtime.GC()
		if err := pprof.WriteHeapProfile(file); err != nil {
			_ = file.Close()
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		_ = file.Close()
	}
	encoded, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(encoded))
}

func validate(cfg config) error {
	if cfg.mode != "queued" && cfg.mode != "serial-latency" {
		return fmt.Errorf("invalid --mode %q", cfg.mode)
	}
	if cfg.transport != "pipeline" && cfg.transport != "many" {
		return fmt.Errorf("invalid --transport %q", cfg.transport)
	}
	if cfg.workerMode != "owner-wakeup" && cfg.workerMode != "polling" {
		return fmt.Errorf("invalid --worker-mode %q", cfg.workerMode)
	}
	if cfg.flows <= 0 || cfg.workers <= 0 || cfg.producers <= 0 || cfg.partitions <= 0 ||
		cfg.claimBatchSize <= 0 || cfg.claimPartitionBatchSize <= 0 ||
		cfg.createBatchSize <= 0 || cfg.steps <= 0 || cfg.iterations <= 0 {
		return fmt.Errorf("numeric options must be positive")
	}
	if cfg.payloadBytes < 0 || cfg.idleSleepMS < 0 || cfg.maxIdleSleepMS < 0 || cfg.wakeCoalesceMS < 0 {
		return fmt.Errorf("duration/payload options must be non-negative")
	}
	return nil
}

func chunks(values []int, size int) [][]int {
	if size <= 0 {
		size = 1
	}
	out := make([][]int, 0, (len(values)+size-1)/size)
	for start := 0; start < len(values); start += size {
		end := start + size
		if end > len(values) {
			end = len(values)
		}
		out = append(out, values[start:end])
	}
	return out
}

func partitionFor(index, partitions int, prefix string) string {
	if partitions <= 0 {
		partitions = 1
	}
	return prefix + ":partition:" + strconv.Itoa(index%partitions)
}

func flowID(prefix string, index int) string {
	return prefix + ":flow:" + strconv.Itoa(index)
}

func workerID(prefix string, index int) string {
	return prefix + ":worker:" + strconv.Itoa(index)
}

func partitionKeysFor(indices []int, partitions int, prefix string) []string {
	keys := make([]string, 0, len(indices))
	for _, index := range indices {
		keys = append(keys, partitionFor(index, partitions, prefix))
	}
	return keys
}

func removeInts(values []int, remove []int) []int {
	if len(values) == 0 || len(remove) == 0 {
		return values
	}
	removeSet := make(map[int]struct{}, len(remove))
	for _, value := range remove {
		removeSet[value] = struct{}{}
	}
	out := values[:0]
	for _, value := range values {
		if _, ok := removeSet[value]; ok {
			continue
		}
		out = append(out, value)
	}
	return out
}

func partitionIndexForClaim(workerIndex, workerCount, partitions, claimRound int) int {
	if partitions <= 0 {
		return 0
	}
	if workerCount >= partitions {
		return workerIndex % partitions
	}
	return (workerIndex + claimRound*workerCount) % partitions
}

func sumStats(stats []phaseStats, fn func(phaseStats) int64) int64 {
	var total int64
	for _, stat := range stats {
		total += fn(stat)
	}
	return total
}

func maxStats(stats []phaseStats, fn func(phaseStats) int64) int64 {
	var max int64
	for _, stat := range stats {
		if value := fn(stat); value > max {
			max = value
		}
	}
	return max
}

func wakeNotifications(wake *partitionWakeCoordinator) int64 {
	if wake == nil {
		return 0
	}
	return wake.notifications.Load()
}

func avg(numerator, denominator int64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func rate(count int64, seconds float64) float64 {
	if seconds <= 0 {
		return 0
	}
	return float64(count) / seconds
}

func avgFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var total float64
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}

func percentile(values []float64, pct float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(math.Ceil((pct/100.0)*float64(len(values)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}
