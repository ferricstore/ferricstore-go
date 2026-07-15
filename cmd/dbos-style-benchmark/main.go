package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
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
