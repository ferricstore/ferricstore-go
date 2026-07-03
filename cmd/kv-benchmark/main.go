package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	ferricstore "github.com/ferricstore/ferricstore-go"
)

type config struct {
	addr       string
	mode       string
	requests   int
	clients    int
	pipeline   int
	valueBytes int
	keyspace   int
	prefix     string
	preload    bool
	cpuProfile string
	memProfile string
}

type workerResult struct {
	ops       int64
	flushes   int64
	latencies []float64
}

func main() {
	var cfg config
	flag.StringVar(&cfg.addr, "addr", "127.0.0.1:6388", "FerricStore native address")
	flag.StringVar(&cfg.mode, "mode", "set", "set or get")
	flag.IntVar(&cfg.requests, "requests", 100000, "total operations to measure")
	flag.IntVar(&cfg.clients, "clients", runtime.GOMAXPROCS(0), "concurrent SDK clients/connections")
	flag.IntVar(&cfg.pipeline, "pipeline", 1, "commands per native protocol pipeline flush")
	flag.IntVar(&cfg.valueBytes, "value-bytes", 256, "bytes per SET value/preloaded GET value")
	flag.IntVar(&cfg.keyspace, "keyspace", 0, "number of keys to cycle; defaults to requests")
	flag.StringVar(&cfg.prefix, "prefix", "kvbench", "key prefix")
	flag.BoolVar(&cfg.preload, "preload", true, "preload keys before GET benchmark")
	flag.StringVar(&cfg.cpuProfile, "cpu-profile", "", "write Go CPU profile to file")
	flag.StringVar(&cfg.memProfile, "mem-profile", "", "write Go heap profile to file after benchmark")
	flag.Parse()

	if err := validate(cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if cfg.keyspace == 0 {
		cfg.keyspace = cfg.requests
	}

	ctx := context.Background()
	if cfg.mode == "get" && cfg.preload {
		if err := preload(ctx, cfg); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
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

	result, err := run(ctx, cfg)
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
	if cfg.mode != "set" && cfg.mode != "get" {
		return fmt.Errorf("invalid --mode %q", cfg.mode)
	}
	if cfg.requests <= 0 || cfg.clients <= 0 || cfg.pipeline <= 0 || cfg.valueBytes < 0 || cfg.keyspace < 0 {
		return fmt.Errorf("numeric options must be positive, except value/keyspace may be zero")
	}
	return nil
}

func preload(ctx context.Context, cfg config) error {
	client := ferricstore.NewClient(cfg.addr, ferricstore.WithCodec(ferricstore.RawCodec{}))
	defer func() { _ = client.Close() }()
	value := makeValue(cfg.valueBytes)
	for start := 0; start < cfg.keyspace; start += cfg.pipeline {
		end := start + cfg.pipeline
		if end > cfg.keyspace {
			end = cfg.keyspace
		}
		commands := make([][]any, 0, end-start)
		for i := start; i < end; i++ {
			commands = append(commands, []any{"SET", keyFor(cfg.prefix, i), value})
		}
		if _, err := client.Pipeline(ctx, commands); err != nil {
			return fmt.Errorf("preload: %w", err)
		}
	}
	return nil
}

func run(ctx context.Context, cfg config) (map[string]any, error) {
	var next atomic.Int64
	results := make([]workerResult, cfg.clients)
	started := time.Now()
	var wg sync.WaitGroup
	var firstErr atomic.Value

	for worker := 0; worker < cfg.clients; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := ferricstore.NewClient(cfg.addr, ferricstore.WithCodec(ferricstore.RawCodec{}))
			defer func() { _ = client.Close() }()
			value := makeValue(cfg.valueBytes)
			for {
				start := int(next.Add(int64(cfg.pipeline)) - int64(cfg.pipeline))
				if start >= cfg.requests {
					return
				}
				end := start + cfg.pipeline
				if end > cfg.requests {
					end = cfg.requests
				}
				flushStarted := time.Now()
				if err := runBatch(ctx, client, cfg, start, end, value); err != nil {
					firstErr.CompareAndSwap(nil, err)
					return
				}
				latencyMS := float64(time.Since(flushStarted).Microseconds()) / 1000.0
				results[worker].latencies = append(results[worker].latencies, latencyMS)
				results[worker].ops += int64(end - start)
				results[worker].flushes++
			}
		}()
	}
	wg.Wait()
	if err, ok := firstErr.Load().(error); ok && err != nil {
		return nil, err
	}
	duration := time.Since(started)
	latencies := make([]float64, 0)
	var ops int64
	var flushes int64
	for _, result := range results {
		ops += result.ops
		flushes += result.flushes
		latencies = append(latencies, result.latencies...)
	}
	sort.Float64s(latencies)
	return map[string]any{
		"mode":             cfg.mode,
		"addr":             cfg.addr,
		"requests":         cfg.requests,
		"clients":          cfg.clients,
		"pipeline":         cfg.pipeline,
		"value_bytes":      cfg.valueBytes,
		"keyspace":         cfg.keyspace,
		"duration_seconds": duration.Seconds(),
		"ops_per_second":   float64(ops) / duration.Seconds(),
		"flushes":          flushes,
		"flush_p50_ms":     percentile(latencies, 50),
		"flush_p95_ms":     percentile(latencies, 95),
		"flush_p99_ms":     percentile(latencies, 99),
		"flush_min_ms":     first(latencies),
		"flush_max_ms":     last(latencies),
	}, nil
}

func runBatch(ctx context.Context, client *ferricstore.Client, cfg config, start, end int, value []byte) error {
	if cfg.pipeline == 1 && end-start == 1 {
		key := keyFor(cfg.prefix, start%cfg.keyspace)
		if cfg.mode == "set" {
			return client.KV().Set(ctx, key, value)
		}
		_, err := client.KV().Get(ctx, key)
		return err
	}
	commands := make([][]any, 0, end-start)
	for i := start; i < end; i++ {
		key := keyFor(cfg.prefix, i%cfg.keyspace)
		if cfg.mode == "set" {
			commands = append(commands, []any{"SET", key, value})
		} else {
			commands = append(commands, []any{"GET", key})
		}
	}
	_, err := client.Pipeline(ctx, commands)
	return err
}

func makeValue(size int) []byte {
	value := make([]byte, size)
	for i := range value {
		value[i] = byte('a' + (i % 26))
	}
	return value
}

func keyFor(prefix string, index int) string {
	return fmt.Sprintf("%s:%d", prefix, index)
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	idx := int((p / 100.0) * float64(len(values)-1))
	return values[idx]
}

func first(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	return values[0]
}

func last(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	return values[len(values)-1]
}
