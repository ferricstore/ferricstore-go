# FerricStore Go SDK

Go SDK for FerricStore and FerricFlow.

FerricStore exposes a native binary command protocol. FerricFlow adds durable workflow state, leases, retries, history, value refs, and repair data while your Go service keeps running the application code.

## Install

```bash
go get github.com/ferricstore/ferricstore-go
```

```go
import ferricstore "github.com/ferricstore/ferricstore-go"
```

## Local Server

```bash
docker compose up -d ferricstore
```

The compose file uses `ghcr.io/ferricstore/ferricstore:0.5.3` and exposes the native protocol on `127.0.0.1:6388`.

## Client

```go
ctx := context.Background()
client := ferricstore.NewClient("127.0.0.1:6388", ferricstore.WithCodec(ferricstore.JSONCodec{}))
defer func() { _ = client.Close() }()
```

Use `NewClientFromURL` when you prefer URL configuration:

```go
client, err := ferricstore.NewClientFromURL("ferric://127.0.0.1:6388")
```

Use `Command` for advanced JSON commands, low-level connection-mode commands, or any command that does not need a polished helper:

```go
value, err := client.Command(ctx, "PING")
```

## Durable Queue

```go
queue := ferricstore.NewQueueClient(client).Queue("email")

_, err := queue.Enqueue(ctx, "email-1", map[string]any{"to": "user@example.com"}, ferricstore.CreateOptions{
	PartitionKey: "tenant:1",
})

worker := queue.Worker("worker-1", func(ctx context.Context, job ferricstore.FlowRecord) error {
	// Send the email here.
	return nil
}, ferricstore.WorkerOptions{
	BatchSize:   50,
	Concurrency: 16,
})

_, err = worker.RunOnce(ctx)
```

The worker claims due jobs, runs handlers concurrently, then completes, retries, fails, or returns handler errors based on `ErrorPolicy`.

## State Workflow

FerricFlow models workflows as explicit states and transitions.

```go
workflow := ferricstore.NewWorkflowClient(client).Workflow("order", "validate")

workflow.State("validate", func(ctx context.Context, w ferricstore.WorkflowContext) (ferricstore.Outcome, error) {
	return ferricstore.TransitionTo("charge", map[string]any{"validated": true}), nil
})

workflow.State("charge", func(ctx context.Context, w ferricstore.WorkflowContext) (ferricstore.Outcome, error) {
	return ferricstore.CompleteWith(map[string]any{"status": "paid"}), nil
})

_, err := workflow.Start(ctx, "order-1", map[string]any{"amount": 42}, ferricstore.CreateOptions{
	PartitionKey: "tenant:1",
})

_, err = workflow.Worker("orders-1", nil, ferricstore.WorkerOptions{
	BatchSize:   25,
	Concurrency: 8,
}).RunOnce(ctx)
```

The state machine data is stored in FerricStore. The SDK does not add another database or persistence layer.

## Stores

Typed helpers cover common FerricStore data structures:

```go
_ = client.KV().Set(ctx, "tenant:1:profile", map[string]any{"plan": "pro"})
profile, _ := client.KV().Get(ctx, "tenant:1:profile")

_, _ = client.Hash().Set(ctx, "order:1", "status", "paid")
_, _ = client.ListStore().RPush(ctx, "outbox", "event-1")
_, _ = client.SetStore().Add(ctx, "seen", "event-1")
```

Available store helpers include KV, hash, list, set, sorted set, stream, bitmap, HyperLogLog, geo, JSON, Bloom, Cuckoo, Count-Min Sketch, TopK, and TDigest. Non-JSON data-structure helpers cover the common FerricStore command surface broadly; advanced JSON commands remain available through `Command`.

## Value Refs

Use value refs for larger durable values that should be attached to workflow state by reference.

```go
ref, err := client.PutValue(ctx, "analysis", map[string]any{"score": 98}, ferricstore.ValuePutOptions{
	OwnerFlowID:  "flow-1",
	PartitionKey: "tenant:1",
	TTLMS:        ferricstore.Int64(3600000),
})

values, err := client.ValueMGet(ctx, []string{fmt.Sprint(ref)}, nil)
```

## Toolchain

The module requires Go 1.24 or newer. This repo pins the development toolchain with mise:

```bash
brew install mise
mise trust ./mise.toml
mise exec -- go test ./...
```

Run the Docker-backed integration suite against the released FerricStore image:

```bash
./scripts/integration-docker.sh
```

## Examples

Run examples against the compose server:

```bash
mise exec -- go run ./examples/durable_queue
mise exec -- go run ./examples/state_workflow
mise exec -- go run ./examples/fanout
mise exec -- go run ./examples/signals
mise exec -- go run ./examples/value_refs
mise exec -- go run ./examples/kv_store
```

## Benchmark

```bash
mise exec -- go run ./cmd/dbos-style-benchmark \
  --mode queued \
  --transport pipeline \
  --flows 10000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --claim-batch-size 100 \
  --create-batch-size 100
```

See:

- [docs/design.md](docs/design.md)
- [docs/python-parity.md](docs/python-parity.md)
- [docs/api.md](docs/api.md)
- [docs/benchmark.md](docs/benchmark.md)
