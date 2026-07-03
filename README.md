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

The compose file uses `ghcr.io/ferricstore/ferricstore:latest` by default and exposes the native protocol on `127.0.0.1:6388`.
Set `FERRICSTORE_IMAGE=ghcr.io/ferricstore/ferricstore:<version>` when you want to pin a specific server image.

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

Use `ferrics://` for TLS when credentials leave a trusted local network:

```go
client, err := ferricstore.NewClientFromURL("ferrics://default:password@ferricstore.example.com:6389")
```

Default client behavior matches the Python SDK:

- one native protocol connection per `Client`
- 30s request timeout
- 30s TCP keepalive and idle protocol heartbeat
- no hidden auto-batching; use `Pipeline` or high-level worker APIs when you want batching
- queue/workflow workers default to batch size 10, concurrency 1, and a 30s claim lease

This is the recommended starting point for application code:

```go
client := ferricstore.NewClient("127.0.0.1:6388")
defer func() { _ = client.Close() }()
```

Use `Command` for low-level connection-mode commands or any command that does not need a polished helper:

```go
value, err := client.Command(ctx, "PING")
```

Use `NewAutoBatchClient` when many goroutines issue independent commands and you want the SDK to coalesce them into native protocol pipeline flushes:

```go
client := ferricstore.NewAutoBatchClient("127.0.0.1:6388", ferricstore.AutoBatchOptions{
	MaxSize:       100,
	FlushInterval: time.Millisecond,
}, ferricstore.WithCodec(ferricstore.JSONCodec{}))
defer func() { _ = client.Close() }()
```

Autobatching is opt-in because it can add up to `FlushInterval` latency to a single lonely request. For latency-first services, use `NewClient`; for high-throughput producers, use `NewAutoBatchClient`, `Pipeline`, or FerricFlow batch APIs.

If a request context is canceled before an autobatch flush starts, the SDK skips that command. Once a command has been flushed to FerricStore, cancellation only stops the caller from waiting; the server may still commit the command.

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

For a long-running process, start the worker and stop it with context cancellation:

```go
ctx, cancel := context.WithCancel(context.Background())
handle := worker.Start(ctx, time.Second)

// Later, during shutdown:
cancel()
stats, err := handle.Join()
```

Use `ErrorPolicyRetry` for transient errors, `ErrorPolicyFail` for permanent business failures, and `ErrorPolicyReturn` when your app wants to handle the error and leave the job lease semantics explicit.

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

For service workers, use the same lifecycle helper:

```go
worker := workflow.Worker("orders-1", nil, ferricstore.WorkerOptions{
	BatchSize:   25,
	Concurrency: 8,
})
handle := worker.Start(ctx, time.Second)
defer handle.Stop()
```

## Native Events and Flow Wake Signals

FerricStore can send native protocol events over the same multiplexed client connection. Use this for wake hints, then still claim work through FerricFlow so leases and fencing stay correct.

```go
pubsub, err := client.OpenPubSub()
if err != nil {
	return err
}

_, err = pubsub.SubscribeFlowWake(ctx, ferricstore.FlowWakeSubscriptionOptions{
	Type:  "email",
	State: "queued",
	Limit: ferricstore.Int(100),
})
if err != nil {
	return err
}

event, err := pubsub.NextEvent(ctx)
if err != nil {
	return err
}
if event.Name == "FLOW_WAKE" {
	// Wake your worker loop and call ClaimDue/ClaimJobs for the advertised type/state.
}
```

Use `OpenPubSub` when you want events on the existing native multiplexed connection. Use `NewPubSub` or `NewPubSubFromURL` when you want an isolated long-lived pub/sub connection.

Shared native events are delivered through a bounded client buffer. If the buffer is full, the SDK drops new events instead of blocking normal command responses. Check `client.DroppedEvents()` or `pubsub.DroppedEvents()` if wake/event loss matters to your worker loop.

## Stores

Typed helpers cover common FerricStore data structures:

```go
_ = client.KV().Set(ctx, "tenant:1:profile", map[string]any{"plan": "pro"})
profile, _ := client.KV().Get(ctx, "tenant:1:profile")

_, _ = client.Hash().Set(ctx, "order:1", "status", "paid")
_, _ = client.ListStore().RPush(ctx, "outbox", "event-1")
_, _ = client.SetStore().Add(ctx, "seen", "event-1")
```

Available store helpers include KV, hash, list, set, sorted set, stream, bitmap, HyperLogLog, geo, Bloom, Cuckoo, Count-Min Sketch, TopK, and TDigest. FerricStore JSON document commands are not exposed by this SDK because the current server does not support them.

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

## Attributes, Schedules, Governance

Workflow attributes are indexed metadata for debugging and search. They are separate from payload bytes.

```go
_, err := client.Create(ctx, ferricstore.CreateOptions{
	ID:         "order-1",
	Type:       "order",
	State:      "validate",
	Payload:    map[string]any{"amount": 42},
	Attributes: map[string]any{"tenant": "acme", "region": "us"},
})

orders, err := client.List(ctx, "order", ferricstore.ReadOptions{
	Attributes: map[string]any{"tenant": "acme"},
	Count:      ferricstore.Int(100),
})
```

Schedules create flows from durable time rules:

```go
schedule, err := client.ScheduleCreate(ctx, "daily-report", ferricstore.ScheduleOptions{
	Cron:     "0 9 * * *",
	Timezone: "America/New_York",
	Target: map[string]any{
		"type":  "report",
		"state": "queued",
		"id":    "report-{{fire_id}}",
	},
	Overwrite: ferricstore.Bool(true),
})
```

Schedules can be paused, resumed, deleted, manually fired, and listed:

```go
_, _ = client.SchedulePause(ctx, "daily-report", nil)
_, _ = client.ScheduleResume(ctx, "daily-report", nil)
_, _ = client.ScheduleFire(ctx, "daily-report", nil)
schedules, _ := client.ScheduleList(ctx, ferricstore.ScheduleListOptions{Limit: ferricstore.Int(100)})
```

Governance helpers expose approvals, budgets, limits, effects, and circuit breakers without dropping to raw commands:

```go
approval, err := client.ApprovalRequest(ctx, "approval-1", ferricstore.ApprovalRequestOptions{
	FlowID:      "order-1",
	Scope:       "tenant:acme:refund",
	RequestedBy: "worker-1",
	Assignees:   []string{"ops@example.com"},
})

budget, err := client.BudgetReserve(ctx, "llm:tenant:acme", 100, ferricstore.Int64(10_000), ferricstore.Int64(60_000), "reservation-1", nil)
```

Circuit breakers are cheap reads in the normal path and explicit writes when the circuit changes:

```go
status, err := client.CircuitGet(ctx, "vendor:payments")
if status != nil && status.Status == "open" {
	return fmt.Errorf("payments circuit open")
}

_, _ = client.CircuitOpen(ctx, "vendor:payments", ferricstore.Int64(30_000), nil, nil)
_, _ = client.CircuitClose(ctx, "vendor:payments", nil)
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
  --transport many \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --claim-batch-size 250 \
  --create-batch-size 500 \
  --wake-coalesce-ms 0
```

KV throughput benchmark:

```bash
mise exec -- go run ./cmd/kv-benchmark \
  --mode set \
  --requests 1000000 \
  --clients 800 \
  --pipeline 50 \
  --value-bytes 256
```

The large `--clients`/`--pipeline` values are benchmark tuning knobs, not SDK defaults.

See:

- [docs/design.md](docs/design.md)
- [docs/python-parity.md](docs/python-parity.md)
- [docs/api.md](docs/api.md)
- [docs/benchmark.md](docs/benchmark.md)
