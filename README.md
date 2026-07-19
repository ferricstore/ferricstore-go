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

The compose file uses the SDK's pinned supported image, `ghcr.io/ferricstore/ferricstore:0.9.1`, by default and exposes the native protocol on `127.0.0.1:6388`.
Set `FERRICSTORE_IMAGE=ghcr.io/ferricstore/ferricstore:<version>` when you want to pin a specific server image.

## Compatibility

The Go package contract is v0.9.0 and requires FerricStore 0.9.1 or newer. This is a breaking beta API update; the native wire protocol remains v1.

## Client

```go
ctx := context.Background()
client := ferricstore.NewClient("127.0.0.1:6388", ferricstore.WithCodec(ferricstore.JSONCodec{}))
defer func() { _ = client.Close() }()
```

`WithCodec` serializes custom codec calls and snapshots mutable results so one
client is safe to share across goroutines. Use `WithConcurrentCodec` only for a
custom codec that supports overlapping calls and transfers ownership of every
mutable value it returns.

Use `NewClientFromURL` when you prefer URL configuration:

```go
client, err := ferricstore.NewClientFromURL("ferric://127.0.0.1:6388")
```

Use `ferrics://` for TLS when credentials leave a trusted local network:

```go
client, err := ferricstore.NewClientFromURL(
	"ferrics://ferricstore.example.com:6389",
	ferricstore.WithNativeOptions(
		ferricstore.WithNativeCredentials("default", password),
	),
)
```

Avoid putting production passwords in URLs because URLs are commonly copied into logs, shell history, and process metadata.

Default client behavior matches the Python SDK:

- one native protocol connection per `Client`
- 30s request timeout
- 30s TCP keepalive and idle protocol heartbeat
- one safe reconnect retry for closed-connection failures before a command reaches the server
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

Manual buffering is bounded by default to 4,096 commands and 16 MiB of
retained command data. Use `NewBufferedExecutorWithOptions` to choose smaller
application-specific limits; admission returns `ErrBufferedCapacity` instead
of growing the process heap without bound.

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

FIFO Flow state policy is opt-in per state:

```go
workflow.State("validate", validateHandler)
workflow.State("charge", chargeHandler, ferricstore.FlowStatePolicy{
	Mode: ferricstore.FlowStateModeFIFO,
})

_, err := workflow.InstallPolicy(ctx, ferricstore.PolicyOptions{})
```

FIFO states require `PartitionKey`; priority is for parallel states.

Direct policy updates deep-patch the stored snapshot. Workflow installation
replaces the declaration snapshot by default; pass `Replace: ferricstore.Bool(false)`
to request a patch explicitly. Policy reads and writes return a typed snapshot:

```go
previous, err := client.PolicyGet(ctx, "order", "")
if err != nil {
	return err
}

updated, err := client.SetPolicy(ctx, "order", ferricstore.PolicyOptions{
	ExpectedGeneration: ferricstore.Int64(previous.Generation),
	StatePolicies: map[string]ferricstore.FlowStatePolicy{
		"charge": {Mode: ferricstore.FlowStateModeFIFO},
	},
})
if errors.Is(err, ferricstore.ErrStalePolicyGeneration) {
	// Reload the policy and resolve the concurrent edit; CAS writes are not retried.
}
if err != nil {
	return err
}
fmt.Printf("installed policy generation %d\n", updated.Generation)
```

Generations must be in `0..9_007_199_254_740_991`. FIFO ordering is enforced
by FerricStore per `(type, state, partition_key)`, so worker concurrency can
remain greater than one to process independent partitions concurrently.

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

Use `OpenPubSub` when you want events on the existing native multiplexed connection. Use one shared pub/sub consumer per client; multiple shared consumers compete for the same event buffer. Use `NewPubSub` or `NewPubSubFromURL` when you want an isolated long-lived pub/sub connection.

Shared native events are delivered through a bounded client buffer. If the buffer is full, the SDK drops new events instead of blocking normal command responses. Check `client.DroppedEvents()` or `pubsub.DroppedEvents()` if wake/event loss matters to your worker loop.

Reconnect is enabled by default with one conservative retry. The SDK reconnects when the socket is already closed or HELLO/connection setup failed before the command was accepted. FerricStore `busy` and `reroute` errors are replayed at most once only when the response supplies both `retryable: true` and `safe_to_retry: true`; `retry_after_ms` is honored. Generation-CAS mutations are never replayed. Context cancellation and transport failures after a mutation may have reached the server, so those unknown outcomes are never replayed. Disable reconnect with `ferricstore.WithNativeReconnect(0)`.

```go
client := ferricstore.NewClient(
	"127.0.0.1:6388",
	ferricstore.WithNativeOptions(ferricstore.WithNativeReconnect(0)),
)
```

`WithNativeOptions` configures only executors owned by `NewClient` and
`NewClientFromURL`. When using `NewClientWithExecutor`, pass native options to
`NewNativeExecutor` itself; the client never mutates a caller-owned executor.

Native options also configure timeout, heartbeat, TLS, credentials, and client name:

```go
client := ferricstore.NewClient(
	"ferricstore.example.com:6389",
	ferricstore.WithNativeOptions(
		ferricstore.WithNativeTLS(tlsConfig),
		ferricstore.WithNativeCredentials("default", password),
		ferricstore.WithNativeClientName("orders-api"),
	),
)
```

For clustered servers, use the topology-aware executor. It probes `SHARDS`, routes single-shard key commands to the learned leader endpoint, and rejects learned endpoints unless they match an exact seed host+port or an explicit trusted host.

```go
client, err := ferricstore.NewTopologyClientFromURLs(
	[]string{"ferrics://ferricstore.example.com:6389"},
	ferricstore.WithTopologyNativeOptions(
		ferricstore.WithNativeCredentials("default", password),
	),
	ferricstore.WithTopologyClientOptions(
		ferricstore.WithCodec(ferricstore.JSONCodec{}),
	),
)
if err != nil {
	return err
}
defer func() { _ = client.Close() }()
```

Topology commands honor structured reroute responses and replay at most once,
only when the server explicitly returns both `retryable: true` and
`safe_to_retry: true`. For a new
server/module command that the SDK cannot infer a routing key for, provide the
key explicitly:

```go
value, err := client.CommandForKey(ctx, "tenant:42", "MODULE.CUSTOM", "argument")
```

The seed URL scheme selects the transport for every learned endpoint. Use
`ferrics://` for TLS; topology construction rejects native TLS options that
conflict with the seed scheme.

## Stores

Typed helpers cover common FerricStore data structures:

```go
_ = client.KV().Set(ctx, "tenant:1:profile", map[string]any{"plan": "pro"})
profile, _ := client.KV().Get(ctx, "tenant:1:profile")

_, _ = client.Hash().Set(ctx, "order:1", "status", "paid")
_, _ = client.ListStore().RPush(ctx, "outbox", "event-1")
_, _ = client.SetStore().Add(ctx, "seen", "event-1")

_, _ = client.TopK().ReserveWithOptions(ctx, "popular", 100, ferricstore.TopKReserveOptions{
	Width: ferricstore.Int64(200),
	Depth: ferricstore.Int64(7),
})
_, _ = client.TopK().IncrBy(ctx, "popular", ferricstore.TopKIncrement{Item: "search", Count: 3})
entries, _ := client.TopK().ListWithCount(ctx, "popular")
```

Available store helpers include KV, hash, list, set, sorted set, stream, bitmap, HyperLogLog, geo, Bloom, Cuckoo, Count-Min Sketch, TopK, and TDigest. FerricStore JSON document commands are not exposed by this SDK because the current server does not support them.

## Value Refs

Use value refs for larger durable values that should be attached to workflow state by reference.

```go
put, err := client.ValuePut(ctx, map[string]any{"score": 98}, ferricstore.ValuePutOptions{
	PartitionKey: "tenant:1",
	TTLMS:        ferricstore.Int64(3600000),
})
putFields := put.(map[string]any)
ref := putFields["ref"].(string)

values, err := client.ValueMGet(ctx, []string{ref}, nil)
```

`PutValue` writes a named value onto an existing owner Flow. Named values do not
accept a TTL; they inherit the owner Flow's retention policy.

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
		"type":      "report",
		"state":     "queued",
		"id_prefix": "report-",
	},
	Overwrite: ferricstore.Bool(true),
})
```

Schedules can be paused, resumed, deleted, manually fired, and listed:

```go
_, _ = client.SchedulePause(ctx, "daily-report", nil)
_, _ = client.ScheduleResume(ctx, "daily-report", nil)
_, _ = client.ScheduleFire(ctx, "daily-report", nil)
schedules, _ := client.ScheduleList(ctx, ferricstore.ScheduleListOptions{Count: ferricstore.Int(100)})
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

FerricStore releases distributed-limit credits only by the exact
reservation IDs returned from `LimitSpend`:

```go
released, err := client.LimitRelease(ctx, "api:tenant:acme", ferricstore.LimitReleaseOptions{
	ShardID:        spend.ShardID,
	ReservationIDs: spend.ReservationIDs,
})
```

The SDK has no amount-only release fallback.

Circuit breakers are cheap reads in the normal path and explicit writes when the circuit changes:

```go
status, err := client.CircuitGet(ctx, "vendor:payments")
if status != nil && status.Status == "open" {
	return fmt.Errorf("payments circuit open")
}

_, _ = client.CircuitOpen(ctx, "vendor:payments", ferricstore.Int64(30_000), nil, nil)
_, _ = client.CircuitClose(ctx, "vendor:payments", nil)
```

## Prometheus Metrics

Use `FerricStoreMetricsText` to retrieve the server's Prometheus exposition
without parsing or normalization:

```go
scrape, err := client.FerricStoreMetricsText(ctx)
```

The raw text preserves comments, metric types, labels, duplicate samples,
timestamps, and numeric spellings. `FerricStoreMetrics` remains available only
for source compatibility with Go SDK v0.1.6; its map representation is lossy
and is deprecated.

## Toolchain

The module requires Go 1.24 or newer. This repo pins Go 1.26.5 for development and release verification with mise:

```bash
brew install mise
mise trust ./mise.toml
mise exec -- go test ./...
```

Before release, run the compatibility, fuzz, stress/performance, and three Docker-backed integration gates:

```bash
./scripts/api-compat.sh
./scripts/fuzz-smoke.sh
./scripts/stress.sh
./scripts/integration-docker.sh
./scripts/integration-security-docker.sh
./scripts/integration-cluster-docker.sh
```

`api-compat.sh` compares the exported API with the release named in `.api-baseline`; any removal or incompatible signature change fails the gate. The security suite covers protected mode, ACLs, TLS verification, and mTLS. The cluster suite starts three real nodes and exercises learned routing and failover.

For release gating against a server image that should support every current command,
enable strict command coverage:

```bash
FERRICSTORE_STRICT_COMMAND_COVERAGE=1 FERRICSTORE_IMAGE=ghcr.io/ferricstore/ferricstore:<version> ./scripts/integration-docker.sh
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
