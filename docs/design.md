# SDK Design

The Go SDK keeps the application code in the Go process and stores durable state in FerricStore.

## Explicit State Machine

FerricFlow workflows are modeled as states:

```go
workflow.State("validate", func(ctx context.Context, w ferricstore.WorkflowContext) (ferricstore.Outcome, error) {
	return ferricstore.TransitionTo("charge", nil), nil
})
```

The durable data is the workflow record: current state, owner/lease, fencing token, retry data, values, value refs, and history. The SDK does not persist a Go execution stack and does not replay Go code to rebuild state.

That makes storage choices explicit. Developers choose what goes into payloads, named values, and value refs, and can use raw store commands for related durable data.

## Persistence

FerricStore owns persistence. The Go SDK does not introduce another database, ORM, or sidecar storage layer.

The SDK sends FerricStore commands over the native binary protocol and decodes structured responses. This is intentionally close to the protocol so high-throughput paths can use:

- compact native KV `SET`/`GET` pipeline payloads
- `CreateMany`, `CompleteMany`, `TransitionMany`, `RetryMany`, `FailMany`, `CancelMany`
- `Pipeline`
- bounded `BufferedExecutor` queues with explicit command/retained-byte limits
- `AutoBatchExecutor` and `NewAutoBatchClient`
- raw `Command` for new or advanced commands, plus `CommandForKey` when an
  extension command needs an explicit topology routing key

Default transport behavior is intentionally simple and aligned with the Python SDK latency-first path: one connection per client, 30s request timeout, TCP keepalive, idle protocol heartbeat, and no hidden auto-batching. Throughput code can opt into `NewAutoBatchClient`, explicit `Pipeline`, or FerricFlow batch APIs.

## Concurrency

Workers use goroutines for handler concurrency:

- `QueueWorker.RunOnce` claims a batch and handles jobs concurrently up to `WorkerOptions.Concurrency`.
- `WorkflowWorker.RunOnce` does the same for state handlers.
- `QueueWorker` batches successful queue completions as jobs finish, while retry and fail mutations remain per job so their individual errors are preserved. `WorkflowWorker` applies each handler outcome per job; callers can use the explicit `*Many` APIs for larger uniform batches.

Applications can either call `RunOnce` from their own loop or use the built-in
`RunForever`/`Start` lifecycle helpers. A started worker is controlled through
its handle's `Stop`, `Join`, and `Stats` methods, while `context.Context`
continues to provide parent cancellation and shutdown propagation.

## Codecs

The default `RawCodec` sends and receives values as-is. `JSONCodec` is available for client-side value encoding of structured payloads:

```go
client := ferricstore.NewClient("127.0.0.1:6388", ferricstore.WithCodec(ferricstore.JSONCodec{}))
```

Codecs apply to payloads, named values, value refs reads, and store wrappers where values are encoded. `JSONCodec` does not use FerricStore JSON document commands; it stores encoded bytes through normal FerricStore values.

## Package Shape

The module root is the import path:

```go
import ferricstore "github.com/ferricstore/ferricstore-go"
```

Examples live under `examples/`. Benchmarks live under `cmd/dbos-style-benchmark` and `cmd/kv-benchmark`.
