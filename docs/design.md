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

- `CreateMany`, `CompleteMany`, `TransitionMany`, `RetryMany`, `FailMany`, `CancelMany`
- `Pipeline`
- `BufferedExecutor`
- raw `Command` for new or advanced commands

## Concurrency

Workers use goroutines for handler concurrency:

- `QueueWorker.RunOnce` claims a batch and handles jobs concurrently up to `WorkerOptions.Concurrency`.
- `WorkflowWorker.RunOnce` does the same for state handlers.
- Completion, retry, fail, and transition commands are sent per job unless the caller uses batch APIs directly.

Long-running worker lifecycle helpers are intentionally not hidden yet. Applications can run `RunOnce` in their own loop and control shutdown with `context.Context`.

## Codecs

The default `RawCodec` sends and receives values as-is. `JSONCodec` is available for structured payloads:

```go
client := ferricstore.NewClient("127.0.0.1:6388", ferricstore.WithCodec(ferricstore.JSONCodec{}))
```

Codecs apply to payloads, named values, value refs reads, and store wrappers where values are encoded.

## Package Shape

The module root is the import path:

```go
import ferricstore "github.com/ferricstore/ferricstore-go"
```

Examples live under `examples/`, and the benchmark lives under `cmd/dbos-style-benchmark`.
