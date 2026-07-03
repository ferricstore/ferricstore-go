# Benchmarks

Run benchmarks against a source FerricStore server or a released Docker image exposing the native protocol on `127.0.0.1:6388`.

The SDK defaults are latency-first: one native connection, no hidden auto-batching, 30s request timeout, 30s TCP keepalive/idle heartbeat. High connection counts and pipeline depth are benchmark knobs, not production defaults.

## DBOS-Style Workflow Benchmark

Go version of the Python queued workflow benchmark:

```bash
$(mise which go) run ./cmd/dbos-style-benchmark \
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

Modes:

* `--transport many`: `FLOW.CREATE_MANY` and `FLOW.COMPLETE_MANY`.
* `--transport pipeline`: normal SDK calls through a buffered SDK executor.
  Commands are queued locally and flushed through `Client.Pipeline`.

The queued benchmark creates and drains live, like the current Python benchmark.
Use it to compare client/runtime overhead between Python and Go against the same
FerricStore server.

For repeatable local baselines, start FerricStore with a clean data directory
for each run. Reusing a long-lived benchmark directory can include retention,
projection, compaction, and WAL pressure from previous runs.

## KV Benchmark

SET throughput:

```bash
$(mise which go) run ./cmd/kv-benchmark \
  --mode set \
  --requests 1000000 \
  --clients 800 \
  --pipeline 50 \
  --value-bytes 256 \
  --keyspace 1000000
```

GET throughput:

```bash
$(mise which go) run ./cmd/kv-benchmark \
  --mode get \
  --requests 2000000 \
  --clients 800 \
  --pipeline 50 \
  --value-bytes 256 \
  --keyspace 100000
```

Latency-style samples:

```bash
$(mise which go) run ./cmd/kv-benchmark \
  --mode set \
  --requests 10000 \
  --clients 1 \
  --pipeline 1 \
  --value-bytes 256

$(mise which go) run ./cmd/kv-benchmark \
  --mode get \
  --requests 10000 \
  --clients 1 \
  --pipeline 1 \
  --value-bytes 256
```

Notes:

* `--clients` means SDK clients/connections.
* `--pipeline` means commands per native protocol pipeline flush.
* GET mode preloads keys by default before timing reads.
* For local write-heavy KV runs, shard count can dominate results. On this development machine, 16 shards gave the best SET throughput, while 24-32 shards favored GET/mixed read paths.
