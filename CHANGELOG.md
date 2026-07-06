# Changelog

## Unreleased

## 0.1.4 - 2026-07-07

- Move the SDK package to the module root import path.
- Add typed FerricFlow command helpers for create, claim, transition, completion, retry, fail, cancel, rewind, history, indexes, children, policy, signals, value refs, and retention cleanup.
- Add queue and workflow helpers with concurrent `RunOnce` workers.
- Add long-running queue and workflow worker lifecycle helpers: `RunForever`, `Start`, `Stop`, `Join`, and `Stats`.
- Add opt-in automatic command batching with `NewAutoBatchClient` and `AutoBatchExecutor`.
- Add store helpers for KV, hash, list, set, sorted set, stream, bitmap, HyperLogLog, geo, Bloom, Cuckoo, Count-Min Sketch, TopK, and TDigest.
- Expand typed command coverage for FerricStore data structures, probabilistic structures, server helpers, pub/sub inspection, and `FERRICSTORE.DOCTOR`.
- Add narrow management helpers for capabilities, namespace metadata, quotas, safe telemetry reads, and ACL load/whoami.
- Add opt-in topology-aware native routing with exact seed endpoint trust by default and explicit trusted-host opt-in for learned cluster endpoints.
- Route `STATE_META` Flow mutations and `FLOW.SEARCH` through `COMMAND_EXEC` for compatibility with released server images that do not support those dedicated native payloads yet.
- Add locks, CAS, rate limit, fetch-or-compute, cluster, and FerricStore admin helpers.
- Add codecs, examples, Docker Compose setup, CI, release workflow, and parity docs.
- Switch the default client transport to FerricStore native protocol on port `6388`.
- Add a Docker-backed integration test runner for the released FerricStore image.
