# Changelog

## Unreleased

- Move the SDK package to the module root import path.
- Add typed FerricFlow command helpers for create, claim, transition, completion, retry, fail, cancel, rewind, history, indexes, children, policy, signals, value refs, and retention cleanup.
- Add queue and workflow helpers with concurrent `RunOnce` workers.
- Add long-running queue and workflow worker lifecycle helpers: `RunForever`, `Start`, `Stop`, `Join`, and `Stats`.
- Add store helpers for KV, hash, list, set, sorted set, stream, bitmap, HyperLogLog, geo, JSON, Bloom, Cuckoo, Count-Min Sketch, TopK, and TDigest.
- Expand non-JSON typed command coverage for Redis-compatible stores, probabilistic structures, server helpers, pub/sub inspection, and `FERRICSTORE.DOCTOR`.
- Add locks, CAS, rate limit, fetch-or-compute, cluster, and FerricStore admin helpers.
- Add codecs, examples, Docker Compose setup, CI, release workflow, and parity docs.
