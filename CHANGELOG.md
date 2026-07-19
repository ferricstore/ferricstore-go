# Changelog

## 0.8.0 - 2026-07-19

- Adopt the breaking FerricStore 0.8.0 beta contract while retaining native wire protocol v1 and declaring FerricStore 0.8.0 as the minimum server.
- Negotiate compact response opcodes and response limits through HELLO, reassemble interleaved chunk streams by lane/opcode/request identity, and bound aggregate response bytes.
- Require fetch ownership tokens and Flow lease/fencing tokens, add `max_active_ms`, canonical lineage, the v0.8 signal schema, absolute/keep-TTL SET options, and slot-local MSET/MSETNX validation.
- Follow explicit server retry dispositions and delays, fail closed on ambiguous post-send mutation outcomes, authenticate before larger frames, and prevent public access to reserved internal keys.
- Remove TopK decay, tokenless fetch completion, lineage aliases, rejected Retry/Rewind fields, unsupported Signal priority, and cross-shard MSET scattering.
- Replace TopK's shape-changing `List(..., withCount)` result with stable `List` and typed `ListWithCount` APIs, and preallocate maximum-size TopK batch commands.
- Decode every FerricStore 0.8 compact mixed-pipeline value shape and require exact reservation IDs for distributed-limit release.
- Use the dedicated v0.8 `FLOW.VALUE.PUT` opcode, including named-value options, and reject invalid named-value TTLs before encoding or transport.
- Remove Invocation helpers that have no command implementation in the exact FerricStore 0.8.0 server contract; retain trusted request context through `CommandExecWithContext`.

## 0.2.0 - 2026-07-16

- Honor structured status-5 reroutes, retry explicitly safe single-route commands and pipelines at most once, and keep topology PubSub subscriptions alive across learned-endpoint retirement.
- Make injected native executors configuration-immutable, validate policy acknowledgements fail-closed, bound manual buffers by command count and retained bytes, and add explicit routing for extension commands through `CommandForKey`.
- Accept both released and tokenized `FETCH_OR_COMPUTE` protocol shapes through additive APIs while preserving the v0.1.6 exported signatures.
- Add strict response validation across typed store, Flow administration, governance, PubSub, and native event surfaces so malformed protocol data fails instead of becoming plausible zero values.
- Handle connection-level native error frames, request cancellation, stale connections, reconnect generations, GOAWAY draining, and unsolicited frames without corrupting multiplexed requests.
- Add `SHARDS` topology discovery, exact endpoint trust checks, typed KV routing/scatter paths, snapshot-consistent refreshes, and real three-node routing/failover coverage.
- Add protected-mode, ACL, TLS verification, and mTLS integration coverage; pin the development and CI toolchain to Go 1.26.5 for the `crypto/tls` fix tracked as GO-2026-5856.
- Fix invalid URL authorities and expand bounded fuzz coverage for URLs, native values, compact responses, decoded surfaces, and round trips.
- Reduce compact claim-response decoding time and allocations, and add repeatable allocation, race-stress, and benchmark regression gates.
- Restore exported API compatibility with v0.1.6, adding opaque scan cursors and tokenized fetch-or-compute helpers without breaking existing callers.
- Add `FerricStoreMetricsText` as the canonical lossless Prometheus exposition API while retaining the deprecated v0.1.6 metrics-map API for source compatibility.
- Preserve FerricStore 0.7.5's amount-based `LimitRelease` contract and add non-downgrading exact reservation-ID release support for newer servers.
- Split large native, topology, Flow admin, store, and client files by responsibility and enforce a 525-line production-file ceiling.
- Pin Docker Compose and all integration/release jobs to FerricStore 0.7.5; gate releases on API compatibility, fuzzing, stress/performance, vulnerability scanning, and all live integration modes.
- Exercise recognized cluster errors, successful `FLOW.CANCEL_MANY`, stateful transactions/WATCH, buffering, autobatching, reconnect, cancellation, and strict command/version coverage against the released server.

## 0.1.6 - 2026-07-08

- Add opt-in FIFO/PARALLEL Flow state policies, queue/workflow policy installation, and FIFO priority guardrails.
- Add invocation definition/create/read/list helpers with request-context support.
- Encode `REQUEST_CONTEXT` into native `COMMAND_EXEC` payloads and fix explicit `COMMAND_EXEC` native payload shaping.
- Run Docker integration tests against FerricStore `0.7.5` by default.

## 0.1.5 - 2026-07-07

- Keep strict Docker integration coverage green against FerricStore `0.7.3` by exercising core fused Flow commands when state-meta Flow options are not supported by the server image.

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
