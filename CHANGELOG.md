# Changelog

## Unreleased

## 0.11.5 - 2026-08-03

- Negotiate FerricStore 0.11.5's compact Stream producer capability and encode
  homogeneous `XADD key * field value...` batches with mode 34 without
  allocating during wire planning. Legacy servers, explicit IDs, trimming,
  `NOMKSTREAM`, malformed pairs, and unsupported values retain the generic path.
- Correct compact SET/GET pipeline frame-budget accounting to include the full
  six-byte request header.
- Negotiate compact Pub/Sub mode 35 for homogeneous `PUBLISH` pipelines and
  expand negotiated `pubsub_batch_v1` envelopes into ordered logical messages
  before applying event queue limits. Legacy events and servers retain their
  existing paths.
- Retain FerricStore 0.11.4 as the minimum server and native wire protocol v1.

## 0.11.4 - 2026-07-28

- Decode and validate the complete durable-schedule recurrence response,
  including creation time, interval period, cron expression, timezone, and
  overlap retry configuration.
- Replace the transitional schedule overloads and generic result envelope with
  the typed beta schedule options and command-specific result contracts.
- Require FerricStore 0.11.4 while retaining native wire protocol v1.

## 0.11.2 - 2026-07-27

- Expose typed specialized-plan capabilities and the complete query-index
  service, field, lifecycle, validation, retirement, and statistics status.
- Enforce server-aligned parameter, diagnostic, cursor, quality, and usage
  bounds, including compact projected event identifiers.
- Propagate the transport timeout into the server-side query deadline with the
  existing context timer, retaining the owned native fast path and bounded
  response decoding.
- Pin live integration to the immutable FerricStore 0.11.3 image while keeping
  0.11.0 as the minimum compatible server and native wire protocol v1.

## 0.11.1 - 2026-07-26

- Validate the unchanged compact FQL1 query/result contract against
  FerricStore 0.11.2's fused index execution and corrected compact
  `EXPLAIN ANALYZE` response path.
- Pin live integration to the immutable FerricStore 0.11.2 image while keeping
  0.11.0 as the minimum compatible server and native wire protocol v1.

## 0.11.0 - 2026-07-26

- Require FerricStore 0.11.0 while retaining native wire protocol v1 and the
  existing FQL1 query/result contracts.
- Expose each query-index generation's bounded `CoveringFields` and opaque
  `Format` codec identities, including the nullable exact-counter format.
- Reject missing, duplicate, oversized, invalid UTF-8, or inconsistent index
  status metadata before returning it to callers, with live OSS integration
  coverage for the 0.11 catalog.

## 0.10.1 - 2026-07-24

- Require FerricStore 0.10.3 for result projections and the negotiated compact FQL1 result codec while retaining native wire protocol v1.
- Add source-aware `ProjectFlowQuery` selectors for bounded sparse run/event
  results and decode the shared server compact-result golden corpus in CI.
- Negotiate the named FQL1 result codec without enabling the broader compact
  Flow response surface, preserving full-record `ClaimDue` responses.

## 0.10.0 - 2026-07-23

- Require FerricStore 0.10.0 and negotiate the complete FQL1 request, result, explain, capability, shape, and native schema manifest during HELLO.
- Add typed `FlowQuery`, `FlowExplain`, `FlowExplainAnalyze`, and `FlowQueryIndexes` APIs with exact count/page shapes, authenticated cursors, resource usage, index lifecycle status, and actionable structured diagnostics.
- Compile `List`, `Search`, `Terminals`, `Failures`, `Stuck`, and lineage convenience reads into bounded FQL instead of sending the removed collection commands; collection helpers now require an explicit partition and reject unsupported cold or synchronous-projection options.
- Rename the unrelated management telemetry helper to `TelemetryFlowQuery`, the release's single audited pre-1.0 source break, so `FlowQuery` names the durable query surface.
- Keep FQL text opaque in topology-aware clients so the server's prepared-command contract remains the sole parser, ACL key discoverer, and shard router; validate query and index bounds before transport and preserve structured native errors through wrapped value or pointer transports.
- Add live pagination, count, explain/analyze, index-status, eventual-projection, convenience, and scoped ACL integration coverage against the pinned FerricStore 0.10.2 image.
- Reject incompatible index-status contracts during HELLO, validate FQL text and names as UTF-8 before I/O, enforce explain fingerprints, and preserve Flow metadata normalization in query conveniences.
- Reject collection shapes that cannot select a bounded index before transport; `FlowQueryIndexes(ctx)` lists the catalog while its optional single non-empty ID selects one index.
- Reject malformed UTF-8 query response text and quality labels over 64 bytes without converting invalid byte slices into retained strings.

## 0.9.0 - 2026-07-19

- Target FerricStore 0.9.1 while retaining native wire protocol v1 and require HELLO to advertise Flow policy replacement and generation-CAS fields.
- Add `Replace` and `ExpectedGeneration` policy options with safe-integer validation, deep-patch direct updates, and full-replacement workflow installs.
- Return typed `PolicySnapshot` values with monotonic generations and expose stale generation conflicts through `StalePolicyGenerationError` without replaying CAS mutations.
- Preserve worker concurrency while enforcing FIFO entry partition and priority constraints, with cross-partition FIFO, patch, replacement, and CAS integration coverage.
- Preserve timeout and no-replay metadata through `COMMAND_EXEC` and native pipelines, including generation-CAS and blocking commands.
- Isolate blocking calls from automatic batches, remove redundant policy-response normalization, and enforce runtime and allocation ceilings in CI.

## 0.8.2 - 2026-07-19

- Make the Linux TLS integration bind mount traversable by FerricStore's non-root container while keeping CA and client private keys owner-only.

## 0.8.1 - 2026-07-19

- Make Bloom sizing validation deterministic for subnormal error rates across supported architectures.
- Complete idempotent PubSub state replay across connection resets without weakening the SDK's unknown-outcome mutation policy.
- Decouple live Flow command coverage from unrelated cold-projection shard health while retaining strict history and rewind wire coverage.

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
