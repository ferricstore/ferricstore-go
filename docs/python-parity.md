# Python SDK Parity

This checklist compares the Go SDK against the Python SDK command surface. Go names use Go casing, but commands map to the same FerricStore/FerricFlow protocol.

## Core Client

| Python SDK | Go SDK | Status |
| --- | --- | --- |
| `from_url` | `NewClientFromURL` | Covered |
| `command` | `Command` | Covered |
| `pipeline` | `Pipeline` | Covered |
| `close` | `Close` | Covered |
| `autobatch` producer | `BufferedExecutor` | Partial: low-level buffered executor, not the Python background producer API |

## FerricFlow Commands

| Python SDK | Go SDK | Status |
| --- | --- | --- |
| `create` | `Create` | Covered |
| `enqueue` | `Enqueue`, `Queue.Enqueue` | Covered |
| `create_many` | `CreateMany` | Covered |
| `enqueue_many` | `EnqueueMany`, `Queue.EnqueueMany` | Covered |
| `claim_due` | `ClaimDue` | Covered |
| `claim_jobs` | `ClaimJobs` | Covered |
| `reclaim` | `Reclaim` | Covered |
| `extend_lease` | `ExtendLease` | Covered |
| `transition` | `Transition`, `TransitionTo` outcome | Covered |
| `complete` | `Complete`, `CompleteWith` outcome | Covered |
| `complete_many` | `CompleteMany` | Covered |
| `retry` | `Retry`, `RetryWith` outcome | Covered |
| `retry_many` | `RetryMany` | Covered |
| `fail` | `Fail`, `FailWith` outcome | Covered |
| `fail_many` | `FailMany` | Covered |
| `cancel` | `Cancel` | Covered |
| `cancel_many` | `CancelMany` | Covered |
| `rewind` | `Rewind` | Covered |
| `get` | `Get` | Covered |
| `list` | `List` | Covered |
| `terminals` | `Terminals` | Covered |
| `failures` | `Failures` | Covered |
| `by_parent` | `ByParent` | Covered |
| `by_root` | `ByRoot` | Covered |
| `by_correlation` | `ByCorrelation` | Covered |
| `info` | `Info` | Covered |
| `stuck` | `Stuck` | Covered |
| `history` | `History` | Covered |
| `spawn_children` | `SpawnChildren` | Covered |
| `signal` | `Signal` | Covered |
| `flow_signal` | `FlowSignal` | Covered |
| `value_put`, `put_value` | `ValuePut`, `PutValue` | Covered |
| `value_mget` | `ValueMGet` | Covered |
| `install_policy` | `InstallPolicy` | Covered |
| `policy_get` | `PolicyGet` | Covered |
| `retention_cleanup` | `RetentionCleanup` | Covered |

## Queue And Workflow Helpers

| Python SDK | Go SDK | Status |
| --- | --- | --- |
| `QueueClient.queue` | `NewQueueClient(...).Queue` | Covered |
| queue `worker` | `Queue.Worker` | Covered |
| queue `run_once` | `QueueWorker.RunOnce` | Covered |
| queue `run_forever`, `start`, `join`, `stop`, stats | `RunForever`, `Start`, handle `Join`/`Stop`/`Stats` | Covered |
| `WorkflowClient.workflow` | `NewWorkflowClient(...).Workflow` | Covered |
| workflow `state` handlers | `Workflow.State` | Covered |
| workflow `start_flow`/`enqueue` | `Workflow.Start` | Covered |
| workflow worker `run_once` | `WorkflowWorker.RunOnce` | Covered |
| workflow `run`, `start`, `join`, `stop` | `RunForever`, `Start`, handle `Join`/`Stop`/`Stats` | Covered |
| async Python SDK | Not applicable | Go uses goroutines and blocking calls |

## Stores And Data Structures

| Area | Go SDK | Status |
| --- | --- | --- |
| Raw FerricStore commands | `Command` | Covered |
| KV | `KV()` | Broad typed coverage |
| Hash | `Hash()` | Broad typed coverage |
| List | `ListStore()` | Broad typed coverage |
| Set | `SetStore()` | Broad typed coverage |
| Sorted set | `SortedSet()` | Broad typed coverage for supported zset commands |
| Stream | `Stream()` | Broad typed coverage |
| Bitmap | `Bitmap()` | Covered |
| HyperLogLog | `HyperLogLog()` | Covered |
| Geo | `Geo()` | Broad typed coverage |
| JSON | `JSON()` | Basic helpers only: `JSON.SET`, `JSON.GET`, `JSON.DEL`; advanced JSON helpers intentionally left to `Command` |
| Bloom, Cuckoo, CMS, TopK, TDigest | `Bloom()`, `Cuckoo()`, `CountMinSketch()`, `TopK()`, `TDigest()` | Broad typed coverage |
| Every supported command as a typed method | `Command` fallback | Broad non-JSON coverage; advanced JSON and true connection-state APIs remain raw/native |

## Locks, CAS, Rate Limit, Admin

| Python SDK | Go SDK | Status |
| --- | --- | --- |
| `cas` | `CAS` | Covered |
| `lock` | `Lock` | Covered |
| `unlock` | `Unlock` | Covered |
| `extend_lock` | `ExtendLock` | Covered |
| `ratelimit_add` | `RateLimitAdd` | Covered |
| `key_info` | `KeyInfo` | Covered |
| `fetch_or_compute` | `FetchOrCompute` | Covered |
| `fetch_or_compute_result` | `FetchOrComputeResult` | Covered |
| `fetch_or_compute_error` | `FetchOrComputeError` | Covered |
| `cluster_health` | `ClusterHealth` | Covered |
| `cluster_stats` | `ClusterStats` | Covered |
| `cluster_keyslot` | `ClusterKeySlot` | Covered |
| `cluster_slots` | `ClusterSlots` | Covered |
| `cluster_status` | `ClusterStatus` | Covered |
| `cluster_role` | `ClusterRole` | Covered |
| `cluster_join` | `ClusterJoin` | Covered |
| `cluster_leave` | `ClusterLeave` | Covered |
| `cluster_failover` | `ClusterFailover` | Covered |
| `cluster_promote` | `ClusterPromote` | Covered |
| `cluster_demote` | `ClusterDemote` | Covered |
| `ferricstore_config` | `FerricStoreConfig` | Covered |
| `ferricstore_hotness` | `FerricStoreHotness` | Covered |
| `ferricstore_metrics` | `FerricStoreMetrics` | Covered |
| `ferricstore_blobgc` | `FerricStoreBlobGC` | Covered |
| `ferricstore_doctor` | `FerricStoreDoctor` | Covered |

## Current Gaps

- Advanced JSON command helpers are intentionally not added yet; use `Command` for `JSON.TYPE`, `JSON.ARRAPPEND`, `JSON.NUMINCRBY`, and related commands.
- Low-level connection-state command families such as ACL user management, CLIENT connection modes, SUBSCRIBE connection state, and MULTI/EXEC connection state remain available through `Command` where appropriate.
- Python's background autobatcher is not ported as a matching API; Go currently exposes `BufferedExecutor` and `Pipeline`.
- Integration tests need a live FerricStore server and are kept separate from unit tests.
