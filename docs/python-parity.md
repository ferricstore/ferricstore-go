# Python SDK Parity

This checklist compares the Go SDK against the Python SDK command surface. Go names use Go casing, but commands map to the same FerricStore/FerricFlow protocol.

## Core Client

| Python SDK | Go SDK | Status |
| --- | --- | --- |
| `from_url` | `NewClientFromURL` | Covered |
| topology-aware native pool | `NewTopologyNativeExecutor`, `NewTopologyClientFromURLs` | Covered |
| `command` | `Command` | Covered |
| `pipeline` | `Pipeline` | Covered |
| `close` | `Close` | Covered |
| `autobatch` producer | `NewAutoBatchClient`, `AutoBatchExecutor`, `BufferedExecutor` | Covered |

Default behavior matches the Python SDK for normal client usage: one native protocol connection, 30s request timeout, 30s TCP keepalive/idle heartbeat, no hidden auto-batching, worker batch size 10, worker concurrency 1, and 30s claim lease.

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
| `search` | `Search` | Covered |
| `stats` | `Stats` | Covered |
| `attributes` | `Attributes` | Covered |
| `attribute_values` | `AttributeValues` | Covered |
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
| `schedule_create` | `ScheduleCreate` | Covered |
| `schedule_get` | `ScheduleGet` | Covered |
| `schedule_fire` | `ScheduleFire` | Covered |
| `schedule_pause` | `SchedulePause` | Covered |
| `schedule_resume` | `ScheduleResume` | Covered |
| `schedule_delete` | `ScheduleDelete` | Covered |
| `schedule_fire_due` | `ScheduleFireDue` | Covered |
| `schedule_list` | `ScheduleList` | Covered |
| `effect_reserve` | `EffectReserve` | Covered |
| `effect_confirm` | `EffectConfirm` | Covered |
| `effect_fail` | `EffectFail` | Covered |
| `effect_compensate` | `EffectCompensate` | Covered |
| `effect_get` | `EffectGet` | Covered |
| `governance_ledger` | `GovernanceLedger` | Covered |
| `approval_request` | `ApprovalRequest` | Covered |
| `approval_approve` | `ApprovalApprove` | Covered |
| `approval_reject` | `ApprovalReject` | Covered |
| `approval_get` | `ApprovalGet` | Covered |
| `approval_list` | `ApprovalList` | Covered |
| `governance_overview` | `GovernanceOverview` | Covered |
| `circuit_open` | `CircuitOpen` | Covered |
| `circuit_close` | `CircuitClose` | Covered |
| `circuit_get` | `CircuitGet` | Covered |
| `budget_reserve` | `BudgetReserve` | Covered |
| `budget_commit` | `BudgetCommit` | Covered |
| `budget_release` | `BudgetRelease` | Covered |
| `budget_get` | `BudgetGet` | Covered |
| `budget_list` | `BudgetList` | Covered |
| `limit_lease` | `LimitLease` | Covered |
| `limit_spend` | `LimitSpend` | Covered |
| `limit_release` | `LimitRelease` | Covered |
| `limit_get` | `LimitGet` | Covered |
| `limit_list` | `LimitList` | Covered |

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
| Bloom, Cuckoo, CMS, TopK, TDigest | `Bloom()`, `Cuckoo()`, `CountMinSketch()`, `TopK()`, `TDigest()` | Broad typed coverage |
| Every supported command as a typed method | `Command` fallback | Broad coverage; true connection-state APIs remain raw/native |

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
| `watch`, `unwatch` | `Watch`, `Unwatch` | Covered |
| `multi`, `exec`, `discard` | `Multi`, `Exec`, `Discard`, `Transaction` | Covered |
| transaction command queueing | `Transaction.Command`, `CommandExec` | Covered |
| `publish`, `subscribe`, `unsubscribe` | `Publish`, `Subscribe`, `Unsubscribe` | Covered |
| `psubscribe`, `punsubscribe` | `PSubscribe`, `PUnsubscribe` | Covered |
| long-lived pub/sub consumption | `NewPubSub`, `NewPubSubFromURL`, `OpenPubSub`, `PubSub.Next` | Covered; uses native multiplexed `request_id=0` events |
| native event subscriptions | `SubscribeEvents`, `UnsubscribeEvents`, `NextEvent` | Covered |
| `subscribe_flow_wake` | `SubscribeFlowWake` | Covered |
| `CLIENT.SETNAME`, `CLIENT.INFO` | `ClientSetName`, `ClientInfo` | Covered |
| `ACL` management | `ACL`, `ACLSetUser`, `ACLDelUser`, `ACLGetUser`, `ACLList`, `ACLSave`, `ACLWhoAmI`, `ACLLoad` | Covered |
| `capabilities` | `Capabilities` | Covered |
| namespace management | `EnsureNamespace`, `GetNamespace`, `ListNamespaces`, `DeleteNamespace` | Covered |
| quota management | `SetQuota`, `GetQuota`, `QuotaUsage` | Covered |
| safe management telemetry | `ClusterInfo`, `NamespaceUsage`, `FlowQuery`, `FlowHistory` | Covered |

## Current Gaps

- FerricStore JSON document commands are not part of the current server command surface, so the Go SDK intentionally does not expose a `JSON()` store helper.
- Go autobatching is opt-in through `NewAutoBatchClient` / `AutoBatchExecutor`. The DBOS-style benchmark still defaults to FerricFlow batch commands because that is the current optimized Go workflow throughput path.
- Go supports compact native pipeline payloads for hot homogeneous KV `SET` and `GET` pipelines. Other pipeline shapes use native AST frames or `COMMAND_EXEC` fallback depending on command support.
- Integration tests need a live FerricStore server and are kept separate from unit tests.
