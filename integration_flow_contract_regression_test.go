//go:build integration

package ferricstore

import (
	"testing"
	"time"
)

func TestIntegrationV0119NestedRetryPolicyTypeAndState(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationDirectClient(JSONCodec{})
	defer client.Close()

	runID := integrationSuffix("flow-contract-policy")
	flowType := "go-sdk-flow-contract-policy-" + runID
	typeRetry := RetryPolicy{
		MaxRetries:  3,
		Backoff:     "exponential",
		BaseMS:      10,
		MaxMS:       1_000,
		JitterPct:   20,
		ExhaustedTo: "failed",
	}
	stateRetry := RetryPolicy{
		MaxRetries:  5,
		Backoff:     "linear",
		BaseMS:      20,
		MaxMS:       2_000,
		JitterPct:   30,
		ExhaustedTo: "failed",
	}

	installed := must[PolicySnapshot](t)(client.SetPolicy(ctx, flowType, PolicyOptions{
		Replace: Bool(true),
		Retry:   &typeRetry,
		States:  map[string]RetryPolicy{"queued": stateRetry},
	}))
	assertIntegrationRetryPolicy(t, installed.Retry, typeRetry, "installed type")
	state, ok := installed.States["queued"]
	if !ok {
		t.Fatalf("installed policy omitted queued state: %+v", installed)
	}
	assertIntegrationRetryPolicy(t, state.Retry, stateRetry, "installed queued state")

	read := must[PolicySnapshot](t)(client.PolicyGet(ctx, flowType, ""))
	assertIntegrationRetryPolicy(t, read.Retry, typeRetry, "read type")
	state, ok = read.States["queued"]
	if !ok {
		t.Fatalf("read policy omitted queued state: %+v", read)
	}
	assertIntegrationRetryPolicy(t, state.Retry, stateRetry, "read queued state")

	zeroType := flowType + "-zero"
	zeroRetry := RetryPolicy{
		MaxRetriesSet: true,
		Backoff:       "none",
		BaseMSSet:     true,
		MaxMSSet:      true,
		JitterPctSet:  true,
		ExhaustedTo:   "failed",
	}
	zeroStateRetry := RetryPolicy{
		MaxRetriesSet: true,
		Backoff:       "none",
		BaseMSSet:     true,
		MaxMSSet:      true,
		JitterPctSet:  true,
		ExhaustedTo:   "failed",
	}
	zero := must[PolicySnapshot](t)(client.SetPolicy(ctx, zeroType, PolicyOptions{
		Replace: Bool(true),
		Retry:   &zeroRetry,
		States:  map[string]RetryPolicy{"queued": zeroStateRetry},
	}))
	assertIntegrationRetryPolicy(t, zero.Retry, zeroRetry, "zero type")
	zeroState, ok := zero.States["queued"]
	if !ok {
		t.Fatalf("zero policy omitted queued state: %+v", zero)
	}
	assertIntegrationRetryPolicy(t, zeroState.Retry, zeroStateRetry, "zero queued state")
}

func TestIntegrationV0119ClaimDueAndReclaimReturnFullRecords(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationDirectClient(JSONCodec{})
	defer client.Close()

	runID := integrationSuffix("flow-contract-claims")
	flowType := "go-sdk-flow-contract-claims-" + runID
	now := time.Now().UnixMilli()
	payload := true

	claimID := "go-sdk-flow-contract-claim:" + runID
	claimPartition := claimID + ":partition"
	claimValue := "claim-selected-" + runID
	must[*FlowRecord](t)(client.Create(ctx, CreateOptions{
		ID:           claimID,
		Type:         flowType,
		State:        "queued",
		PartitionKey: claimPartition,
		Payload:      map[string]any{"kind": "claim", "marker": runID},
		Values:       map[string]any{"result": claimValue},
		Attributes:   map[string]any{"tenant": "acme"},
		StateMeta:    map[string]any{"attempt": int64(7)},
		RunAtMS:      now,
		NowMS:        now,
		Idempotent:   Bool(true),
	}))

	claimed := must[[]FlowRecord](t)(client.ClaimDue(ctx, ClaimDueOptions{
		Type:            flowType,
		State:           "queued",
		Worker:          "go-sdk-flow-contract-claim-worker",
		PartitionKey:    claimPartition,
		LeaseMS:         30_000,
		Limit:           1,
		NowMS:           now + 1,
		Payload:         &payload,
		PayloadMaxBytes: Int64(4_096),
		Values:          []string{"result"},
	}))
	requireLen(t, claimed, 1)
	assertIntegrationFullRecord(t, claimed[0], claimID, "claim", runID, claimValue, 7)
	if claimed[0].FencingToken <= 0 || claimed[0].Version <= 0 {
		t.Fatalf("claim fencing/version missing: %+v", claimed[0])
	}
	must[*FlowRecord](t)(client.Complete(ctx, CompleteOptions{
		ID:           claimed[0].ID,
		LeaseToken:   claimed[0].LeaseToken,
		FencingToken: claimed[0].FencingToken,
		PartitionKey: claimed[0].PartitionKey,
		NowMS:        now + 2,
	}))

	reclaimID := "go-sdk-flow-contract-reclaim:" + runID
	reclaimPartition := reclaimID + ":partition"
	reclaimValue := "reclaim-selected-" + runID
	must[*FlowRecord](t)(client.Create(ctx, CreateOptions{
		ID:           reclaimID,
		Type:         flowType,
		State:        "queued",
		PartitionKey: reclaimPartition,
		Payload:      map[string]any{"kind": "reclaim", "marker": runID},
		Values:       map[string]any{"result": reclaimValue},
		Attributes:   map[string]any{"tenant": "acme"},
		StateMeta:    map[string]any{"attempt": int64(11)},
		RunAtMS:      now,
		NowMS:        now,
		Idempotent:   Bool(true),
	}))
	initial := must[[]ClaimedItem](t)(client.ClaimJobs(ctx, ClaimDueOptions{
		Type:         flowType,
		State:        "queued",
		Worker:       "go-sdk-flow-contract-initial-worker",
		PartitionKey: reclaimPartition,
		LeaseMS:      1,
		Limit:        1,
		NowMS:        now + 3,
	}))
	requireLen(t, initial, 1)
	if initial[0].FencingToken <= 0 || initial[0].LeaseToken == "" {
		t.Fatalf("initial compact claim missing fencing/lease: %+v", initial[0])
	}

	reclaimed := must[[]FlowRecord](t)(client.Reclaim(ctx, ReclaimOptions{
		Type:            flowType,
		Worker:          "go-sdk-flow-contract-reclaim-worker",
		PartitionKey:    reclaimPartition,
		LeaseMS:         30_000,
		Limit:           1,
		NowMS:           now + 10,
		Payload:         &payload,
		PayloadMaxBytes: Int64(4_096),
		Values:          []string{"result"},
	}))
	requireLen(t, reclaimed, 1)
	assertIntegrationFullRecord(t, reclaimed[0], reclaimID, "reclaim", runID, reclaimValue, 11)
	if reclaimed[0].FencingToken <= initial[0].FencingToken || reclaimed[0].LeaseToken == "" {
		t.Fatalf("reclaim did not issue a new fencing/lease token: initial=%+v reclaimed=%+v", initial[0], reclaimed[0])
	}
	must[*FlowRecord](t)(client.Complete(ctx, CompleteOptions{
		ID:           reclaimed[0].ID,
		LeaseToken:   reclaimed[0].LeaseToken,
		FencingToken: reclaimed[0].FencingToken,
		PartitionKey: reclaimed[0].PartitionKey,
		NowMS:        now + 11,
	}))
}

func assertIntegrationRetryPolicy(t *testing.T, value map[string]any, want RetryPolicy, scope string) {
	t.Helper()
	retry, ok := value["max_retries"]
	if !ok || asInt64(retry) != int64(want.MaxRetries) {
		t.Fatalf("%s max_retries = %#v, want %d", scope, value["max_retries"], want.MaxRetries)
	}
	if asString(value["exhausted_to"]) != want.ExhaustedTo {
		t.Fatalf("%s exhausted_to = %#v, want %q", scope, value["exhausted_to"], want.ExhaustedTo)
	}
	backoff, ok := value["backoff"].(map[string]any)
	if !ok {
		t.Fatalf("%s backoff = %T, want map", scope, value["backoff"])
	}
	if asString(backoff["kind"]) != want.Backoff ||
		asInt64(backoff["base_ms"]) != want.BaseMS ||
		asInt64(backoff["max_ms"]) != want.MaxMS ||
		asInt64(backoff["jitter_pct"]) != int64(want.JitterPct) {
		t.Fatalf("%s backoff = %#v, want kind=%q base=%d max=%d jitter=%d", scope, backoff, want.Backoff, want.BaseMS, want.MaxMS, want.JitterPct)
	}
}

func assertIntegrationFullRecord(t *testing.T, record FlowRecord, id, kind, marker, selected string, attempt int64) {
	t.Helper()
	if record.ID != id || record.Type == "" || record.PartitionKey == "" {
		t.Fatalf("full record identity = %+v", record)
	}
	payload, ok := record.Payload.(map[string]any)
	if !ok || asString(payload["kind"]) != kind || asString(payload["marker"]) != marker {
		t.Fatalf("full record payload = %#v", record.Payload)
	}
	if asString(record.Attributes["tenant"]) != "acme" {
		t.Fatalf("full record attributes = %#v", record.Attributes)
	}
	if asString(record.Values["result"]) != selected {
		t.Fatalf("full record selected values = %#v", record.Values)
	}
	if !integrationStateMetaContains(record.StateMeta, "attempt", attempt) {
		t.Fatalf("full record state_meta = %#v, want nested attempt=%d", record.StateMeta, attempt)
	}
}

func integrationStateMetaContains(meta map[string]any, name string, want int64) bool {
	for _, raw := range meta {
		nested, err := nativeMap(raw)
		if err == nil && asInt64(nested[name]) == want {
			return true
		}
	}
	return false
}
