//go:build integration

package ferricstore

import (
	"context"
	"sort"
	"testing"
	"time"
)

func TestIntegrationFlowQueryConvenienceV010(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := integrationClient(JSONCodec{})
	defer client.Close()

	suffix := integrationSuffix("query-convenience-v010")
	flowType := "go-sdk-query-convenience-" + suffix
	now := time.Now().UnixMilli()
	must[PolicySnapshot](t)(client.SetPolicy(ctx, flowType, PolicyOptions{
		Replace:             Bool(true),
		IndexedAttributes:   []string{"query_marker"},
		IndexedStateMeta:    "risk",
		IndexedStateMetaSet: true,
	}))

	t.Run("list and metadata search", func(t *testing.T) {
		partition := "go-sdk:query-convenience:" + suffix + ":search"
		targetID := "go-sdk:query-convenience:" + suffix + ":target"
		attributeDecoyID := "go-sdk:query-convenience:" + suffix + ":attribute-decoy"
		stateMetaDecoyID := "go-sdk:query-convenience:" + suffix + ":state-meta-decoy"

		fixtures := []CreateOptions{
			{ID: targetID, Type: flowType, State: "queued", Attributes: map[string]any{"query_marker": "wanted"}, StateMeta: map[string]any{"risk": "high"}},
			{ID: attributeDecoyID, Type: flowType, State: "queued", Attributes: map[string]any{"query_marker": "other"}, StateMeta: map[string]any{"risk": "high"}},
			{ID: stateMetaDecoyID, Type: flowType, State: "queued", Attributes: map[string]any{"query_marker": "wanted"}, StateMeta: map[string]any{"risk": "low"}},
			{ID: "go-sdk:query-convenience:" + suffix + ":state-decoy", Type: flowType, State: "waiting", Attributes: map[string]any{"query_marker": "wanted"}, StateMeta: map[string]any{"risk": "high"}},
			{ID: "go-sdk:query-convenience:" + suffix + ":type-decoy", Type: flowType + "-other", State: "queued", Attributes: map[string]any{"query_marker": "wanted"}, StateMeta: map[string]any{"risk": "high"}},
		}
		for index := range fixtures {
			fixtures[index].PartitionKey = partition
			fixtures[index].RunAtMS = now + int64(index)
			fixtures[index].NowMS = now + int64(index)
			fixtures[index].Idempotent = Bool(true)
			must[*FlowRecord](t)(client.Create(ctx, fixtures[index]))
		}

		attribute := must[FlowProjectionField](t)(FlowAttributeProjection("query_marker"))
		projectionQuery := must[string](t)(ProjectFlowQuery(
			"FROM runs WHERE partition_key = @partition AND run_id = @run",
			FlowProjectionRecord,
			FlowRunID,
			FlowRunState,
			attribute,
		))
		projected := must[*FlowQueryResult](t)(client.FlowQuery(
			ctx,
			projectionQuery,
			map[string]any{"partition": partition, "run": targetID},
		))
		if len(projected.Records) != 1 || len(projected.Records[0]) != 3 {
			t.Fatalf("projected record = %#v", projected.Records)
		}
		if _, present := projected.Records[0]["type"]; present {
			t.Fatalf("unrequested field present in %#v", projected.Records[0])
		}

		waitForExactFlowQueryRecords(t, ctx, []string{targetID, attributeDecoyID, stateMetaDecoyID}, func() ([]FlowRecord, error) {
			return client.List(ctx, flowType, ReadOptions{
				PartitionKey: partition,
				State:        "queued",
				Count:        Int(20),
			})
		})

		waitForExactFlowQueryRecords(t, ctx, []string{targetID}, func() ([]FlowRecord, error) {
			return client.Search(ctx, SearchOptions{
				Type:         flowType,
				State:        "queued",
				PartitionKey: partition,
				Count:        Int(20),
				Attributes:   map[string]any{"query_marker": "wanted"},
				StateMeta:    map[string]map[string]any{"queued": {"risk": "high"}},
			})
		})
	})

	t.Run("terminals and failures", func(t *testing.T) {
		partition := "go-sdk:query-convenience:" + suffix + ":terminals"
		completedID := "go-sdk:query-convenience:" + suffix + ":completed"
		failedID := "go-sdk:query-convenience:" + suffix + ":failed"
		cancelledID := "go-sdk:query-convenience:" + suffix + ":cancelled"

		createDueFlow(t, ctx, client, completedID, flowType, "complete-ready", partition, now)
		createDueFlow(t, ctx, client, failedID, flowType, "fail-ready", partition, now)
		createDueFlow(t, ctx, client, cancelledID, flowType, "cancel-ready", partition, now)
		createDueFlow(t, ctx, client, "go-sdk:query-convenience:"+suffix+":non-terminal", flowType, "waiting", partition, now)

		completed := claimOne(t, ctx, client, flowType, "complete-ready", partition, "query-complete-worker", now, 30_000)
		failed := claimOne(t, ctx, client, flowType, "fail-ready", partition, "query-fail-worker", now, 30_000)
		cancelled := claimOne(t, ctx, client, flowType, "cancel-ready", partition, "query-cancel-worker", now, 30_000)
		must[*FlowRecord](t)(client.Complete(ctx, CompleteOptions{
			ID: completed.ID, LeaseToken: completed.LeaseToken, FencingToken: completed.FencingToken, PartitionKey: partition, NowMS: now + 1,
		}))
		must[*FlowRecord](t)(client.Fail(ctx, FailOptions{
			ID: failed.ID, LeaseToken: failed.LeaseToken, FencingToken: failed.FencingToken, PartitionKey: partition, Error: map[string]any{"kind": "expected"}, NowMS: now + 1,
		}))
		must[*FlowRecord](t)(client.Cancel(ctx, CancelOptions{
			ID: cancelled.ID, LeaseToken: cancelled.LeaseToken, FencingToken: cancelled.FencingToken, PartitionKey: partition, Reason: map[string]any{"kind": "expected"}, NowMS: now + 1,
		}))

		waitForExactFlowQueryRecords(t, ctx, []string{failedID}, func() ([]FlowRecord, error) {
			return client.Failures(ctx, flowType, ReadOptions{PartitionKey: partition, Count: Int(20)})
		})
		waitForExactFlowQueryRecords(t, ctx, []string{completedID, failedID, cancelledID}, func() ([]FlowRecord, error) {
			return client.Terminals(ctx, flowType, ReadOptions{PartitionKey: partition, Count: Int(20)})
		})
		waitForExactFlowQueryRecords(t, ctx, []string{completedID}, func() ([]FlowRecord, error) {
			return client.Terminals(ctx, flowType, ReadOptions{PartitionKey: partition, State: "completed", Count: Int(20)})
		})
	})

	t.Run("stuck lease cutoff", func(t *testing.T) {
		partition := "go-sdk:query-convenience:" + suffix + ":stuck"
		stuckID := "go-sdk:query-convenience:" + suffix + ":stuck"
		activeID := "go-sdk:query-convenience:" + suffix + ":active"
		createDueFlow(t, ctx, client, stuckID, flowType, "stale-work", partition, 1_000)
		createDueFlow(t, ctx, client, activeID, flowType, "active-work", partition, 100_000)
		claimOne(t, ctx, client, flowType, "stale-work", partition, "query-stale-worker", 1_000, 60_000)
		claimOne(t, ctx, client, flowType, "active-work", partition, "query-active-worker", 100_000, 60_000)

		waitForExactFlowQueryRecords(t, ctx, []string{stuckID}, func() ([]FlowRecord, error) {
			return client.Stuck(ctx, flowType, partition, Int(20), Int64(1), Int64(120_000))
		})
	})

	t.Run("canonical lineage", func(t *testing.T) {
		partition := "go-sdk:query-convenience:" + suffix + ":lineage"
		parentID := "go-sdk:query-convenience:" + suffix + ":parent"
		childIDs := []string{
			"go-sdk:query-convenience:" + suffix + ":child-a",
			"go-sdk:query-convenience:" + suffix + ":child-b",
		}
		rootID := "go-sdk:query-convenience:" + suffix + ":root"
		correlationID := "go-sdk:query-convenience:" + suffix + ":correlation"

		must[*FlowRecord](t)(client.Create(ctx, CreateOptions{
			ID: parentID, Type: flowType, State: "dispatch", PartitionKey: partition,
			RootFlowID: rootID, CorrelationID: correlationID, RunAtMS: now, NowMS: now, Idempotent: Bool(true),
		}))
		must[*FlowRecord](t)(client.Create(ctx, CreateOptions{
			ID: "go-sdk:query-convenience:" + suffix + ":lineage-decoy", Type: flowType, State: "dispatch", PartitionKey: partition,
			RootFlowID: rootID + ":other", CorrelationID: correlationID + ":other", RunAtMS: now, NowMS: now, Idempotent: Bool(true),
		}))
		for index, childID := range childIDs {
			must[*FlowRecord](t)(client.Create(ctx, CreateOptions{
				ID: childID, Type: flowType, State: "child-ready", PartitionKey: partition,
				ParentFlowID: parentID, RootFlowID: rootID, CorrelationID: correlationID,
				Payload: map[string]any{"child": index}, RunAtMS: now + int64(index+1), NowMS: now + int64(index+1), Idempotent: Bool(true),
			}))
		}

		waitForExactFlowQueryRecords(t, ctx, childIDs, func() ([]FlowRecord, error) {
			return client.ByParent(ctx, parentID, ReadOptions{PartitionKey: partition, Count: Int(20)})
		})
		lineageIDs := append([]string{parentID}, childIDs...)
		waitForExactFlowQueryRecords(t, ctx, lineageIDs, func() ([]FlowRecord, error) {
			return client.ByRoot(ctx, rootID, ReadOptions{PartitionKey: partition, Count: Int(20)})
		})
		waitForExactFlowQueryRecords(t, ctx, lineageIDs, func() ([]FlowRecord, error) {
			return client.ByCorrelation(ctx, correlationID, ReadOptions{PartitionKey: partition, Count: Int(20)})
		})
	})
}

func createDueFlow(t *testing.T, ctx context.Context, client *Client, id, flowType, state, partition string, now int64) {
	t.Helper()
	must[*FlowRecord](t)(client.Create(ctx, CreateOptions{
		ID: id, Type: flowType, State: state, PartitionKey: partition,
		RunAtMS: now, NowMS: now, Idempotent: Bool(true),
	}))
}

func waitForExactFlowQueryRecords(
	t *testing.T,
	ctx context.Context,
	wantIDs []string,
	query func() ([]FlowRecord, error),
) []FlowRecord {
	t.Helper()
	deadline := time.NewTimer(integrationProjectionTimeout)
	defer deadline.Stop()
	retry := time.NewTicker(integrationProjectionRetry)
	defer retry.Stop()
	var lastRecords []FlowRecord
	var lastErr error

	for {
		lastRecords, lastErr = query()
		if lastErr == nil && exactFlowRecordIDs(lastRecords, wantIDs) {
			return lastRecords
		}
		if lastErr != nil && !transientIntegrationFlowQueryError(lastErr) {
			t.Fatalf("FLOW.QUERY while waiting for exact records %v: %v", wantIDs, lastErr)
		}

		select {
		case <-ctx.Done():
			t.Fatalf("FLOW.QUERY while waiting for exact records %v: %v (records=%v, last_error=%v)", wantIDs, ctx.Err(), flowRecordIDs(lastRecords), lastErr)
		case <-deadline.C:
			t.Fatalf("FLOW.QUERY did not converge to records %v within %s (records=%v, last_error=%v)", wantIDs, integrationProjectionTimeout, flowRecordIDs(lastRecords), lastErr)
		case <-retry.C:
		}
	}
}

func exactFlowRecordIDs(records []FlowRecord, wantIDs []string) bool {
	if len(records) != len(wantIDs) {
		return false
	}
	seen := make(map[string]struct{}, len(records))
	for _, record := range records {
		seen[record.ID] = struct{}{}
	}
	if len(seen) != len(wantIDs) {
		return false
	}
	for _, id := range wantIDs {
		if _, ok := seen[id]; !ok {
			return false
		}
	}
	return true
}

func flowRecordIDs(records []FlowRecord) []string {
	ids := make([]string, len(records))
	for index := range records {
		ids[index] = records[index].ID
	}
	sort.Strings(ids)
	return ids
}
