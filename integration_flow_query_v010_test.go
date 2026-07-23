//go:build integration

package ferricstore

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestIntegrationFlowQueryPlannerV010(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(JSONCodec{})
	defer client.Close()

	suffix := integrationSuffix("query-v010")
	partition := "go-sdk:query:" + suffix + ":partition"
	flowType := "go-sdk-query-" + suffix
	state := "query-ready"
	now := time.Now().UnixMilli()
	ids := make([]string, 3)
	for index := range ids {
		ids[index] = fmt.Sprintf("go-sdk:query:%s:%d", suffix, index)
		if _, err := client.Create(ctx, CreateOptions{
			ID: ids[index], Type: flowType, State: state, PartitionKey: partition,
			Payload: map[string]any{"secret": fmt.Sprintf("payload-%d", index)},
			RunAtMS: now + int64(index), NowMS: now + int64(index), Idempotent: Bool(true),
		}); err != nil {
			t.Fatal(err)
		}
	}

	baseQuery := "FROM runs WHERE partition_key = @partition AND type = @type AND state = @state ORDER BY updated_at_ms ASC LIMIT 2 RETURN RECORDS"
	params := map[string]any{"partition": partition, "type": flowType, "state": state}
	first := waitForFlowQueryResult(t, ctx, func() (*FlowQueryResult, error) {
		return client.FlowQuery(ctx, baseQuery, params)
	}, func(result *FlowQueryResult) bool {
		return result != nil && len(result.Records) == 2 && result.Page != nil && result.Page.HasMore
	})
	if first.Page.Cursor == "" || first.Usage.ResultRecords != 2 || first.Quality.Pagination == "" {
		t.Fatalf("first query page = %#v", first)
	}

	nextQuery := "FROM runs WHERE partition_key = @partition AND type = @type AND state = @state ORDER BY updated_at_ms ASC LIMIT 2 CURSOR @cursor RETURN RECORDS"
	nextParams := map[string]any{
		"partition": partition, "type": flowType, "state": state, "cursor": first.Page.Cursor,
	}
	second, err := client.FlowQuery(ctx, nextQuery, nextParams)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Records) != 1 || second.Page == nil || second.Page.HasMore {
		t.Fatalf("second query page = %#v", second)
	}
	assertFlowQueryIDs(t, append(append([]map[string]any{}, first.Records...), second.Records...), ids)

	countQuery := "FROM runs WHERE partition_key = @partition AND type = @type AND state = @state RETURN COUNT"
	count := waitForFlowQueryResult(t, ctx, func() (*FlowQueryResult, error) {
		return client.FlowQuery(ctx, countQuery, params)
	}, func(result *FlowQueryResult) bool {
		return result != nil && result.Count != nil && *result.Count == int64(len(ids))
	})
	if count.Page != nil || count.Usage.ResultRecords != 1 {
		t.Fatalf("count query = %#v", count)
	}

	explain, err := client.FlowExplain(ctx, baseQuery, params)
	if err != nil {
		t.Fatal(err)
	}
	if explain.Status != "planned" || explain.Actual != nil || explain.Plan["path"] == nil {
		t.Fatalf("EXPLAIN = %#v", explain)
	}
	analyze, err := client.FlowExplainAnalyze(ctx, baseQuery, params)
	if err != nil {
		t.Fatal(err)
	}
	if analyze.Status != "executed" || analyze.Actual == nil || analyze.Actual.ResultRecords != 2 {
		t.Fatalf("EXPLAIN ANALYZE = %#v", analyze)
	}

	status, err := client.FlowQueryIndexes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Registry.CatalogVersion <= 0 || len(status.Indexes) == 0 {
		t.Fatalf("FLOW.QUERY.INDEXES = %#v", status)
	}

	_, err = client.FlowQuery(ctx, "FROM runs WHERE partition_key = @partition AND unsupported = 1 ORDER BY updated_at_ms ASC LIMIT 2 RETURN RECORDS", map[string]any{"partition": partition})
	var queryErr *FlowQueryError
	if !errors.As(err, &queryErr) || queryErr.Code != "unsupported_field" || queryErr.Position == nil {
		t.Fatalf("actionable query error = %T %#v", err, err)
	}

	listed := waitForFlowQueryRecord(t, ctx, ids[0], func() ([]FlowRecord, error) {
		return client.List(ctx, flowType, ReadOptions{PartitionKey: partition, State: state, Count: Int(3)})
	})
	if len(listed) != len(ids) {
		t.Fatalf("List FQL convenience = %#v", listed)
	}
}

func assertFlowQueryIDs(t *testing.T, records []map[string]any, want []string) {
	t.Helper()
	seen := make(map[string]struct{}, len(records))
	for _, record := range records {
		id, ok := record["id"].(string)
		if !ok || id == "" {
			t.Fatalf("query record has no id: %#v", record)
		}
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("query pagination duplicated %q", id)
		}
		seen[id] = struct{}{}
	}
	for _, id := range want {
		if _, ok := seen[id]; !ok {
			t.Fatalf("query pagination omitted %q: %#v", id, records)
		}
	}
}
