package ferricstore

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestV010FlowQueryIndexesDecodesManagementContract(t *testing.T) {
	maximum := uint64(math.MaxUint64)
	response := flowQueryIndexStatusResponseForTest()
	response["registry"] = map[string]any{"epoch": maximum, "catalog_version": maximum}
	response["indexes"].([]any)[0].(map[string]any)["version"] = maximum
	exec := &fakeExecutor{value: response}
	client := NewClientWithExecutor(exec)

	status, err := client.FlowQueryIndexes(context.Background(), "flow_runs_tenant_updated")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(status.Registry.Epoch, maximum) ||
		!reflect.DeepEqual(status.Registry.CatalogVersion, maximum) ||
		len(status.Indexes) != 1 ||
		!reflect.DeepEqual(status.Indexes[0].Version, maximum) ||
		!status.Indexes[0].Queryable ||
		!reflect.DeepEqual(status.Indexes[0].CoveringFields, []string{
			"partition_key", "run_id", "updated_at_ms", "version",
		}) ||
		status.Indexes[0].Source != "runs" || len(status.Indexes[0].Fields) != 2 ||
		status.Indexes[0].Coverage.Validation != "passed" || status.Indexes[0].Build.ScannedRecords != 10 ||
		status.Indexes[0].Validation.ValidatedAtMS == nil || status.Indexes[0].Statistics.Status != "fresh" ||
		status.Indexes[0].Format.QueryRow != "ferric.flow.query.row/v1" ||
		status.Indexes[0].Format.Entry != "ferric.flow.query.composite.entry/v2" ||
		status.Indexes[0].Format.Counter != "ferric.flow.query.composite.counter/v1" {
		t.Fatalf("status = %#v", status)
	}
	want := [][]any{{"FLOW.QUERY.INDEXES", "flow_runs_tenant_updated"}}
	if !reflect.DeepEqual(exec.calls, want) {
		t.Fatalf("calls = %#v, want %#v", exec.calls, want)
	}
}

func TestV011FlowQueryIndexesPreservesUnsigned64BitLifecycleCounters(t *testing.T) {
	maximum := uint64(math.MaxUint64)
	response := flowQueryIndexStatusResponseForTest()
	response["observed_at_ms"] = maximum
	response["statistics_max_age_ms"] = maximum
	index := response["indexes"].([]any)[0].(map[string]any)
	index["state"] = "retiring"
	index["queryable"] = false
	index["coverage"].(map[string]any)["complete_shards"] = maximum
	index["coverage"].(map[string]any)["total_shards"] = maximum
	for _, section := range []string{"build", "validation"} {
		progress := index[section].(map[string]any)
		progress["phase_counts"] = map[string]any{"done": maximum}
		progress["completed_shards"] = maximum
		progress["total_shards"] = maximum
	}
	build := index["build"].(map[string]any)
	build["scanned_records"] = maximum
	build["written_entries"] = maximum
	build["written_bytes"] = maximum
	validation := index["validation"].(map[string]any)
	validation["checked_records"] = maximum
	validation["checked_entries"] = maximum
	validation["validated_at_ms"] = maximum
	index["retirement"] = map[string]any{
		"status": "complete", "phase_counts": map[string]any{"done": maximum},
		"current_phases": []any{"done"}, "completed_shards": maximum, "total_shards": maximum,
		"deleted_entries": maximum, "deleted_bytes": maximum, "rewritten_reverse_rows": maximum,
	}
	statistics := index["statistics"].(map[string]any)
	statistics["samples"] = maximum
	statistics["fresh_samples"] = maximum
	statistics["stale_samples"] = uint64(0)
	statistics["future_samples"] = uint64(0)
	statistics["oldest_collected_at_ms"] = uint64(0)
	statistics["newest_collected_at_ms"] = uint64(0)
	statistics["oldest_age_ms"] = maximum
	statistics["newest_age_ms"] = maximum

	status, err := NewClientWithExecutor(&fakeExecutor{value: response}).FlowQueryIndexes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.ObservedAtMS != maximum ||
		status.Indexes[0].Build.ScannedRecords != maximum ||
		status.Indexes[0].Validation.ValidatedAtMS == nil ||
		*status.Indexes[0].Validation.ValidatedAtMS != maximum ||
		status.Indexes[0].Retirement.DeletedEntries == nil ||
		*status.Indexes[0].Retirement.DeletedEntries != maximum ||
		status.Indexes[0].Statistics.Samples != maximum {
		t.Fatalf("status lost unsigned values: %#v", status)
	}
}

func TestV011FlowQueryIndexesRequiresBoundedCoveringAndFormatMetadata(t *testing.T) {
	validIndex := func() map[string]any {
		return flowQueryIndexStatusResponseForTest()["indexes"].([]any)[0].(map[string]any)
	}
	response := func(index map[string]any) map[string]any {
		status := flowQueryIndexStatusResponseForTest()
		status["indexes"] = []any{index}
		return status
	}

	for name, mutate := range map[string]func(map[string]any){
		"missing covering fields": func(index map[string]any) { delete(index, "covering_fields") },
		"duplicate covering field": func(index map[string]any) {
			index["covering_fields"] = []any{"run_id", "run_id"}
		},
		"too many covering fields": func(index map[string]any) {
			fields := make([]any, 33)
			for position := range fields {
				fields[position] = fmt.Sprintf("attribute.field_%d", position)
			}
			index["covering_fields"] = fields
		},
		"missing format": func(index map[string]any) { delete(index, "format") },
		"invalid nullable counter": func(index map[string]any) {
			index["format"].(map[string]any)["counter"] = false
		},
	} {
		t.Run(name, func(t *testing.T) {
			index := validIndex()
			mutate(index)
			client := NewClientWithExecutor(&fakeExecutor{value: response(index)})
			if _, err := client.FlowQueryIndexes(context.Background()); err == nil {
				t.Fatalf("accepted %s", name)
			}
		})
	}
}

func TestV011FlowQueryIndexesRequiresCompleteLifecycleContract(t *testing.T) {
	for _, field := range []string{
		"source", "fields", "workloads", "count_prefixes", "coverage",
		"build", "validation", "retirement", "statistics",
	} {
		t.Run("missing_"+field, func(t *testing.T) {
			response := flowQueryIndexStatusResponseForTest()
			delete(response["indexes"].([]any)[0].(map[string]any), field)
			client := NewClientWithExecutor(&fakeExecutor{value: response})
			if _, err := client.FlowQueryIndexes(context.Background()); err == nil {
				t.Fatalf("accepted index without %s", field)
			}
		})
	}
	for _, service := range []string{"registry", "lifecycle_worker", "statistics_store", "statistics_worker"} {
		t.Run("missing_service_"+service, func(t *testing.T) {
			response := flowQueryIndexStatusResponseForTest()
			delete(response["services"].(map[string]any), service)
			client := NewClientWithExecutor(&fakeExecutor{value: response})
			if _, err := client.FlowQueryIndexes(context.Background()); err == nil {
				t.Fatalf("accepted services without %s", service)
			}
		})
	}
}

func TestV011FlowQueryIndexesDecodesRetirementWithoutBuildScope(t *testing.T) {
	response := flowQueryIndexStatusResponseForTest()
	index := response["indexes"].([]any)[0].(map[string]any)
	index["state"] = "retiring"
	index["queryable"] = false
	index["retirement"] = map[string]any{
		"status": "pending", "phase_counts": map[string]any{"pending": int64(2)},
		"current_phases": []any{"pending"}, "completed_shards": int64(0), "total_shards": int64(2),
		"deleted_entries": int64(0), "deleted_bytes": int64(0), "rewritten_reverse_rows": int64(0),
	}
	client := NewClientWithExecutor(&fakeExecutor{value: response})
	status, err := client.FlowQueryIndexes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.Indexes[0].Retirement.Status != "pending" ||
		status.Indexes[0].Retirement.CompletedShards == nil ||
		*status.Indexes[0].Retirement.CompletedShards != 0 {
		t.Fatalf("retirement = %#v", status.Indexes[0].Retirement)
	}
}

func TestV011FlowQueryIndexesRejectsCrossFieldContradictions(t *testing.T) {
	tests := map[string]func(map[string]any){
		"invalid source": func(index map[string]any) { index["source"] = "events" },
		"invalid first field": func(index map[string]any) {
			index["fields"].([]any)[0].(map[string]any)["name"] = "type"
		},
		"counter without prefix": func(index map[string]any) { index["count_prefixes"] = []any{} },
		"inconsistent shard totals": func(index map[string]any) {
			index["validation"].(map[string]any)["total_shards"] = int64(3)
		},
		"invalid queryable flag": func(index map[string]any) { index["queryable"] = false },
		"invalid validation fields": func(index map[string]any) {
			index["validation"].(map[string]any)["mismatches"] = int64(1)
		},
		"invalid statistics counters": func(index map[string]any) {
			index["statistics"].(map[string]any)["fresh_samples"] = int64(1)
		},
		"invalid statistics ages": func(index map[string]any) {
			index["statistics"].(map[string]any)["newest_age_ms"] = int64(2)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			response := flowQueryIndexStatusResponseForTest()
			mutate(response["indexes"].([]any)[0].(map[string]any))
			client := NewClientWithExecutor(&fakeExecutor{value: response})
			if _, err := client.FlowQueryIndexes(context.Background()); err == nil {
				t.Fatalf("accepted %s", name)
			}
		})
	}
}

func TestV011FlowQueryIndexesRejectsFilteredIdentityMismatch(t *testing.T) {
	response := flowQueryIndexStatusResponseForTest()
	response["indexes"].([]any)[0].(map[string]any)["id"] = "different_index"
	client := NewClientWithExecutor(&fakeExecutor{value: response})
	if _, err := client.FlowQueryIndexes(context.Background(), "flow_runs_tenant_updated"); err == nil {
		t.Fatal("accepted a filtered response for a different index")
	}
}

func TestV010FlowQueryIndexesRejectsTextEncodedGenerations(t *testing.T) {
	for _, encoded := range []any{"3", []byte("3")} {
		response := map[string]any{
			"contract_version":      "ferric.flow.query.indexes/v1",
			"observed_at_ms":        int64(1000),
			"statistics_max_age_ms": int64(60_000),
			"registry":              map[string]any{"epoch": encoded, "catalog_version": uint64(3)},
			"services":              map[string]any{"registry": "ready"},
			"indexes":               []any{},
		}
		client := NewClientWithExecutor(&fakeExecutor{value: response})
		if _, err := client.FlowQueryIndexes(context.Background()); err == nil {
			t.Fatalf("accepted text-encoded registry epoch %#v", encoded)
		}
	}
}

func TestV010FlowQueryIndexesRejectsOversizedCatalog(t *testing.T) {
	indexes := make([]any, 33)
	for index := range indexes {
		indexes[index] = map[string]any{
			"id": fmt.Sprintf("index-%d", index), "version": uint64(1),
			"build_id": fmt.Sprintf("build-%d", index), "state": "active", "queryable": true,
		}
	}
	response := map[string]any{
		"contract_version":      "ferric.flow.query.indexes/v1",
		"observed_at_ms":        int64(1000),
		"statistics_max_age_ms": int64(60_000),
		"registry":              map[string]any{"epoch": uint64(1), "catalog_version": uint64(3)},
		"services":              map[string]any{"registry": "ready"},
		"indexes":               indexes,
	}
	client := NewClientWithExecutor(&fakeExecutor{value: response})
	if _, err := client.FlowQueryIndexes(context.Background()); err == nil {
		t.Fatal("accepted an index catalog larger than the negotiated query contract")
	}
}

func TestV010FlowQueryIndexesValidatesServerIdentifierContractBeforeIO(t *testing.T) {
	for _, indexID := range []string{
		"contains space",
		"contains/slash",
		strings.Repeat("a", 65),
	} {
		exec := &fakeExecutor{}
		client := NewClientWithExecutor(exec)

		if _, err := client.FlowQueryIndexes(context.Background(), indexID); err == nil {
			t.Fatalf("accepted invalid index id %q", indexID)
		}
		if len(exec.calls) != 0 {
			t.Fatalf("index validation performed IO: %#v", exec.calls)
		}
	}

	for _, indexIDs := range [][]string{{""}, {"first", "second"}} {
		exec := &fakeExecutor{}
		client := NewClientWithExecutor(exec)
		if _, err := client.FlowQueryIndexes(context.Background(), indexIDs...); err == nil {
			t.Fatalf("accepted index ids %#v", indexIDs)
		}
		if len(exec.calls) != 0 {
			t.Fatalf("index validation performed IO: %#v", exec.calls)
		}
	}
}

func flowQueryIndexStatusResponseForTest() map[string]any {
	return map[string]any{
		"contract_version":      flowQueryIndexesContract,
		"observed_at_ms":        int64(1_000_000),
		"statistics_max_age_ms": int64(300_000),
		"registry":              map[string]any{"epoch": uint64(2), "catalog_version": uint64(3)},
		"services": map[string]any{
			"registry": "ready", "lifecycle_worker": "ready",
			"statistics_store": "ready", "statistics_worker": "unavailable",
		},
		"indexes": []any{map[string]any{
			"id": "flow_runs_tenant_updated", "version": uint64(1), "build_id": "build-1",
			"source": "runs", "state": "active", "queryable": true,
			"fields": []any{
				map[string]any{"name": "partition_key", "direction": "asc", "encoding": "hashed"},
				map[string]any{"name": "updated_at_ms", "direction": "desc", "encoding": "ordered"},
			},
			"workloads":      []any{"tenant_updated"},
			"count_prefixes": []any{int64(1)},
			"covering_fields": []any{
				"partition_key", "run_id", "updated_at_ms", "version",
			},
			"format": map[string]any{
				"query_row": "ferric.flow.query.row/v1",
				"key":       "ferric.flow.query.composite.key/v1",
				"entry":     "ferric.flow.query.composite.entry/v2",
				"reverse":   "ferric.flow.query.composite.reverse/v1",
				"counter":   "ferric.flow.query.composite.counter/v1",
			},
			"coverage": map[string]any{
				"complete_shards": int64(2), "total_shards": int64(2), "validation": "passed",
			},
			"build": map[string]any{
				"scope": "catalog_build", "phase_counts": map[string]any{"done": int64(2)},
				"current_phases": []any{"done"}, "completed_shards": int64(2), "total_shards": int64(2),
				"scanned_records": int64(10), "written_entries": int64(10), "written_bytes": int64(900),
			},
			"validation": map[string]any{
				"scope": "catalog_build", "status": "passed", "phase_counts": map[string]any{"done": int64(2)},
				"current_phases": []any{"done"}, "completed_shards": int64(2), "total_shards": int64(2),
				"checked_records": int64(10), "checked_entries": int64(10), "mismatches": int64(0),
				"failure_reason": nil, "validated_at_ms": int64(999_000),
			},
			"retirement": map[string]any{"status": "not_applicable"},
			"statistics": map[string]any{
				"status": "fresh", "samples": int64(2), "fresh_samples": int64(2),
				"stale_samples": int64(0), "future_samples": int64(0),
				"oldest_collected_at_ms": int64(998_000), "newest_collected_at_ms": int64(999_000),
				"oldest_age_ms": int64(2_000), "newest_age_ms": int64(1_000),
			},
		}},
	}
}
