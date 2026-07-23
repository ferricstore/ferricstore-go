package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestV010FlowQueryBuildsDeterministicRequestAndDecodesPage(t *testing.T) {
	query := "FROM runs WHERE partition_key = @tenant AND type = @type ORDER BY updated_at_ms DESC LIMIT 2 RETURN RECORDS"
	exec := &fakeExecutor{value: flowQueryPageResponse([]any{
		map[string]any{
			"id": []byte("run-2"), "type": []byte("order"), "state": []byte("ready"),
			"partition_key": []byte("tenant-a"), "root_flow_id": []byte("run-2"),
			"attributes": map[string]any{"opaque": []byte{0xff}}, "updated_at_ms": int64(20),
		},
	}, true, "fqc1_next")}
	client := NewClientWithExecutor(exec)

	result, err := client.FlowQuery(context.Background(), query, map[string]any{
		"type": "order", "tenant": "tenant-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 1 || result.Records[0]["id"] != "run-2" {
		t.Fatalf("records = %#v", result.Records)
	}
	if result.Records[0]["type"] != "order" || result.Records[0]["state"] != "ready" ||
		result.Records[0]["partition_key"] != "tenant-a" || result.Records[0]["root_flow_id"] != "run-2" {
		t.Fatalf("record text fields = %#v", result.Records[0])
	}
	opaque := result.Records[0]["attributes"].(map[string]any)["opaque"]
	if !reflect.DeepEqual(opaque, []byte{0xff}) {
		t.Fatalf("opaque attribute = %#v", opaque)
	}
	if result.Page == nil || !result.Page.HasMore || result.Page.Cursor != "fqc1_next" {
		t.Fatalf("page = %#v", result.Page)
	}
	if result.Quality.Exactness != "projected_exact" || result.Usage.ResultRecords != 1 {
		t.Fatalf("quality/usage = %#v / %#v", result.Quality, result.Usage)
	}
	want := []any{"FLOW.QUERY", "FQL1", query, "tenant", "tenant-a", "type", "order"}
	if !reflect.DeepEqual(exec.calls, [][]any{want}) {
		t.Fatalf("calls = %#v, want %#v", exec.calls, [][]any{want})
	}
}

func TestV010FlowQueryDecodesExactCount(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{
		"version": "ferric.flow.query.result/v1",
		"result":  map[string]any{"kind": "count", "value": int64(7)},
		"quality": flowQueryQualityResponse("none"),
		"usage":   flowQueryUsageResponse(1),
	}}
	client := NewClientWithExecutor(exec)

	result, err := client.FlowQuery(
		context.Background(),
		"FROM runs WHERE partition_key = @tenant AND type = @type RETURN COUNT",
		map[string]any{"tenant": "tenant-a", "type": "order"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Count == nil || *result.Count != 7 || result.Page != nil || result.Records != nil {
		t.Fatalf("count result = %#v", result)
	}
}

func TestV010FlowQueryRejectsMalformedOrAmbiguousResponses(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{name: "contract", value: map[string]any{"version": "future/v2"}},
		{name: "both shapes", value: map[string]any{
			"version": "ferric.flow.query.result/v1", "records": []any{},
			"page":    map[string]any{"has_more": false, "cursor": nil},
			"result":  map[string]any{"kind": "count", "value": int64(0)},
			"quality": flowQueryQualityResponse("none"), "usage": flowQueryUsageResponse(0),
		}},
		{name: "missing cursor", value: flowQueryPageResponse([]any{}, true, nil)},
		{name: "negative usage", value: func() any {
			response := flowQueryPageResponse([]any{}, false, nil)
			response["usage"].(map[string]any)["scanned_entries"] = int64(-1)
			return response
		}()},
		{name: "text-encoded usage", value: func() any {
			response := flowQueryPageResponse([]any{}, false, nil)
			response["usage"].(map[string]any)["scanned_entries"] = "0"
			return response
		}()},
		{name: "invalid UTF-8 quality", value: func() any {
			response := flowQueryPageResponse([]any{}, false, nil)
			response["quality"].(map[string]any)["exactness"] = []byte{0xff}
			return response
		}()},
		{name: "oversized quality", value: func() any {
			response := flowQueryPageResponse([]any{}, false, nil)
			response["quality"].(map[string]any)["exactness"] = strings.Repeat("x", 65)
			return response
		}()},
		{name: "invalid UTF-8 record text", value: flowQueryPageResponse(
			[]any{map[string]any{"id": []byte{0xff}}}, false, nil,
		)},
		{name: "empty record text", value: flowQueryPageResponse(
			[]any{map[string]any{"id": []byte{}}}, false, nil,
		)},
		{name: "too many records", value: func() any {
			records := make([]any, 101)
			for index := range records {
				records[index] = map[string]any{"id": fmt.Sprintf("run-%d", index)}
			}
			return flowQueryPageResponse(records, false, nil)
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := NewClientWithExecutor(&fakeExecutor{value: test.value})
			if _, err := client.FlowQuery(context.Background(), "FROM runs WHERE run_id = 'one' RETURN RECORD", nil); err == nil {
				t.Fatalf("accepted malformed response %#v", test.value)
			}
		})
	}
}

func TestV010FlowQueryValidatesBoundedWireInputsBeforeIO(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		params map[string]any
	}{
		{name: "empty query"},
		{name: "oversized query", query: strings.Repeat("x", 16*1024+1)},
		{name: "invalid UTF-8 query", query: "FROM runs " + string([]byte{0xff})},
		{name: "invalid parameter name", query: "FROM runs WHERE run_id = @id RETURN RECORD", params: map[string]any{"": "one"}},
		{name: "invalid UTF-8 parameter name", query: "FROM runs WHERE run_id = @id RETURN RECORD", params: map[string]any{string([]byte{0xff}): "one"}},
		{name: "invalid UTF-8 text parameter", query: "FROM runs WHERE run_id = @id RETURN RECORD", params: map[string]any{"id": string([]byte{0xff})}},
		{name: "unsupported parameter", query: "FROM runs WHERE run_id = @id RETURN RECORD", params: map[string]any{"id": map[string]any{"nested": true}}},
		{name: "too many parameters", query: "FROM runs WHERE run_id = @id RETURN RECORD", params: func() map[string]any {
			params := make(map[string]any, 65)
			for index := 0; index < 65; index++ {
				params[string(rune('a'+index%26))+string(rune('A'+index/26))] = int64(index)
			}
			return params
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{}
			client := NewClientWithExecutor(exec)
			if _, err := client.FlowQuery(context.Background(), test.query, test.params); err == nil {
				t.Fatal("expected validation error")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("validation performed IO: %#v", exec.calls)
			}
		})
	}
}

func TestV010FlowQueryRejectsExplainAcrossFQLWhitespaceBeforeIO(t *testing.T) {
	for _, separator := range []string{" ", "\t", "\n", "\r"} {
		t.Run(fmt.Sprintf("separator_%x", separator), func(t *testing.T) {
			exec := &fakeExecutor{}
			client := NewClientWithExecutor(exec)
			query := "EXPLAIN" + separator + "FROM runs WHERE run_id = 'one' RETURN RECORD"

			if _, err := client.FlowQuery(context.Background(), query, nil); err == nil {
				t.Fatal("expected dedicated EXPLAIN API error")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("EXPLAIN validation performed IO: %#v", exec.calls)
			}
		})
	}
}

func TestV010FlowExplainRejectsEmptyAndAlreadyPrefixedQueriesBeforeIO(t *testing.T) {
	for _, query := range []string{"", " \t\n ", "EXPLAIN\tFROM runs WHERE run_id = 'one' RETURN RECORD"} {
		exec := &fakeExecutor{}
		client := NewClientWithExecutor(exec)

		if _, err := client.FlowExplain(context.Background(), query, nil); err == nil {
			t.Fatalf("FlowExplain accepted %q", query)
		}
		if len(exec.calls) != 0 {
			t.Fatalf("FlowExplain validation performed IO: %#v", exec.calls)
		}
	}
}

func TestV010FlowExplainAndAnalyzeUseDedicatedResultContract(t *testing.T) {
	query := "FROM runs WHERE partition_key = @tenant AND type = 'order' ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS"
	explainResponse := map[string]any{
		"version":           "ferric.flow.explain/v1",
		"query_fingerprint": strings.Repeat("a", 64),
		"status":            "planned",
		"plan":              map[string]any{"path": "composite", "index": "flow_runs_tenant_type_updated"},
		"estimate":          map[string]any{"scanned_entries": int64(10)},
		"bounds":            map[string]any{"scanned_entries": int64(50_000)},
		"actual":            nil,
	}
	exec := &fakeExecutor{values: []any{
		explainResponse,
		func() any {
			copy := make(map[string]any, len(explainResponse)+1)
			for key, value := range explainResponse {
				copy[key] = value
			}
			copy["status"] = "executed"
			copy["actual"] = flowQueryUsageResponse(0)
			return copy
		}(),
	}}
	client := NewClientWithExecutor(exec)

	explain, err := client.FlowExplain(context.Background(), query, map[string]any{"tenant": "tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	if explain.Status != "planned" || explain.Plan["path"] != "composite" || explain.Actual != nil {
		t.Fatalf("explain = %#v", explain)
	}
	analyze, err := client.FlowExplainAnalyze(context.Background(), query, map[string]any{"tenant": "tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	if analyze.Actual == nil || analyze.Actual.WallTimeUS != 10 {
		t.Fatalf("analyze = %#v", analyze)
	}
	if got := exec.calls[0][2]; got != "EXPLAIN "+query {
		t.Fatalf("explain query = %q", got)
	}
	if got := exec.calls[1][2]; got != "EXPLAIN ANALYZE "+query {
		t.Fatalf("analyze query = %q", got)
	}
}

func TestV010FlowExplainRejectsMalformedQueryFingerprint(t *testing.T) {
	response := map[string]any{
		"version":           flowExplainContract,
		"query_fingerprint": "abc123",
		"status":            "planned",
		"plan":              map[string]any{},
		"estimate":          map[string]any{},
		"bounds":            map[string]any{},
	}
	client := NewClientWithExecutor(&fakeExecutor{value: response})

	if _, err := client.FlowExplain(context.Background(), "FROM runs WHERE run_id = 'one' RETURN RECORD", nil); err == nil {
		t.Fatal("accepted malformed query fingerprint")
	}
}

func TestV010FlowExplainSurfacesRejectedPlanDiagnostic(t *testing.T) {
	response := map[string]any{
		"version":           "ferric.flow.explain/v1",
		"query_fingerprint": strings.Repeat("a", 64),
		"status":            "rejected",
		"plan":              map[string]any{"path": "reject", "fallback_reason": "no_active_bounded_index"},
		"estimate":          map[string]any{"scanned_entries": int64(0)},
		"bounds":            map[string]any{"scanned_entries": int64(50_000)},
		"actual":            nil,
		"diagnostic": map[string]any{
			"code": "query_no_bounded_plan", "message": "no bounded plan",
			"detail":    "The active index catalog cannot serve this predicate set.",
			"hint":      "Inspect FLOW.QUERY.INDEXES and add the suggested index.",
			"retryable": false, "safe_to_retry": false, "retry_after_ms": int64(0),
			"position": map[string]any{"byte": int64(25), "line": int64(1), "column": int64(25)},
			"context":  map[string]any{"status_command": "FLOW.QUERY.INDEXES"},
		},
	}
	client := NewClientWithExecutor(&fakeExecutor{value: response})

	explain, err := client.FlowExplain(context.Background(), "FROM runs WHERE partition_key = @tenant ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS", map[string]any{"tenant": "tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	if explain.Diagnostic == nil || explain.Diagnostic.Code != "query_no_bounded_plan" ||
		explain.Diagnostic.Position == nil || explain.Diagnostic.Context["status_command"] != "FLOW.QUERY.INDEXES" {
		t.Fatalf("rejected explain diagnostic = %#v", explain.Diagnostic)
	}
}

func TestV010FlowQueryPreservesStructuredDiagnostics(t *testing.T) {
	nativeErr := NativeError{Status: 4, Kind: "bad_request", Value: map[string]any{
		"code":           "unsupported_field",
		"message":        "unsupported query field",
		"detail":         "Use a supported field.",
		"hint":           "Valid fields are listed in context.supported_fields.",
		"retryable":      false,
		"safe_to_retry":  false,
		"retry_after_ms": int64(0),
		"position":       map[string]any{"byte": int64(18), "line": int64(1), "column": int64(19)},
		"context":        map[string]any{"supported_fields": []any{"partition_key", "run_id", "type"}},
	}}
	for _, transportErr := range []error{nativeErr, &nativeErr} {
		client := NewClientWithExecutor(&fakeExecutor{err: transportErr})

		_, err := client.FlowQuery(context.Background(), "FROM runs WHERE nope = 1 RETURN RECORD", nil)
		var queryErr *FlowQueryError
		if !errors.As(err, &queryErr) {
			t.Fatalf("error = %T %v, want *FlowQueryError", err, err)
		}
		if queryErr.Code != "unsupported_field" || queryErr.Position == nil || queryErr.Position.Column != 19 {
			t.Fatalf("query error = %#v", queryErr)
		}
		if !reflect.DeepEqual(queryErr.cause, transportErr) {
			t.Fatal("structured query error does not preserve its exact transport cause")
		}
	}
}

func TestV010FlowQueryIndexesDecodesManagementContract(t *testing.T) {
	maximum := uint64(math.MaxUint64)
	exec := &fakeExecutor{value: map[string]any{
		"contract_version":      "ferric.flow.query.indexes/v1",
		"observed_at_ms":        int64(1000),
		"statistics_max_age_ms": int64(60_000),
		"registry":              map[string]any{"epoch": maximum, "catalog_version": maximum},
		"services":              map[string]any{"registry": "ready"},
		"indexes": []any{map[string]any{
			"id": "flow_runs_tenant_updated", "version": maximum, "build_id": "build-1",
			"state": "active", "queryable": true,
		}},
	}}
	client := NewClientWithExecutor(exec)

	status, err := client.FlowQueryIndexes(context.Background(), "flow_runs_tenant_updated")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(status.Registry.Epoch, maximum) ||
		!reflect.DeepEqual(status.Registry.CatalogVersion, maximum) ||
		len(status.Indexes) != 1 ||
		!reflect.DeepEqual(status.Indexes[0].Version, maximum) ||
		!status.Indexes[0].Queryable {
		t.Fatalf("status = %#v", status)
	}
	want := [][]any{{"FLOW.QUERY.INDEXES", "flow_runs_tenant_updated"}}
	if !reflect.DeepEqual(exec.calls, want) {
		t.Fatalf("calls = %#v, want %#v", exec.calls, want)
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

func TestV010NativeFlowQueryUsesDedicatedOpcodeAndMapPayload(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.QUERY", "FQL1", "FROM runs WHERE run_id = @id RETURN RECORD", "id", "run-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpFlowQuery || command.laneID != 1 || command.flags != 0 {
		t.Fatalf("command metadata = opcode:%#x lane:%d flags:%#x", command.opcode, command.laneID, command.flags)
	}
	want := map[string]any{
		"version": "FQL1", "query": "FROM runs WHERE run_id = @id RETURN RECORD",
		"params": map[string]any{"id": "run-1"},
	}
	if !reflect.DeepEqual(command.payload, want) {
		t.Fatalf("payload = %#v, want %#v", command.payload, want)
	}
}

func TestV010NativeFlowQueryRejectsInvalidUTF8ParameterNames(t *testing.T) {
	_, err := buildNativeCommand([]any{
		"FLOW.QUERY", "FQL1", "FROM runs WHERE run_id = @id RETURN RECORD",
		string([]byte{0xff}), "run-1",
	})
	if err == nil {
		t.Fatal("accepted an invalid UTF-8 native parameter name")
	}
}

func TestV010RemovedFlowCollectionsHaveNoDedicatedNativeOpcodes(t *testing.T) {
	commands := [][]any{
		{"FLOW.LIST", "order"},
		{"FLOW.SEARCH", "TYPE", "order"},
		{"FLOW.TERMINALS", "order"},
		{"FLOW.FAILURES", "order"},
		{"FLOW.STUCK", "order", "OLDER_THAN", int64(1)},
		{"FLOW.BY_PARENT", "parent-1"},
		{"FLOW.BY_ROOT", "root-1"},
		{"FLOW.BY_CORRELATION", "correlation-1"},
	}

	for _, args := range commands {
		name := args[0].(string)
		t.Run(name, func(t *testing.T) {
			command, err := buildNativeCommand(args)
			if err != nil {
				t.Fatal(err)
			}
			if command.opcode != nativeOpCommandExec {
				t.Fatalf("%s selected removed dedicated opcode %#x", name, command.opcode)
			}
			payload, ok := command.payload.(map[string]any)
			if !ok || payload["command"] != name {
				t.Fatalf("%s fallback payload = %#v", name, command.payload)
			}
		})
	}
}

func TestV010HelloNegotiatesCompleteOSSFlowQueryContract(t *testing.T) {
	contract, err := parseNativeHelloContract(nativeHelloForTest(), nativeDefaultResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	if contract.flowQuery.RequestContract != flowQueryRequestContract ||
		contract.flowQuery.ResultContract != flowQueryResultContract ||
		contract.flowQuery.ExplainContract != flowExplainContract ||
		contract.flowQuery.IndexStatusContract != flowQueryIndexesContract ||
		!contract.flowQuery.supportsCapability("flow_query_index_status_v1") ||
		!contract.flowQuery.supportsShape("runs_by_partition_predicates_count") {
		t.Fatalf("flow query contract = %#v", contract.flowQuery)
	}
}

func TestV010HelloRejectsIncompleteFlowQueryManifest(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "missing manifest", mutate: func(capabilities map[string]any) { delete(capabilities, "flow_query") }},
		{name: "wrong result contract", mutate: func(capabilities map[string]any) {
			capabilities["flow_query"].(map[string]any)["result_contract"] = "future/v2"
		}},
		{name: "wrong index status contract", mutate: func(capabilities map[string]any) {
			capabilities["flow_query"].(map[string]any)["index_status_contract"] = "future/v2"
		}},
		{name: "missing language", mutate: func(capabilities map[string]any) {
			capabilities["flow_query"].(map[string]any)["language_versions"] = []any{}
		}},
		{name: "missing capability", mutate: func(capabilities map[string]any) {
			capabilities["flow_query"].(map[string]any)["capabilities"] = []any{"flow_query_v1"}
		}},
		{name: "missing count shape", mutate: func(capabilities map[string]any) {
			capabilities["flow_query"].(map[string]any)["shapes"] = []any{"runs_by_run_id_record"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hello := nativeHelloForTest()
			capabilities := hello["capabilities"].(map[string]any)
			test.mutate(capabilities)
			if _, err := parseNativeHelloContract(hello, nativeDefaultResponseBytes); err == nil || !strings.Contains(err.Error(), "flow_query") {
				t.Fatalf("error = %v, want flow_query negotiation failure", err)
			}
		})
	}
}

func flowQueryPageResponse(records []any, hasMore bool, cursor any) map[string]any {
	return map[string]any{
		"version": "ferric.flow.query.result/v1",
		"records": records,
		"page":    map[string]any{"has_more": hasMore, "cursor": cursor},
		"quality": flowQueryQualityResponse("live_seek"),
		"usage":   flowQueryUsageResponse(int64(len(records))),
	}
}

func flowQueryQualityResponse(pagination string) map[string]any {
	return map[string]any{
		"exactness": "projected_exact", "freshness": "projection_watermark",
		"coverage": "complete", "pagination": pagination,
	}
}

func flowQueryUsageResponse(resultRecords int64) map[string]any {
	return map[string]any{
		"range_seeks": int64(1), "range_pages": int64(1),
		"scanned_entries": resultRecords, "scanned_bytes": int64(128),
		"hydrated_records": resultRecords, "residual_checks": resultRecords,
		"duplicate_entries": int64(0), "result_records": resultRecords,
		"response_bytes": int64(256), "memory_high_water_bytes": int64(1024),
		"wall_time_us": int64(10),
	}
}

var flowQueryArgsBenchmarkSink []any

func BenchmarkV010FlowQueryCommandArgs32Parameters(b *testing.B) {
	params := make(map[string]any, 32)
	for index := 0; index < 32; index++ {
		params[fmt.Sprintf("parameter_%02d", index)] = int64(index)
	}
	query := "FROM runs WHERE partition_key = @parameter_00 ORDER BY updated_at_ms ASC LIMIT 10 RETURN RECORDS"
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		args, err := flowQueryCommandArgs(query, params)
		if err != nil {
			b.Fatal(err)
		}
		flowQueryArgsBenchmarkSink = args
	}
}
