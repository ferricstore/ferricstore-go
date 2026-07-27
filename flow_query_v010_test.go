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
	}, true, "fqc1_next-page-token")}
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
	if result.Page == nil || !result.Page.HasMore || result.Page.Cursor != "fqc1_next-page-token" {
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

func TestV010FlowQueryPreservesSparseProjectedRecords(t *testing.T) {
	query := "FROM runs WHERE run_id = @run RETURN RECORD (run_id, state, attribute['customer'])"
	exec := &fakeExecutor{value: flowQueryPageResponse([]any{
		map[string]any{
			"id": []byte("run-1"), "state": []byte("ready"),
			"attributes": map[string]any{"customer": []byte("acme")},
		},
	}, false, nil)}
	client := NewClientWithExecutor(exec)

	result, err := client.FlowQuery(context.Background(), query, map[string]any{"run": "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 1 || len(result.Records[0]) != 3 {
		t.Fatalf("projected records = %#v", result.Records)
	}
	if _, present := result.Records[0]["type"]; present {
		t.Fatalf("unrequested type field present in %#v", result.Records[0])
	}
}

func TestV010FlowQueryNormalizesProjectedEventID(t *testing.T) {
	exec := &fakeExecutor{value: flowQueryPageResponse([]any{
		map[string]any{
			"event_id": []byte("1710000000000-0"),
			"fields":   map[string]any{"kind": []byte("transitioned")},
		},
	}, false, nil)}
	client := NewClientWithExecutor(exec)

	result, err := client.FlowQuery(
		context.Background(),
		"FROM events WHERE run_id = @run RETURN RECORD (event_id, fields['kind'])",
		map[string]any{"run": "run-1"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Records[0]["event_id"]; got != "1710000000000-0" {
		t.Fatalf("event_id = %#v (%T), want UTF-8 text", got, got)
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
		{name: "unsupported quality", value: func() any {
			response := flowQueryPageResponse([]any{}, false, nil)
			response["quality"].(map[string]any)["exactness"] = "future_exactness"
			return response
		}()},
		{name: "short cursor", value: flowQueryPageResponse([]any{}, true, "fqc1_short")},
		{name: "hydrated exceeds scanned", value: flowQueryResponseWithUsage("hydrated_records", int64(2))},
		{name: "duplicates exceed scanned", value: flowQueryResponseWithUsage("duplicate_entries", int64(2))},
		{name: "pages exceed entries and seeks", value: flowQueryResponseWithUsage("range_pages", int64(3))},
		{name: "residual checks exceed predicates", value: flowQueryResponseWithUsage("residual_checks", int64(13))},
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
		{name: "parameter name with space", query: "FROM runs WHERE run_id = @id RETURN RECORD", params: map[string]any{"bad name": "one"}},
		{name: "parameter name with unicode", query: "FROM runs WHERE run_id = @id RETURN RECORD", params: map[string]any{"unicode_ä": "one"}},
		{name: "parameter name with colon", query: "FROM runs WHERE run_id = @id RETURN RECORD", params: map[string]any{"bad:name": "one"}},
		{name: "invalid UTF-8 text parameter", query: "FROM runs WHERE run_id = @id RETURN RECORD", params: map[string]any{"id": string([]byte{0xff})}},
		{name: "oversized text parameter", query: "FROM runs WHERE run_id = @id RETURN RECORD", params: map[string]any{"id": strings.Repeat("x", 65_536)}},
		{name: "oversized bytes parameter", query: "FROM runs WHERE run_id = @id RETURN RECORD", params: map[string]any{"id": make([]byte, 65_536)}},
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
		"stats":             map[string]any{"source": "fresh"},
		"quality":           flowQueryQualityResponse("live_seek"),
		"bounds":            map[string]any{"scanned_entries": int64(50_000)},
		"pressure":          map[string]any{"resources": []any{}},
		"decision":          map[string]any{"reason": "only_bounded_candidate"},
		"alternatives":      []any{},
		"actual":            nil,
		"diagnostic":        nil,
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
	if explain.Status != "planned" || explain.Plan["path"] != "composite" || explain.Actual != nil ||
		explain.Stats["source"] != "fresh" || explain.Quality.Pagination != "live_seek" ||
		explain.Decision["reason"] != "only_bounded_candidate" || len(explain.Alternatives) != 0 {
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

func TestV011FlowExplainPreservesNonGrammarLeadingWhitespace(t *testing.T) {
	query := "\u00a0FROM runs WHERE run_id = @id RETURN RECORD"
	exec := &fakeExecutor{value: flowExplainResponseForTest("planned")}
	client := NewClientWithExecutor(exec)

	if _, err := client.FlowExplain(context.Background(), query, map[string]any{"id": "run-1"}); err != nil {
		t.Fatal(err)
	}
	if got := exec.calls[0][2]; got != "EXPLAIN "+query {
		t.Fatalf("explain query = %q, want preserved input", got)
	}
}

func TestV011FlowExplainRequiresCompleteActionableEnvelope(t *testing.T) {
	for _, field := range []string{
		"stats", "quality", "pressure", "decision", "alternatives", "actual", "diagnostic",
	} {
		t.Run(field, func(t *testing.T) {
			response := flowExplainResponseForTest("planned")
			delete(response, field)
			client := NewClientWithExecutor(&fakeExecutor{value: response})
			if _, err := client.FlowExplain(context.Background(), "FROM runs WHERE run_id = 'one' RETURN RECORD", nil); err == nil {
				t.Fatalf("accepted EXPLAIN response without %s", field)
			}
		})
	}
}

func TestV011FlowExplainDecodesSpecializedCapabilities(t *testing.T) {
	response := specializedFlowExplainResponseForTest()
	client := NewClientWithExecutor(&fakeExecutor{value: response})

	explain, err := client.FlowExplain(
		context.Background(),
		"FROM runs WHERE run_id = 'one' RETURN RECORD",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if explain.Capabilities == nil ||
		!reflect.DeepEqual(explain.Capabilities.Requested, []string{"flow_query_point_v1"}) ||
		!reflect.DeepEqual(explain.Capabilities.Available, []string{"flow_query_point_v1", "flow_query_history_v1"}) ||
		len(explain.Capabilities.Missing) != 0 || explain.Stats != nil || explain.Quality != nil ||
		explain.Pressure != nil || explain.Decision != nil || len(explain.Alternatives) != 0 {
		t.Fatalf("specialized explain = %#v", explain)
	}
}

func TestV011FlowExplainRejectsMalformedSpecializedEnvelope(t *testing.T) {
	tests := map[string]func(map[string]any){
		"missing capabilities": func(response map[string]any) {
			delete(response, "capabilities")
		},
		"missing capability list": func(response map[string]any) {
			delete(response["capabilities"].(map[string]any), "requested")
		},
		"duplicate capability": func(response map[string]any) {
			response["capabilities"].(map[string]any)["available"] = []any{
				"flow_query_point_v1", "flow_query_point_v1",
			}
		},
		"too many capabilities": func(response map[string]any) {
			items := make([]any, 65)
			for index := range items {
				items[index] = fmt.Sprintf("missing_%d", index)
			}
			response["capabilities"].(map[string]any)["missing"] = items
		},
		"invalid UTF-8 capability": func(response map[string]any) {
			response["capabilities"].(map[string]any)["available"] = []any{string([]byte{0xff})}
		},
		"partial actionable envelope": func(response map[string]any) {
			response["stats"] = map[string]any{}
		},
		"executed status": func(response map[string]any) {
			response["status"] = "executed"
		},
		"extended status field": func(response map[string]any) {
			response["actual"] = nil
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			response := specializedFlowExplainResponseForTest()
			mutate(response)
			client := NewClientWithExecutor(&fakeExecutor{value: response})
			if _, err := client.FlowExplain(context.Background(), "FROM runs WHERE run_id = 'one' RETURN RECORD", nil); err == nil {
				t.Fatal("accepted malformed specialized EXPLAIN response")
			}
		})
	}
}

func TestV010FlowExplainRejectsMalformedQueryFingerprint(t *testing.T) {
	response := map[string]any{
		"version":           flowExplainContract,
		"query_fingerprint": "abc123",
		"status":            "planned",
		"plan":              map[string]any{},
		"estimate":          map[string]any{},
		"stats":             map[string]any{},
		"quality":           flowQueryQualityResponse("live_seek"),
		"bounds":            map[string]any{},
		"pressure":          map[string]any{},
		"decision":          map[string]any{},
		"alternatives":      []any{},
		"actual":            nil,
		"diagnostic":        nil,
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
		"stats":             map[string]any{"source": "fresh"},
		"quality":           flowQueryQualityResponse("live_seek"),
		"bounds":            map[string]any{"scanned_entries": int64(50_000)},
		"pressure":          map[string]any{"resources": []any{}},
		"decision":          map[string]any{"reason": "no_bounded_candidate"},
		"alternatives":      []any{},
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

func TestV011FlowQueryRejectsDiagnosticsOutsideBoundedWireContract(t *testing.T) {
	tests := map[string]func(map[string]any){
		"oversized text": func(diagnostic map[string]any) {
			diagnostic["detail"] = strings.Repeat("x", 1_025)
		},
		"too many context entries": func(diagnostic map[string]any) {
			context := make(map[string]any, 17)
			for index := 0; index < 17; index++ {
				context[fmt.Sprintf("field_%d", index)] = int64(index)
			}
			diagnostic["context"] = context
		},
		"empty context key": func(diagnostic map[string]any) {
			diagnostic["context"] = map[string]any{"": "invalid"}
		},
		"oversized context list": func(diagnostic map[string]any) {
			diagnostic["context"] = map[string]any{"fields": make([]any, 33)}
		},
		"floating context value": func(diagnostic map[string]any) {
			diagnostic["context"] = map[string]any{"estimate": 1.5}
		},
		"context integer overflow": func(diagnostic map[string]any) {
			diagnostic["context"] = map[string]any{"estimate": uint64(math.MaxUint64)}
		},
		"context too deep": func(diagnostic map[string]any) {
			var nested any = int64(1)
			for _, key := range []string{"g", "f", "e", "d", "c", "b", "a"} {
				nested = map[string]any{key: nested}
			}
			diagnostic["context"] = nested
		},
	}

	if _, err := decodeFlowQueryErrorPayload(flowQueryDiagnosticForTest(), nil); err != nil {
		t.Fatalf("valid diagnostic: %v", err)
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			diagnostic := flowQueryDiagnosticForTest()
			mutate(diagnostic)
			if _, err := decodeFlowQueryErrorPayload(diagnostic, nil); err == nil {
				t.Fatal("accepted diagnostic outside the bounded wire contract")
			}
		})
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

func TestV011NativeFlowQueryUsesSharedParameterBounds(t *testing.T) {
	for _, test := range []struct {
		name  any
		value any
	}{
		{name: "bad name", value: "run-1"},
		{name: "unicode_ä", value: "run-1"},
		{name: "bad:name", value: "run-1"},
		{name: "id", value: strings.Repeat("x", 65_536)},
		{name: "id", value: make([]byte, 65_536)},
	} {
		if _, err := buildNativeCommand([]any{
			"FLOW.QUERY", "FQL1", "FROM runs WHERE run_id = @id RETURN RECORD", test.name, test.value,
		}); err == nil {
			t.Fatalf("accepted native parameter %#v", test)
		}
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
		{name: "missing projection capability", mutate: func(capabilities map[string]any) {
			capabilities["flow_query"].(map[string]any)["capabilities"] = []any{
				"flow_query_v1", "flow_explain_v1", "flow_explain_analyze_v1",
				"flow_composite_index_v1", "flow_query_index_status_v1",
			}
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

func flowExplainResponseForTest(status string) map[string]any {
	response := map[string]any{
		"version":           flowExplainContract,
		"query_fingerprint": strings.Repeat("a", 64),
		"status":            status,
		"plan":              map[string]any{"path": "ordered_range"},
		"estimate":          map[string]any{"scanned_entries": int64(1)},
		"stats":             map[string]any{"source": "fresh"},
		"quality":           flowQueryQualityResponse("live_seek"),
		"bounds":            map[string]any{"scanned_entries": int64(50_000)},
		"pressure":          map[string]any{"resources": []any{}},
		"decision":          map[string]any{"reason": "only_bounded_candidate"},
		"alternatives":      []any{},
		"actual":            nil,
		"diagnostic":        nil,
	}
	if status == "executed" {
		response["actual"] = flowQueryUsageResponse(0)
	}
	return response
}

func flowQueryDiagnosticForTest() map[string]any {
	return map[string]any{
		"code":           "unsupported_field",
		"message":        "unsupported query field",
		"detail":         "Use a supported field.",
		"hint":           "See context.supported_fields.",
		"retryable":      false,
		"safe_to_retry":  false,
		"retry_after_ms": int64(0),
		"position":       map[string]any{"byte": int64(18), "line": int64(1), "column": int64(19)},
		"context":        map[string]any{"supported_fields": []any{"partition_key", "run_id", "type"}},
	}
}

func specializedFlowExplainResponseForTest() map[string]any {
	return map[string]any{
		"version":           flowExplainContract,
		"query_fingerprint": strings.Repeat("b", 64),
		"status":            "planned",
		"plan":              map[string]any{"path": "point_lookup"},
		"estimate":          map[string]any{"scanned_entries": int64(1)},
		"bounds":            map[string]any{"scanned_entries": int64(1)},
		"capabilities": map[string]any{
			"requested": []any{"flow_query_point_v1"},
			"available": []any{"flow_query_point_v1", "flow_query_history_v1"},
			"missing":   []any{},
		},
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

func flowQueryResponseWithUsage(field string, value int64) map[string]any {
	response := flowQueryPageResponse([]any{map[string]any{"id": "run-1"}}, false, nil)
	response["usage"].(map[string]any)[field] = value
	return response
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
