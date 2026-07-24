package ferricstore

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type flowQueryFastPathProbe struct {
	genericCalls int
	directCalls  int
	prepared     preparedFlowQuery
	value        any
	err          error
}

func (e *flowQueryFastPathProbe) Do(context.Context, ...any) (any, error) {
	e.genericCalls++
	return nil, errors.New("generic FLOW.QUERY execution should not be used")
}

func (e *flowQueryFastPathProbe) executePreparedFlowQuery(_ context.Context, query preparedFlowQuery) (any, error) {
	e.directCalls++
	e.prepared = query
	return e.value, e.err
}

func TestFlowQueryUsesPreparedExecutorWithoutRevalidatingParameters(t *testing.T) {
	query := "FROM runs WHERE partition_key = @partition AND priority = @priority RETURN RECORDS"
	exec := &flowQueryFastPathProbe{value: flowQueryPageResponse([]any{}, false, nil)}
	client := NewClientWithExecutor(exec)

	if _, err := client.FlowQuery(context.Background(), query, map[string]any{
		"priority":  float32(2.5),
		"partition": "tenant-a",
		"attempt":   uint8(3),
	}); err != nil {
		t.Fatal(err)
	}

	if exec.directCalls != 1 || exec.genericCalls != 0 {
		t.Fatalf("direct calls = %d, generic calls = %d", exec.directCalls, exec.genericCalls)
	}
	if exec.prepared.query != query {
		t.Fatalf("prepared query = %q", exec.prepared.query)
	}
	want := []flowQueryParameter{
		{name: "attempt", value: int64(3)},
		{name: "partition", value: "tenant-a"},
		{name: "priority", value: float64(2.5)},
	}
	if !reflect.DeepEqual(exec.prepared.parameters, want) {
		t.Fatalf("prepared parameters = %#v, want %#v", exec.prepared.parameters, want)
	}
}

func TestPreparedFlowQueryNativeEncodingMatchesGenericCanonicalPayload(t *testing.T) {
	query := "FROM runs WHERE partition_key = @partition AND attempts >= @attempts RETURN RECORDS"
	for _, params := range []map[string]any{
		nil,
		{"partition": "tenant-a", "attempts": uint16(2), "binary": []byte{0, 1, 2}},
	} {
		prepared, err := prepareFlowQuery(query, params)
		if err != nil {
			t.Fatal(err)
		}
		fastBody, err := encodeNativeValue(newNativeFlowQueryCommand(prepared).payload)
		if err != nil {
			t.Fatal(err)
		}
		payload := newNativeFlowQueryCommand(prepared).payload.(nativeFlowQueryPayload)
		hintedSize, err := payload.nativeEncodedSize(nativeMaxFrameBytes)
		if err != nil {
			t.Fatal(err)
		}
		if hintedSize != len(fastBody) {
			t.Fatalf("encoded size hint = %d, actual body = %d", hintedSize, len(fastBody))
		}
		if _, err := encodeNativeValueWithLimit(payload, hintedSize-1); err == nil {
			t.Fatal("prepared query encoder exceeded its exact size limit")
		}
		genericCommand, err := buildNativeCommand(prepared.commandArgs())
		if err != nil {
			t.Fatal(err)
		}
		genericBody, err := encodeNativeValue(genericCommand.payload)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(fastBody, genericBody) {
			t.Fatalf("prepared body differs from generic canonical body:\nfast:    %x\ngeneric: %x", fastBody, genericBody)
		}
	}
}

func TestPreparedFlowQueryCommandArgsRemainStableForFallbackExecutors(t *testing.T) {
	prepared, err := prepareFlowQuery(
		"FROM runs WHERE type = @type AND partition_key = @partition RETURN RECORDS",
		map[string]any{"type": "invoice", "partition": "tenant-a"},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []any{
		"FLOW.QUERY", "FQL1",
		"FROM runs WHERE type = @type AND partition_key = @partition RETURN RECORDS",
		"partition", "tenant-a", "type", "invoice",
	}
	if got := prepared.commandArgs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("fallback args = %#v, want %#v", got, want)
	}
}

func TestPreparedNativeFlowQueryPropagatesContextDeadlineToServer(t *testing.T) {
	prepared, err := prepareFlowQuery("FROM runs WHERE run_id = 'run-1' RETURN RECORD", nil)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Unix(2_000_000_000, 123_456)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	command := newNativeFlowQueryCommandForContext(ctx, prepared)
	payload := command.payload.(nativeFlowQueryPayload)
	wantDeadlineMS := deadline.UnixMilli() + 1
	if payload.deadlineMS != wantDeadlineMS {
		t.Fatalf("deadline_ms = %d, want %d", payload.deadlineMS, wantDeadlineMS)
	}
	fastBody, err := encodeNativeValue(payload)
	if err != nil {
		t.Fatal(err)
	}
	wantBody, err := encodeNativeValue(map[string]any{
		"deadline_ms": wantDeadlineMS,
		"query":       prepared.query,
		"version":     flowQueryLanguageVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fastBody, wantBody) {
		t.Fatalf("deadline payload differs from canonical map:\nfast: %x\nwant: %x", fastBody, wantBody)
	}
}

func TestFlowQueryDoesNotMutateCustomExecutorResponseMaps(t *testing.T) {
	record := map[string]any{"id": []byte("run-1"), "type": []byte("invoice")}
	response := flowQueryPageResponse([]any{record}, false, nil)
	client := NewClientWithExecutor(&fakeExecutor{value: response})

	result, err := client.FlowQuery(context.Background(), "FROM runs WHERE run_id = 'run-1' RETURN RECORD", nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Records[0]["id"] != "run-1" {
		t.Fatalf("decoded id = %#v", result.Records[0]["id"])
	}
	if !reflect.DeepEqual(record["id"], []byte("run-1")) || !reflect.DeepEqual(record["type"], []byte("invoice")) {
		t.Fatalf("custom executor response was mutated: %#v", record)
	}
}

func TestOwnedNativeFlowQueryResponseAvoidsRedundantMapCopy(t *testing.T) {
	record := map[string]any{"id": []byte("run-1"), "type": []byte("invoice")}
	response := flowQueryPageResponse([]any{record}, false, nil)

	result, err := decodeFlowQueryResult(ownedNativeFlowQueryResponse(response))
	if err != nil {
		t.Fatal(err)
	}
	if result.Records[0]["id"] != "run-1" || record["id"] != "run-1" {
		t.Fatalf("owned record was not normalized in place: result=%#v source=%#v", result.Records[0], record)
	}
	response["future_extension"] = "preserved"
	if result.Raw["future_extension"] != "preserved" {
		t.Fatal("owned native response was copied instead of transferred")
	}
}
