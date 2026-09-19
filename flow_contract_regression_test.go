package ferricstore

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

func TestClaimDueFullRecordsPreservePayloadAndSelectedValues(t *testing.T) {
	exec := &fakeExecutor{value: []any{}}
	client := NewClientWithExecutor(exec)
	payload := true

	if _, err := client.ClaimDue(context.Background(), ClaimDueOptions{
		Type:            "order",
		State:           "queued",
		Worker:          "worker",
		PartitionKey:    "tenant:1",
		Limit:           1,
		NowMS:           100,
		Payload:         &payload,
		PayloadMaxBytes: Int64(4096),
		Values:          []string{"result"},
	}); err != nil {
		t.Fatal(err)
	}
	if len(exec.calls) != 1 {
		t.Fatalf("ClaimDue issued %d calls, want 1", len(exec.calls))
	}

	call := exec.calls[0]
	for _, want := range [][]any{
		{"RETURN", "RECORDS"},
		{"PAYLOAD", "MAXBYTES", int64(4096)},
		{"VALUE", "result"},
	} {
		if !containsSubsequence(call, want) {
			t.Fatalf("FLOW.CLAIM_DUE omitted %v: %#v", want, call)
		}
	}
}

func TestReclaimFullRecordsPreservePayloadAndSelectedValues(t *testing.T) {
	exec := &fakeExecutor{value: []any{}}
	client := NewClientWithExecutor(exec)
	payload := true

	if _, err := client.Reclaim(context.Background(), ReclaimOptions{
		Type:            "order",
		Worker:          "worker",
		PartitionKey:    "tenant:1",
		Limit:           1,
		NowMS:           100,
		Payload:         &payload,
		PayloadMaxBytes: Int64(4096),
		Values:          []string{"result"},
	}); err != nil {
		t.Fatal(err)
	}
	if len(exec.calls) != 1 {
		t.Fatalf("Reclaim issued %d calls, want 1", len(exec.calls))
	}

	call := exec.calls[0]
	for _, want := range [][]any{
		{"RETURN", "RECORDS"},
		{"PAYLOAD", "MAXBYTES", int64(4096)},
		{"VALUE", "result"},
	} {
		if !containsSubsequence(call, want) {
			t.Fatalf("FLOW.RECLAIM omitted %v: %#v", want, call)
		}
	}
}

func TestRewindReasonIsEncodedOnTheWire(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)
	reason := []byte{0, 255, 17}

	if _, err := client.Rewind(context.Background(), RewindOptions{
		ID:      "flow-1",
		ToEvent: "1-0",
		Reason:  reason,
		NowMS:   100,
	}); err != nil {
		t.Fatal(err)
	}

	want := []any{"FLOW.REWIND", "flow-1", "TO_EVENT", "1-0", "NOW", int64(100), "REASON", reason}
	if !reflect.DeepEqual(exec.calls[0], want) {
		t.Fatalf("rewind call = %#v, want %#v", exec.calls[0], want)
	}
}

func TestRewindReasonUsesConfiguredCodec(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec, WithCodec(JSONCodec{}))
	reason := map[string]any{"attempt": 2, "source": "operator"}

	if _, err := client.Rewind(context.Background(), RewindOptions{
		ID:      "flow-1",
		ToEvent: "1-0",
		Reason:  reason,
		NowMS:   100,
	}); err != nil {
		t.Fatal(err)
	}
	if len(exec.calls) != 1 {
		t.Fatalf("Rewind issued %d calls, want 1", len(exec.calls))
	}

	encoded, err := json.Marshal(reason)
	if err != nil {
		t.Fatal(err)
	}
	reasonIndex := indexOf(exec.calls[0], "REASON")
	if reasonIndex < 0 || reasonIndex+1 >= len(exec.calls[0]) || !bytes.Equal(asBytes(exec.calls[0][reasonIndex+1]), encoded) {
		t.Fatalf("rewind reason = %#v, want codec bytes %q", exec.calls[0], encoded)
	}
}

func TestPolicyRetryNestedTypeAndStateWireShape(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.POLICY.SET", "order",
		"MAX_RETRIES", int64(3), "BACKOFF", "exponential", "BASE_MS", int64(10),
		"MAX_MS", int64(1000), "JITTER_PCT", int64(20), "EXHAUSTED_TO", "failed",
		"STATE", "queued",
		"MAX_RETRIES", int64(5), "BACKOFF", "linear", "BASE_MS", int64(20),
		"MAX_MS", int64(2000), "JITTER_PCT", int64(30), "EXHAUSTED_TO", "cancelled",
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, ok := command.payload.(map[string]any)
	if !ok {
		t.Fatalf("native policy payload = %T, want map", command.payload)
	}
	assertRetryPolicyWireShape(t, payload["retry"], RetryPolicy{
		MaxRetries: 3, Backoff: "exponential", BaseMS: 10, MaxMS: 1000, JitterPct: 20, ExhaustedTo: "failed",
	})
	states, ok := payload["states"].(map[string]any)
	if !ok {
		t.Fatalf("native policy states = %T, want map", payload["states"])
	}
	state, ok := states["queued"].(map[string]any)
	if !ok {
		t.Fatalf("native queued policy = %T, want map", states["queued"])
	}
	assertRetryPolicyWireShape(t, state["retry"], RetryPolicy{
		MaxRetries: 5, Backoff: "linear", BaseMS: 20, MaxMS: 2000, JitterPct: 30, ExhaustedTo: "cancelled",
	})
}

func assertRetryPolicyWireShape(t *testing.T, value any, want RetryPolicy) {
	t.Helper()
	retry, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("retry wire value = %T, want map", value)
	}
	if asInt64(retry["max_retries"]) != int64(want.MaxRetries) || asString(retry["exhausted_to"]) != want.ExhaustedTo {
		t.Fatalf("retry wire fields = %#v", retry)
	}
	backoff, ok := retry["backoff"].(map[string]any)
	if !ok {
		t.Fatalf("backoff wire value = %T, want map", retry["backoff"])
	}
	if asString(backoff["kind"]) != want.Backoff ||
		asInt64(backoff["base_ms"]) != want.BaseMS ||
		asInt64(backoff["max_ms"]) != want.MaxMS ||
		asInt64(backoff["jitter_pct"]) != int64(want.JitterPct) {
		t.Fatalf("backoff wire fields = %#v", backoff)
	}
}
