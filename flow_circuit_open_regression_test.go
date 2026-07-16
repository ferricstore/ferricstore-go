package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestCircuitOpenWithOptionsExposesCompleteServerRule(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{"scope": "payments", "status": "open"}}
	client := NewClientWithExecutor(exec)
	result, err := client.CircuitOpenWithOptions(context.Background(), "payments", CircuitOpenOptions{
		OpenMS: Int64(10_000), FailureThreshold: Int64(80), WindowMS: Int64(60_000),
		MinCalls: Int64(12), FailureRatePct: Int64(25), LatencyThresholdMS: Int64(500),
		ErrorClasses: []string{"timeout", "io", "timeout"}, HalfOpenMaxProbes: Int64(3),
		HalfOpenSuccessThreshold: Int64(2), NowMS: Int64(100), DeadlineMS: Int64(200),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Scope != "payments" || result.Status != "open" {
		t.Fatalf("result = %#v", result)
	}
	want := []any{
		"FLOW.CIRCUIT.OPEN", "payments", "OPEN_MS", int64(10_000), "FAILURE_THRESHOLD", int64(80),
		"WINDOW_MS", int64(60_000), "MIN_CALLS", int64(12), "FAILURE_RATE_PCT", int64(25),
		"LATENCY_THRESHOLD_MS", int64(500), "ERROR_CLASSES", []string{"timeout", "io"},
		"HALF_OPEN_MAX_PROBES", int64(3), "HALF_OPEN_SUCCESS_THRESHOLD", int64(2),
		"NOW", int64(100), "DEADLINE_MS", int64(200),
	}
	if !reflect.DeepEqual(exec.calls[0], want) {
		t.Fatalf("command = %#v, want %#v", exec.calls[0], want)
	}
}

func TestCircuitOpenWithOptionsUsesDirectNativeOpcode(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.CIRCUIT.OPEN", "payments", "WINDOW_MS", int64(60_000), "MIN_CALLS", int64(12),
		"FAILURE_RATE_PCT", int64(25), "LATENCY_THRESHOLD_MS", int64(500),
		"ERROR_CLASSES", []string{"timeout"}, "HALF_OPEN_MAX_PROBES", int64(3),
		"HALF_OPEN_SUCCESS_THRESHOLD", int64(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpFlowCircuitOpen {
		t.Fatalf("opcode = %#x, want %#x", command.opcode, nativeOpFlowCircuitOpen)
	}
	payload, err := nativeMap(command.payload)
	if err != nil {
		t.Fatal(err)
	}
	if payload["window_ms"] != int64(60_000) || payload["min_calls"] != int64(12) ||
		payload["failure_rate_pct"] != int64(25) || payload["latency_threshold_ms"] != int64(500) ||
		payload["half_open_max_probes"] != int64(3) || payload["half_open_success_threshold"] != int64(2) {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestCircuitOpenWithOptionsRejectsInvalidRulesBeforeIO(t *testing.T) {
	tests := []CircuitOpenOptions{
		{WindowMS: Int64(0)},
		{FailureRatePct: Int64(101)},
		{MinCalls: Int64(65)},
		{FailureThreshold: Int64(65), FailureRatePct: Int64(25)},
		{ErrorClasses: []string{"timeout", " "}},
		{HalfOpenMaxProbes: Int64(0)},
		{DeadlineMS: Int64(-1)},
	}
	for _, opt := range tests {
		exec := &fakeExecutor{}
		if _, err := NewClientWithExecutor(exec).CircuitOpenWithOptions(context.Background(), "payments", opt); err == nil {
			t.Fatalf("invalid options succeeded: %#v", opt)
		}
		if len(exec.calls) != 0 {
			t.Fatalf("invalid options reached transport: %#v", exec.calls)
		}
	}
}
