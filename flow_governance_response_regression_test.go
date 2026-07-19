package ferricstore

import (
	"reflect"
	"strings"
	"testing"
)

func TestGovernanceTypedResultsDecodeCanonicalServerFields(t *testing.T) {
	approval, err := approvalResult(map[string]any{
		"id": "approval-1", "flow_id": "flow-1", "scope": "tenant", "status": "approved",
		"reason": "review", "requested_by": "alice", "decided_by": "operator",
		"decision_reason": "verified", "requested_at_ms": int64(10), "decided_at_ms": int64(20),
		"expires_at_ms": int64(30), "assignees": []any{"ops"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if approval.Approver != "operator" || approval.DecisionReason != "verified" ||
		approval.RequestedAtMS != 10 || approval.DecidedAtMS != 20 || approval.ExpiresAtMS != 30 {
		t.Fatalf("approval = %#v", approval)
	}

	effect, err := effectResult(map[string]any{
		"flow_id": "flow-1", "partition_key": "tenant", "type": "order", "state": "running",
		"effect_key": "email", "effect_type": "email.send", "status": "confirmed",
		"decision": "confirmed", "scope": "tenant", "policy_hash": "sha256:x", "policy_version": int64(7),
		"latency_ms": int64(4), "created_at_ms": int64(100), "updated_at_ms": int64(125),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if effect.FlowID != "flow-1" || effect.PartitionKey != "tenant" || effect.FlowType != "order" ||
		effect.State != "running" || effect.Decision != "confirmed" || effect.Scope != "tenant" ||
		effect.PolicyHash != "sha256:x" || effect.PolicyVersion != "7" || effect.LatencyMS != 4 ||
		effect.CreatedAtMS != 100 || effect.UpdatedAtMS != 125 || effect.ReservedAtMS != 100 ||
		effect.ConfirmedAtMS != 125 {
		t.Fatalf("effect = %#v", effect)
	}

	circuit, err := circuitResult(map[string]any{
		"scope": "payments", "status": "half_open", "failure_threshold": int64(5),
		"open_ms": int64(1_000), "opened_at_ms": int64(100), "failures": int64(3),
		"failure_count": int64(3), "window_ms": int64(10_000), "min_calls": int64(4),
		"failure_rate_pct": int64(50), "latency_threshold_ms": int64(250),
		"error_classes": []any{"timeout"}, "half_open_max_probes": int64(2),
		"half_open_success_threshold": int64(2), "half_open_in_flight": int64(1),
		"half_open_successes": int64(1), "half_open_started_at_ms": int64(1_500),
		"last_failure_ms": int64(1_400), "last_success_ms": int64(1_450), "updated_at_ms": int64(1_500),
		"events": []any{map[string]any{"outcome": "failure"}}, "event_count": int64(1),
		"retry_after_ms": int64(20),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if circuit.FailureThreshold != 5 || circuit.OpenMS != 1_000 || circuit.OpenedAtMS != 100 ||
		circuit.Failures != 3 || circuit.FailureCount != 3 || circuit.WindowMS != 10_000 ||
		circuit.MinCalls != 4 || circuit.FailureRatePct != 50 || circuit.LatencyThresholdMS != 250 ||
		!reflect.DeepEqual(circuit.ErrorClasses, []string{"timeout"}) || circuit.HalfOpenMaxProbes != 2 ||
		circuit.HalfOpenSuccessThreshold != 2 || circuit.HalfOpenInFlight != 1 ||
		circuit.HalfOpenSuccesses != 1 || circuit.HalfOpenStartedAtMS != 1_500 ||
		circuit.LastFailureMS != 1_400 || circuit.LastSuccessMS != 1_450 || circuit.UpdatedAtMS != 1_500 ||
		len(circuit.Events) != 1 || circuit.EventCount != 1 || circuit.RetryAfterMS != 20 {
		t.Fatalf("circuit = %#v", circuit)
	}
}

func TestGovernanceOverviewIncludesTypedCircuits(t *testing.T) {
	rawCircuit := map[string]any{
		"scope": "payments", "status": "open", "failure_threshold": int64(5),
	}
	overview, err := governanceOverviewFromMap(map[string]any{
		"circuits": []any{rawCircuit},
		"counts":   map[string]any{"circuits": int64(1), "open_circuits": int64(1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(overview.Circuits) != 1 || overview.Circuits[0].Scope != "payments" ||
		overview.Circuits[0].Status != "open" || overview.Circuits[0].FailureThreshold != 5 {
		t.Fatalf("overview = %#v", overview)
	}
}

func TestGovernanceTypedResultsRejectIntegrityViolations(t *testing.T) {
	tests := []struct {
		name string
		run  func() error
		want string
	}{
		{name: "approval timestamp", want: "requested_at_ms", run: func() error {
			_, err := approvalResult(map[string]any{"requested_at_ms": int64(-1)}, nil)
			return err
		}},
		{name: "approval exact integer overflow", want: "requested_at_ms", run: func() error {
			_, err := approvalResult(map[string]any{"requested_at_ms": int64(maxFlowExactIntegerV080 + 1)}, nil)
			return err
		}},
		{name: "approval negative policy version", want: "policy_version", run: func() error {
			_, err := approvalResult(map[string]any{"policy_version": int64(-1)}, nil)
			return err
		}},
		{name: "approval alias conflict", want: "conflicting", run: func() error {
			_, err := approvalResult(map[string]any{"approver": "a", "decided_by": "b"}, nil)
			return err
		}},
		{name: "effect timestamp", want: "created_at_ms", run: func() error {
			_, err := effectResult(map[string]any{"created_at_ms": int64(-1)}, nil)
			return err
		}},
		{name: "circuit failure count", want: "conflicting", run: func() error {
			_, err := circuitResult(map[string]any{"failures": int64(1), "failure_count": int64(2)}, nil)
			return err
		}},
		{name: "circuit event count", want: "event_count", run: func() error {
			_, err := circuitResult(map[string]any{"events": []any{}, "event_count": int64(1)}, nil)
			return err
		}},
		{name: "circuit exact integer overflow", want: "open_ms", run: func() error {
			_, err := circuitResult(map[string]any{"open_ms": int64(maxFlowExactIntegerV080 + 1)}, nil)
			return err
		}},
		{name: "overview circuit shape", want: "circuits", run: func() error {
			_, err := governanceOverviewFromMap(map[string]any{"circuits": map[string]any{}})
			return err
		}},
		{name: "overview approval count", want: "approvals count", run: func() error {
			_, err := governanceOverviewFromMap(map[string]any{
				"approvals": []any{map[string]any{"status": "pending"}},
				"counts":    map[string]any{"approvals": int64(2)},
			})
			return err
		}},
		{name: "overview pending count", want: "pending_approvals count", run: func() error {
			_, err := governanceOverviewFromMap(map[string]any{
				"approvals": []any{map[string]any{"status": "approved"}},
				"counts":    map[string]any{"pending_approvals": int64(1)},
			})
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want text %q", err, test.want)
			}
		})
	}
}
