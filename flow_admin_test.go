package ferricstore

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestScheduleCreateBuildsCommand(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{"id": "sched-1", "status": "active", "kind": "interval"}}
	client := NewClientWithExecutor(exec)
	overwrite := true
	every := int64(1000)

	result, err := client.ScheduleCreate(context.Background(), "sched-1", ScheduleOptions{
		Kind:      "interval",
		EveryMS:   &every,
		Target:    map[string]any{"type": "email"},
		Overwrite: &overwrite,
	})

	if err != nil {
		t.Fatal(err)
	}
	if result.ID != "sched-1" || result.Status != "active" {
		t.Fatalf("unexpected result: %#v", result)
	}
	want := []any{"FLOW.SCHEDULE.CREATE", "sched-1", "KIND", "interval", "EVERY_MS", int64(1000), "TARGET", map[string]any{"type": "email"}, "OVERWRITE", "true"}
	if !reflect.DeepEqual(exec.calls[0], want) {
		t.Fatalf("unexpected call\n got: %#v\nwant: %#v", exec.calls[0], want)
	}
}

func TestCountByStateBuildsStatsCommand(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{"count": int64(42)}}
	client := NewClientWithExecutor(exec)
	consistent := false

	count, err := client.CountByState(context.Background(), "gitea.queue.default", "queued", ReadOptions{
		PartitionKey:         "queue-partition",
		Count:                Int(999),
		ConsistentProjection: &consistent,
	})

	if err != nil {
		t.Fatal(err)
	}
	if count != 42 {
		t.Fatalf("unexpected count: %d", count)
	}
	want := []any{"FLOW.STATS", "gitea.queue.default", "PARTITION", "queue-partition", "STATE", "queued", "CONSISTENT_PROJECTION", "false"}
	if !reflect.DeepEqual(exec.calls[0], want) {
		t.Fatalf("unexpected call\n got: %#v\nwant: %#v", exec.calls[0], want)
	}
}

func TestExistsBuildsStatsCommand(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{"count": int64(1)}}
	client := NewClientWithExecutor(exec)

	exists, err := client.Exists(context.Background(), "gitea.queue.default", ReadOptions{
		PartitionKey: "queue-partition",
		Count:        Int(500),
		State:        "queued",
		Attributes:   map[string]any{"gitea_payload_hash": "hash-1"},
	})

	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("expected matching workflow to exist")
	}
	want := []any{
		"FLOW.STATS", "gitea.queue.default",
		"PARTITION", "queue-partition",
		"STATE", "queued",
		"ATTRIBUTE", "gitea_payload_hash", "hash-1",
	}
	if !reflect.DeepEqual(exec.calls[0], want) {
		t.Fatalf("unexpected call\n got: %#v\nwant: %#v", exec.calls[0], want)
	}
}

func TestCountByStateRejectsMalformedStatsCount(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{"type": "gitea.queue.default"}}
	client := NewClientWithExecutor(exec)

	_, err := client.CountByState(context.Background(), "gitea.queue.default", "queued", ReadOptions{})

	if err == nil {
		t.Fatal("expected malformed stats count to fail")
	}
}

func TestExistsRejectsMalformedStatsCount(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{"count": "not-a-number"}}
	client := NewClientWithExecutor(exec)

	_, err := client.Exists(context.Background(), "gitea.queue.default", ReadOptions{State: "queued"})

	if err == nil {
		t.Fatal("expected malformed stats count to fail")
	}
}

func TestSetPolicyBuildsIndexedStateMetaCommand(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{
		"type":               "order",
		"indexed_attributes": []any{"tenant"},
		"indexed_state_meta": "version",
		"retry": map[string]any{
			"max_retries": int64(2),
		},
		"states": map[string]any{
			"queued": map[string]any{"retry": map[string]any{"max_retries": int64(5)}},
		},
	}}
	client := NewClientWithExecutor(exec)

	_, err := client.SetPolicy(context.Background(), "order", PolicyOptions{
		IndexedAttributes: []string{"tenant"},
		IndexedStateMeta:  "version",
		Retry:             &RetryPolicy{MaxRetries: 2},
		States: map[string]RetryPolicy{
			"queued": {MaxRetries: 5},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	if !containsSubsequence(exec.calls[0], []any{"FLOW.POLICY.SET", "order", "INDEXED_ATTRIBUTES", []string{"tenant"}, "INDEXED_STATE_META", "version", "MAX_RETRIES", 2}) {
		t.Fatalf("missing indexed policy options in %#v", exec.calls[0])
	}
	if !containsSubsequence(exec.calls[0], []any{"STATE", "queued", "MAX_RETRIES", 5}) {
		t.Fatalf("missing state policy in %#v", exec.calls[0])
	}
}

func TestSetPolicyCanApplyZeroAndClearValues(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{
		"type":               "order",
		"indexed_attributes": []any{},
		"indexed_state_meta": "",
		"retry": map[string]any{
			"max_retries": int64(0),
			"backoff": map[string]any{
				"base_ms":    int64(0),
				"max_ms":     int64(0),
				"jitter_pct": int64(0),
			},
		},
	}}
	client := NewClientWithExecutor(exec)

	_, err := client.SetPolicy(context.Background(), "order", PolicyOptions{
		IndexedAttributes:   []string{},
		IndexedStateMeta:    "",
		IndexedStateMetaSet: true,
		Retry: &RetryPolicy{
			MaxRetries:    0,
			MaxRetriesSet: true,
			BaseMS:        0,
			BaseMSSet:     true,
			MaxMS:         0,
			MaxMSSet:      true,
			JitterPct:     0,
			JitterPctSet:  true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	call := exec.calls[0]
	for _, want := range [][]any{
		{"INDEXED_ATTRIBUTES", []string{}},
		{"INDEXED_STATE_META", ""},
		{"MAX_RETRIES", 0},
		{"BASE_MS", int64(0)},
		{"MAX_MS", int64(0)},
		{"JITTER_PCT", 0},
	} {
		if !containsSubsequence(call, want) {
			t.Fatalf("policy command omitted explicit zero/clear option %v: %#v", want, call)
		}
	}
}

func TestSetPolicyRejectsResponseThatDropsRequestedFields(t *testing.T) {
	tests := []struct {
		name     string
		response map[string]any
		options  PolicyOptions
	}{
		{
			name:     "indexed attributes",
			response: map[string]any{"type": "order"},
			options:  PolicyOptions{IndexedAttributes: []string{"tenant"}},
		},
		{
			name:     "indexed state meta",
			response: map[string]any{"type": "order", "indexed_state_meta": "wrong"},
			options:  PolicyOptions{IndexedStateMeta: "version"},
		},
		{
			name:     "type retry",
			response: map[string]any{"type": "order", "retry": map[string]any{"max_retries": int64(1)}},
			options:  PolicyOptions{Retry: &RetryPolicy{MaxRetries: 2}},
		},
		{
			name: "state retry",
			response: map[string]any{
				"type": "order",
				"states": map[string]any{
					"queued": map[string]any{"retry": map[string]any{"max_retries": int64(4)}},
				},
			},
			options: PolicyOptions{States: map[string]RetryPolicy{"queued": {MaxRetries: 5}}},
		},
		{
			name: "full state retry",
			response: map[string]any{
				"type": "order",
				"states": map[string]any{
					"queued": map[string]any{"mode": "fifo", "retry": map[string]any{"max_retries": int64(1)}},
				},
			},
			options: PolicyOptions{StatePolicies: map[string]FlowStatePolicy{
				"queued": {Mode: FlowStateModeFIFO, Retry: &RetryPolicy{MaxRetries: 2}},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewClientWithExecutor(&fakeExecutor{value: tt.response})
			if _, err := client.SetPolicy(context.Background(), "order", tt.options); err == nil {
				t.Fatalf("accepted policy response that dropped %s", tt.name)
			}
		})
	}
}

func TestTypedAdminResultsRejectMalformedNumericAndNestedFields(t *testing.T) {
	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "schedule numeric",
			run: func() error {
				_, err := scheduleResult(map[string]any{"id": "schedule-1", "fires": "not-a-number"}, nil)
				return err
			},
		},
		{
			name: "budget numeric",
			run: func() error {
				_, err := budgetResult(map[string]any{"scope": "tenant", "limit": uint64(^uint64(0))}, nil)
				return err
			},
		},
		{
			name: "limit numeric",
			run: func() error {
				_, err := limitResult(map[string]any{"scope": "tenant", "free": "invalid"}, nil)
				return err
			},
		},
		{
			name: "governance nested collection",
			run: func() error {
				client := NewClientWithExecutor(&fakeExecutor{value: map[string]any{"approvals": map[string]any{"not": "an array"}}})
				_, err := client.GovernanceOverview(context.Background(), ApprovalListOptions{})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.run(); err == nil {
				t.Fatal("expected malformed typed response to fail")
			}
		})
	}
}

func TestSetPolicyBuildsStateModeCommand(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{
		"type": "order",
		"states": map[string]any{
			"queued": map[string]any{"mode": "fifo"},
			"ready":  map[string]any{"mode": "parallel", "retry": map[string]any{"max_retries": int64(1)}},
		},
	}}
	client := NewClientWithExecutor(exec)

	_, err := client.SetPolicy(context.Background(), "order", PolicyOptions{
		StatePolicies: map[string]FlowStatePolicy{
			"queued": {Mode: FlowStateModeFIFO},
			"ready":  {Mode: FlowStateModeParallel, Retry: &RetryPolicy{MaxRetries: 1}},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	if !containsSubsequence(exec.calls[0], []any{"STATE", "queued", "MODE", "FIFO"}) {
		t.Fatalf("missing fifo state mode in %#v", exec.calls[0])
	}
	if !containsSubsequence(exec.calls[0], []any{"STATE", "ready", "MODE", "PARALLEL", "MAX_RETRIES", 1}) {
		t.Fatalf("missing parallel state mode with retry policy in %#v", exec.calls[0])
	}
}

func TestInstallPolicyUsesFullPolicyOptions(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{
		"type":               "order",
		"indexed_state_meta": "version",
		"states": map[string]any{
			"queued": map[string]any{"mode": "fifo"},
		},
	}}
	client := NewClientWithExecutor(exec)

	_, err := client.InstallPolicy(context.Background(), "order", PolicyOptions{
		IndexedStateMeta: "version",
		StatePolicies: map[string]FlowStatePolicy{
			"queued": {Mode: FlowStateModeFIFO},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	if !containsSubsequence(exec.calls[0], []any{"FLOW.POLICY.SET", "order", "INDEXED_STATE_META", "version", "STATE", "queued", "MODE", "FIFO"}) {
		t.Fatalf("missing full install policy options in %#v", exec.calls[0])
	}
}

func TestSetPolicyRejectsInvalidStateMode(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{"type": "order"}}
	client := NewClientWithExecutor(exec)

	_, err := client.SetPolicy(context.Background(), "order", PolicyOptions{
		StatePolicies: map[string]FlowStatePolicy{
			"queued": {Mode: FlowStateMode("priority")},
		},
	})

	if err == nil {
		t.Fatal("expected invalid state mode error")
	}
	if len(exec.calls) != 0 {
		t.Fatalf("expected invalid policy to be rejected before command execution, got %#v", exec.calls)
	}
}

func TestSetPolicyRejectsServerThatSilentlyDropsStatePolicy(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{"type": "order", "states": nil}}
	client := NewClientWithExecutor(exec)

	_, err := client.SetPolicy(context.Background(), "order", PolicyOptions{
		StatePolicies: map[string]FlowStatePolicy{
			"queued": {Mode: FlowStateModeFIFO},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "omitted state policy") {
		t.Fatalf("expected silent state-policy loss to fail, got %v", err)
	}
}

func TestPolicyGetNormalizesProtocolTextRecursively(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{
		"type":  []byte("order"),
		"state": []byte("queued"),
		"mode":  []byte("fifo"),
		"retry": map[interface{}]interface{}{
			"exhausted_to": []byte("failed"),
		},
	}}
	client := NewClientWithExecutor(exec)

	policy, err := client.PolicyGet(context.Background(), "order", "queued")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"type":  "order",
		"state": "queued",
		"mode":  "fifo",
		"retry": map[string]any{"exhausted_to": "failed"},
	}
	if !reflect.DeepEqual(policy, want) {
		t.Fatalf("unexpected normalized policy\n got: %#v\nwant: %#v", policy, want)
	}
}

func TestGovernanceHelpersBuildCommands(t *testing.T) {
	exec := &fakeExecutor{values: []any{
		map[string]any{"id": "approval-1", "status": "pending", "scope": "tenant:1"},
		map[string]any{"scope": "openai", "status": "reserved", "reservation_id": "res-1"},
		map[string]any{"scope": "api", "status": "open", "retry_after_ms": int64(500)},
	}}
	client := NewClientWithExecutor(exec)

	approval, err := client.ApprovalRequest(context.Background(), "approval-1", ApprovalRequestOptions{FlowID: "flow-1", Scope: "tenant:1", RequestedBy: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	budget, err := client.BudgetReserve(context.Background(), "openai", 10, Int64(100), Int64(60_000), "res-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	circuit, err := client.CircuitOpen(context.Background(), "api", Int64(30_000), Int64(5), nil)
	if err != nil {
		t.Fatal(err)
	}

	if approval.Status != "pending" || budget.ReservationID != "res-1" || circuit.RetryAfterMS != 500 {
		t.Fatalf("unexpected results: %#v %#v %#v", approval, budget, circuit)
	}
	if exec.calls[0][0] != "FLOW.APPROVAL.REQUEST" || exec.calls[1][0] != "FLOW.BUDGET.RESERVE" || exec.calls[2][0] != "FLOW.CIRCUIT.OPEN" {
		t.Fatalf("unexpected calls: %#v", exec.calls)
	}
}

func TestLimitHelpersReturnTypedResults(t *testing.T) {
	exec := &fakeExecutor{values: []any{
		map[string]any{
			"owner": map[string]any{
				"scope": "tenant:acme",
				"limit": int64(10),
				"free":  int64(4),
				"epoch": int64(1),
				"leases": map[string]any{
					"1": map[string]any{
						"shard_id":      int64(1),
						"epoch":         int64(1),
						"expires_at_ms": int64(10_000),
						"available":     int64(6),
						"in_use":        int64(0),
					},
				},
			},
			"lease": map[string]any{
				"shard_id":      int64(1),
				"epoch":         int64(1),
				"expires_at_ms": int64(10_000),
				"available":     int64(6),
				"in_use":        int64(0),
			},
		},
		map[string]any{
			"scope": "tenant:acme",
			"limit": int64(10),
			"free":  int64(10),
			"epoch": int64(1),
		},
		[]any{
			map[string]any{"scope": "tenant:acme", "limit": int64(10), "free": int64(10)},
		},
	}}
	client := NewClientWithExecutor(exec)

	lease, err := client.LimitLease(context.Background(), "tenant:acme", 1, 6, 5_000, Int64(10), Int64(5_000))
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.LimitGet(context.Background(), "tenant:acme", Int64(6_000))
	if err != nil {
		t.Fatal(err)
	}
	list, err := client.LimitList(context.Background(), "tenant:acme", "", Int(10), Int64(7_000))
	if err != nil {
		t.Fatal(err)
	}

	if lease.Scope != "tenant:acme" || lease.Limit != 10 || lease.Free != 4 || lease.Lease == nil {
		t.Fatalf("unexpected lease result: %#v", lease)
	}
	if shardLease := lease.Leases[1]; shardLease.Available != 6 || shardLease.ExpiresAtMS != 10_000 {
		t.Fatalf("unexpected parsed shard lease: %#v", shardLease)
	}
	if got == nil || got.Scope != "tenant:acme" || got.Free != 10 {
		t.Fatalf("unexpected get result: %#v", got)
	}
	if len(list) != 1 || list[0].Scope != "tenant:acme" {
		t.Fatalf("unexpected list result: %#v", list)
	}

	wantLeaseCall := []any{"FLOW.LIMIT.LEASE", "tenant:acme", "SHARD_ID", int64(1), "AMOUNT", int64(6), "TTL_MS", int64(5_000), "LIMIT", int64(10), "NOW", int64(5_000)}
	if !reflect.DeepEqual(exec.calls[0], wantLeaseCall) {
		t.Fatalf("unexpected lease call\n got: %#v\nwant: %#v", exec.calls[0], wantLeaseCall)
	}
}
