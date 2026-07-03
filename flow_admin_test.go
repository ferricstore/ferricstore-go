package ferricstore

import (
	"context"
	"reflect"
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
