package ferricstore

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestLimitReleasePreservesReservationIDs(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{"scope": "tenant", "free": int64(2)}}
	ids := []string{"flr1:1:batch:1", "flr1:1:batch:2"}
	result, err := NewClientWithExecutor(exec).LimitRelease(context.Background(), "tenant", LimitReleaseOptions{
		ShardID: 2, ReservationIDs: ids, NowMS: Int64(100), DeadlineMS: Int64(200),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Scope != "tenant" || result.Free != 2 {
		t.Fatalf("result = %#v", result)
	}
	ids[0] = "mutated"
	want := []any{
		"FLOW.LIMIT.RELEASE", "tenant", "SHARD_ID", int64(2),
		"RESERVATION_IDS", []string{"flr1:1:batch:1", "flr1:1:batch:2"},
		"NOW", int64(100), "DEADLINE_MS", int64(200),
	}
	if !reflect.DeepEqual(exec.calls[0], want) {
		t.Fatalf("command = %#v, want %#v", exec.calls[0], want)
	}
}

func TestLimitReleaseRequiresReservationIDsBeforeIO(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{"scope": "tenant"}}
	_, err := NewClientWithExecutor(exec).LimitRelease(context.Background(), "tenant", LimitReleaseOptions{
		ShardID: 2,
	})
	if err == nil || !strings.Contains(err.Error(), "reservation_ids") {
		t.Fatalf("amount-only release error = %v", err)
	}
	if len(exec.calls) != 0 {
		t.Fatalf("amount-only release reached transport: %#v", exec.calls)
	}
}

func TestLimitReleaseReservationIDsUseDirectNativeOpcode(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.LIMIT.RELEASE", "tenant", "SHARD_ID", int64(2),
		"RESERVATION_IDS", []string{"flr1:1:batch:1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpFlowLimitRelease {
		t.Fatalf("opcode = %#x, want %#x", command.opcode, nativeOpFlowLimitRelease)
	}
	payload, err := nativeMap(command.payload)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(payload["reservation_ids"], []string{"flr1:1:batch:1"}) {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestLimitReleaseRejectsInvalidIdentityContracts(t *testing.T) {
	tooLong := strings.Repeat("x", 257)
	tests := []LimitReleaseOptions{
		{ShardID: 0},
		{ShardID: -1, ReservationIDs: []string{"id"}},
		{ShardID: 0, ReservationIDs: []string{}},
		{ShardID: 0, ReservationIDs: []string{"id", "id"}},
		{ShardID: 0, ReservationIDs: []string{tooLong}},
		{ShardID: 0, ReservationIDs: []string{"id"}, NowMS: Int64(-1)},
		{ShardID: 0, ReservationIDs: []string{"id"}, DeadlineMS: Int64(-1)},
	}
	for _, opt := range tests {
		exec := &fakeExecutor{}
		if _, err := NewClientWithExecutor(exec).LimitRelease(context.Background(), "tenant", opt); err == nil {
			t.Fatalf("invalid options succeeded: %#v", opt)
		}
		if len(exec.calls) != 0 {
			t.Fatalf("invalid options reached transport: %#v", exec.calls)
		}
	}
}
