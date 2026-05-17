package ferricstore

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/redis/go-redis/v9"
)

type fakeExecutor struct {
	calls [][]any
	value any
	err   error
}

func (f *fakeExecutor) Do(ctx context.Context, args ...any) *redis.Cmd {
	f.calls = append(f.calls, append([]any(nil), args...))
	cmd := redis.NewCmd(ctx, args...)
	if f.err != nil {
		cmd.SetErr(f.err)
		return cmd
	}
	cmd.SetVal(f.value)
	return cmd
}

func TestCreateBuildsCommandDefaults(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)

	err := client.Create(context.Background(), CreateOptions{
		ID:           "flow-1",
		Type:         "order",
		State:        "created",
		PartitionKey: "tenant:1",
		Payload:      []byte("payload"),
		NowMS:        100,
	})

	if err != nil {
		t.Fatal(err)
	}
	want := []any{
		"FLOW.CREATE", "flow-1", "TYPE", "order", "STATE", "created", "NOW", int64(100),
		"PARTITION", "tenant:1", "PAYLOAD", []byte("payload"), "RUN_AT", int64(100),
	}
	assertCall(t, exec, want)
}

func TestCreateManyMixedBuildsItems(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)
	independent := true

	err := client.CreateMany(context.Background(), CreateManyOptions{
		Type:        "order",
		State:       "queued",
		NowMS:       100,
		Independent: &independent,
		Items: []CreateItem{
			{ID: "f1", PartitionKey: "p1", Payload: []byte("a")},
			{ID: "f2", PartitionKey: "p2", Payload: []byte("b")},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	want := []any{
		"FLOW.CREATE_MANY", "MIXED", "TYPE", "order", "STATE", "queued", "NOW", int64(100),
		"RUN_AT", int64(100), "INDEPENDENT", "true", "ITEMS",
		"f1", "p1", []byte("a"),
		"f2", "p2", []byte("b"),
	}
	assertCall(t, exec, want)
}

func TestCreateManyMixedRequiresPartitionKey(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)

	err := client.CreateMany(context.Background(), CreateManyOptions{
		Type:  "order",
		NowMS: 100,
		Items: []CreateItem{{ID: "f1"}},
	})

	if err == nil {
		t.Fatal("expected error")
	}
	if len(exec.calls) != 0 {
		t.Fatalf("expected no command, got %d", len(exec.calls))
	}
}

func TestClaimDueDecodesRESP3Maps(t *testing.T) {
	exec := &fakeExecutor{
		value: []any{
			map[interface{}]interface{}{
				"id":             []byte("flow-1"),
				"type":           []byte("order"),
				"state":          []byte("running"),
				"partition_key":  []byte("tenant:1"),
				"lease_token":    []byte("lease"),
				"fencing_token":  int64(7),
				"version":        int64(3),
				"payload":        []byte("payload"),
				"root_flow_id":   []byte("root"),
				"correlation_id": []byte("corr"),
			},
		},
	}
	client := NewClientWithExecutor(exec)

	records, err := client.ClaimDue(context.Background(), ClaimDueOptions{
		Type:         "order",
		State:        "queued",
		Worker:       "worker-1",
		PartitionKey: "tenant:1",
		Limit:        10,
		NowMS:        100,
	})

	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	record := records[0]
	if record.ID != "flow-1" || record.Type != "order" || record.State != "running" {
		t.Fatalf("unexpected record: %+v", record)
	}
	if record.LeaseToken != "lease" || record.FencingToken != 7 || string(record.Payload) != "payload" {
		t.Fatalf("unexpected lease/payload fields: %+v", record)
	}

	want := []any{
		"FLOW.CLAIM_DUE", "order", "STATE", "queued", "WORKER", "worker-1",
		"LEASE_MS", int64(30000), "LIMIT", 10, "NOW", int64(100), "PARTITION", "tenant:1",
	}
	assertCall(t, exec, want)
}

func TestClaimDuePropagatesExecutorError(t *testing.T) {
	exec := &fakeExecutor{err: errors.New("boom")}
	client := NewClientWithExecutor(exec)

	_, err := client.ClaimDue(context.Background(), ClaimDueOptions{Type: "order", Worker: "w"})

	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCompleteManyMixedBuildsItems(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)
	independent := true

	err := client.CompleteMany(context.Background(), CompleteManyOptions{
		Result:      []byte("ok"),
		NowMS:       100,
		Independent: &independent,
		Items: []ClaimedItem{
			{ID: "f1", PartitionKey: "p1", LeaseToken: "l1", FencingToken: 1},
			{ID: "f2", PartitionKey: "p2", LeaseToken: "l2", FencingToken: 2},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	want := []any{
		"FLOW.COMPLETE_MANY", "MIXED", "RESULT", []byte("ok"), "NOW", int64(100),
		"INDEPENDENT", "true", "ITEMS",
		"f1", "p1", "l1", int64(1),
		"f2", "p2", "l2", int64(2),
	}
	assertCall(t, exec, want)
}

func TestTransitionBuildsCommand(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)

	err := client.Transition(context.Background(), TransitionOptions{
		ID:           "flow-1",
		FromState:    "running",
		ToState:      "next",
		LeaseToken:   "lease",
		FencingToken: 9,
		PartitionKey: "tenant:1",
		Payload:      []byte("payload"),
		NowMS:        100,
	})

	if err != nil {
		t.Fatal(err)
	}
	want := []any{
		"FLOW.TRANSITION", "flow-1", "running", "next",
		"LEASE_TOKEN", "lease", "FENCING", int64(9), "NOW", int64(100),
		"PARTITION", "tenant:1", "PAYLOAD", []byte("payload"), "RUN_AT", int64(100),
	}
	assertCall(t, exec, want)
}

func TestRecordsFromRESPRejectsMalformedInput(t *testing.T) {
	if _, err := recordsFromRESP("OK"); err == nil {
		t.Fatal("expected non-array error")
	}
	if _, err := recordsFromRESP([]any{[]any{"id"}}); err == nil {
		t.Fatal("expected odd map array error")
	}
}

func assertCall(t *testing.T, exec *fakeExecutor, want []any) {
	t.Helper()
	if len(exec.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(exec.calls))
	}
	if !reflect.DeepEqual(exec.calls[0], want) {
		t.Fatalf("unexpected call\n got: %#v\nwant: %#v", exec.calls[0], want)
	}
}
