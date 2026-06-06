package ferricstore

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/redis/go-redis/v9"
)

type fakeExecutor struct {
	calls  [][]any
	value  any
	values []any
	err    error
}

func (f *fakeExecutor) Do(ctx context.Context, args ...any) *redis.Cmd {
	f.calls = append(f.calls, append([]any(nil), args...))
	cmd := redis.NewCmd(ctx, args...)
	if f.err != nil {
		cmd.SetErr(f.err)
		return cmd
	}
	if len(f.values) > 0 {
		index := len(f.calls) - 1
		if index >= len(f.values) {
			index = len(f.values) - 1
		}
		cmd.SetVal(f.values[index])
	} else {
		cmd.SetVal(f.value)
	}
	return cmd
}

func TestCreateBuildsCommandDefaults(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)

	_, err := client.Create(context.Background(), CreateOptions{
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

	_, err := client.CreateMany(context.Background(), CreateManyOptions{
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

	_, err := client.CreateMany(context.Background(), CreateManyOptions{
		Type:  "order",
		NowMS: 100,
		Items: []CreateItem{
			{ID: "f1", PartitionKey: "p1"},
			{ID: "f2"},
		},
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
	if record.LeaseToken != "lease" || record.FencingToken != 7 || string(asBytes(record.Payload)) != "payload" {
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

	_, err := client.CompleteMany(context.Background(), CompleteManyOptions{
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

	_, err := client.Transition(context.Background(), TransitionOptions{
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

func TestRewindReturnRecordLoadsRecordWithoutReturnOption(t *testing.T) {
	exec := &fakeExecutor{values: []any{
		[]byte("OK"),
		map[string]any{
			"id":            "flow-1",
			"type":          "order",
			"state":         "queued",
			"partition_key": "tenant:1",
			"fencing_token": int64(9),
			"version":       int64(2),
		},
	}}
	client := NewClientWithExecutor(exec)

	record, err := client.Rewind(context.Background(), RewindOptions{
		ID:           "flow-1",
		ToEvent:      "event-1",
		PartitionKey: "tenant:1",
		ExpectState:  "completed",
		RunAtMS:      120,
		ReasonRef:    "reason",
		NowMS:        100,
		ReturnRecord: true,
	})

	if err != nil {
		t.Fatal(err)
	}
	if len(exec.calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(exec.calls))
	}
	wantRewind := []any{
		"FLOW.REWIND", "flow-1", "TO_EVENT", "event-1", "NOW", int64(100),
		"PARTITION", "tenant:1", "EXPECT_STATE", "completed", "RUN_AT", int64(120),
		"REASON_REF", "reason",
	}
	if !reflect.DeepEqual(exec.calls[0], wantRewind) {
		t.Fatalf("unexpected rewind call\n got: %#v\nwant: %#v", exec.calls[0], wantRewind)
	}
	wantGet := []any{"FLOW.GET", "flow-1", "PARTITION", "tenant:1"}
	if !reflect.DeepEqual(exec.calls[1], wantGet) {
		t.Fatalf("unexpected get call\n got: %#v\nwant: %#v", exec.calls[1], wantGet)
	}
	if record == nil || record.ID != "flow-1" || record.State != "queued" {
		t.Fatalf("unexpected record %#v", record)
	}
}

func TestRecordsFromRESPRejectsMalformedInput(t *testing.T) {
	if _, err := recordsFromRESP("OK", RawCodec{}); err == nil {
		t.Fatal("expected non-array error")
	}
	if _, err := recordsFromRESP([]any{[]any{"id"}}, RawCodec{}); err == nil {
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
