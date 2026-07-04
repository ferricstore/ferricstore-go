package ferricstore

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type fakeExecutor struct {
	calls  [][]any
	value  any
	values []any
	err    error
}

func (f *fakeExecutor) Do(ctx context.Context, args ...any) (any, error) {
	f.calls = append(f.calls, append([]any(nil), args...))
	if f.err != nil {
		return nil, f.err
	}
	if len(f.values) > 0 {
		index := len(f.calls) - 1
		if index >= len(f.values) {
			index = len(f.values) - 1
		}
		return f.values[index], nil
	}
	return f.value, nil
}

func TestWithNativeOptionsAppliesToNewClient(t *testing.T) {
	client := NewClient("127.0.0.1:6388",
		WithNativeOptions(
			WithNativeReconnect(0),
			WithNativeHeartbeat(0, 0),
			WithNativeTimeout(2*time.Second),
		),
	)
	defer func() { _ = client.Close() }()

	exec, ok := client.exec.(*NativeExecutor)
	if !ok {
		t.Fatalf("expected native executor, got %T", client.exec)
	}
	if exec.opts.ReconnectMaxRetries != 0 {
		t.Fatalf("unexpected reconnect retries: %d", exec.opts.ReconnectMaxRetries)
	}
	if exec.opts.HeartbeatInterval != 0 || exec.opts.HeartbeatTimeout != 0 {
		t.Fatalf("unexpected heartbeat: interval=%s timeout=%s", exec.opts.HeartbeatInterval, exec.opts.HeartbeatTimeout)
	}
	if exec.opts.Timeout != 2*time.Second || exec.opts.Dialer.Timeout != 2*time.Second {
		t.Fatalf("unexpected timeout: opts=%s dialer=%s", exec.opts.Timeout, exec.opts.Dialer.Timeout)
	}
}

func TestWithNativeOptionsIgnoredForCustomExecutor(t *testing.T) {
	exec := &fakeExecutor{value: []byte("PONG")}
	client := NewClientWithExecutor(exec, WithNativeOptions(WithNativeReconnect(0)))

	got, err := client.Ping(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != "PONG" {
		t.Fatalf("expected PONG, got %q", got)
	}
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

func TestCreateBuildsAttributes(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)

	_, err := client.Create(context.Background(), CreateOptions{
		ID:         "flow-1",
		Type:       "order",
		State:      "created",
		Payload:    []byte("payload"),
		NowMS:      100,
		Attributes: map[string]any{"tenant": "acme", "priority": "high"},
	})

	if err != nil {
		t.Fatal(err)
	}
	got := exec.calls[0]
	if !containsSubsequence(got, []any{"ATTRIBUTE", "tenant", "acme"}) {
		t.Fatalf("missing tenant attribute in %#v", got)
	}
	if !containsSubsequence(got, []any{"ATTRIBUTE", "priority", "high"}) {
		t.Fatalf("missing priority attribute in %#v", got)
	}
}

func TestCreateManyMixedBuildsItems(t *testing.T) {
	exec := &fakeExecutor{value: []any{[]byte("OK"), []byte("OK")}}
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

func TestClaimDueDecodesNativeMaps(t *testing.T) {
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
				"attributes":     map[string]any{"tenant": "acme"},
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
	if record.Attributes["tenant"] != "acme" {
		t.Fatalf("unexpected attributes: %#v", record.Attributes)
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

func TestClaimDueDefaultsNow(t *testing.T) {
	exec := &fakeExecutor{value: []any{}}
	client := NewClientWithExecutor(exec)

	_, err := client.ClaimDue(context.Background(), ClaimDueOptions{Type: "order", Worker: "w"})

	if err != nil {
		t.Fatal(err)
	}
	got := exec.calls[0]
	index := indexOf(got, "NOW")
	if index < 0 || index+1 >= len(got) {
		t.Fatalf("expected NOW in claim command, got %#v", got)
	}
	if asInt64(got[index+1]) <= 0 {
		t.Fatalf("expected positive NOW, got %#v", got[index+1])
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

func TestRecordsFromNativeRejectsMalformedInput(t *testing.T) {
	if _, err := recordsFromNative("OK", RawCodec{}); err == nil {
		t.Fatal("expected non-array error")
	}
	if _, err := recordsFromNative([]any{[]any{"id"}}, RawCodec{}); err == nil {
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

func containsSubsequence(values []any, want []any) bool {
	for idx := 0; idx+len(want) <= len(values); idx++ {
		if reflect.DeepEqual(values[idx:idx+len(want)], want) {
			return true
		}
	}
	return false
}

func indexOf(values []any, needle string) int {
	for index, value := range values {
		if asString(value) == needle {
			return index
		}
	}
	return -1
}
