package ferricstore

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

type fakeExecutor struct {
	calls  [][]any
	value  any
	values []any
	err    error
}

func (e *fakeExecutor) acquireCommandSession(context.Context, ...any) (commandSession, error) {
	return &executorCommandSession{exec: e}, nil
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

func TestWithNativeOptionsUpdatesRuntimeRequestQueueLimit(t *testing.T) {
	client := NewClient("127.0.0.1:6388",
		WithNativeOptions(WithNativeMaxQueuedRequests(0)),
	)
	defer func() { _ = client.Close() }()

	exec := client.exec.(*NativeExecutor)
	exec.flow.mu.Lock()
	maxQueued := exec.flow.maxQueued
	exec.flow.mu.Unlock()
	if maxQueued != 0 {
		t.Fatalf("runtime request queue limit = %d; want disabled queue", maxQueued)
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

func TestWithNativeOptionsDoesNotMutateInjectedNativeExecutor(t *testing.T) {
	exec := NewNativeExecutor("127.0.0.1:6388",
		WithNativeTimeout(3*time.Second),
		WithNativeHeartbeat(4*time.Second, 5*time.Second),
		WithNativeReconnect(6),
	)
	defer func() { _ = exec.Close() }()

	client := NewClientWithExecutor(exec, WithNativeOptions(
		WithNativeTimeout(time.Millisecond),
		WithNativeHeartbeat(0, 0),
		WithNativeReconnect(0),
	))
	if client.exec != exec {
		t.Fatalf("client executor = %T %p, want injected executor %p", client.exec, client.exec, exec)
	}
	if exec.opts.Timeout != 3*time.Second || exec.opts.Dialer.Timeout != 3*time.Second {
		t.Fatalf("injected timeout mutated to %s/%s", exec.opts.Timeout, exec.opts.Dialer.Timeout)
	}
	if exec.opts.HeartbeatInterval != 4*time.Second || exec.opts.HeartbeatTimeout != 5*time.Second {
		t.Fatalf("injected heartbeat mutated to %s/%s", exec.opts.HeartbeatInterval, exec.opts.HeartbeatTimeout)
	}
	if exec.opts.ReconnectMaxRetries != 6 {
		t.Fatalf("injected reconnect retries mutated to %d", exec.opts.ReconnectMaxRetries)
	}
}

func TestClientConstructorHandlesNilDependencies(t *testing.T) {
	exec := &fakeExecutor{value: []byte("PONG")}
	client := NewClientWithExecutor(exec, nil)
	if pong, err := client.Ping(context.Background()); err != nil || pong != "PONG" {
		t.Fatalf("client with nil option PING = %q, %v", pong, err)
	}

	client = NewClientWithExecutor(nil)
	if _, err := client.Ping(context.Background()); err == nil || !strings.Contains(err.Error(), "executor") {
		t.Fatalf("client with nil executor error = %v; want descriptive error", err)
	}
}

func TestRateLimitAddRejectsMalformedTypedFields(t *testing.T) {
	tests := []struct {
		name     string
		response any
	}{
		{name: "shape", response: []any{"allowed", int64(1), int64(2)}},
		{name: "status", response: []any{int64(1), int64(1), int64(2), int64(3)}},
		{name: "count", response: []any{"allowed", "not-an-integer", int64(2), int64(3)}},
		{name: "remaining", response: []any{"allowed", int64(1), true, int64(3)}},
		{name: "reset", response: []any{"allowed", int64(1), int64(2), map[string]any{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewClientWithExecutor(&fakeExecutor{value: tt.response})
			if _, err := client.RateLimitAdd(context.Background(), "rate", 1000, 10, 1); err == nil {
				t.Fatalf("accepted malformed ratelimit response %#v", tt.response)
			}
		})
	}
}

func TestKeyInfoRejectsMalformedTypedFields(t *testing.T) {
	valid := map[string]any{
		"type":             "string",
		"value_size":       int64(5),
		"ttl_ms":           int64(-1),
		"hot_cache_status": "hot",
		"last_write_shard": int64(2),
	}
	for _, field := range []string{"value_size", "ttl_ms", "last_write_shard"} {
		t.Run(field, func(t *testing.T) {
			response := make(map[string]any, len(valid))
			for key, value := range valid {
				response[key] = value
			}
			response[field] = "not-an-integer"
			client := NewClientWithExecutor(&fakeExecutor{value: response})
			if _, err := client.KeyInfo(context.Background(), "key"); err == nil {
				t.Fatalf("accepted malformed key_info %s", field)
			}
		})
	}
}

func TestFlowRecordPropagatesCodecErrors(t *testing.T) {
	_, err := recordFromNative(map[string]any{
		"id":      "flow-1",
		"payload": []byte("not-json"),
	}, JSONCodec{})
	if err == nil {
		t.Fatal("expected malformed payload to return a codec error")
	}

	_, err = recordFromNative(map[string]any{
		"id":      "flow-2",
		"payload": []byte(`{"ok":true}`),
		"values":  map[string]any{"broken": []byte("not-json")},
	}, JSONCodec{})
	if err == nil {
		t.Fatal("expected malformed named value to return a codec error")
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

func TestFlowMutationCommandsBuildStateMeta(t *testing.T) {
	exec := &fakeExecutor{values: []any{
		[]byte("OK"),
		map[string]any{"id": "f2", "type": "order", "state": "running"},
		[]byte("OK"),
		map[string]any{"id": "f1", "type": "order", "state": "running"},
		[]byte("OK"),
		[]byte("OK"),
		[]byte("OK"),
		[]byte("OK"),
		[]byte("OK"),
		[]byte("OK"),
		[]byte("OK"),
		[]byte("OK"),
	}}
	client := NewClientWithExecutor(exec)
	claimed := []ClaimedItem{{ID: "f1", PartitionKey: "tenant:1", LeaseToken: "lease", FencingToken: 7}}
	fenced := []FencedItem{{ID: "f1", PartitionKey: "tenant:1", LeaseToken: "lease", FencingToken: 7}}

	_, _ = client.Create(context.Background(), CreateOptions{ID: "f1", Type: "order", NowMS: 100, StateMeta: map[string]any{"version": 1}})
	_, _ = client.StartAndClaim(context.Background(), StartAndClaimOptions{ID: "f2", Type: "order", InitialState: "accept", Worker: "worker-1", NowMS: 101, StateMeta: map[string]any{"version": 2}})
	_, _ = client.Transition(context.Background(), TransitionOptions{ID: "f1", FromState: "queued", ToState: "charged", LeaseToken: "lease", FencingToken: 7, NowMS: 102, StateMeta: map[string]any{"version": 3}})
	_, _ = client.StepContinue(context.Background(), StepContinueOptions{ID: "f1", LeaseToken: "lease", FromState: "charged", ToState: "settled", FencingToken: 7, NowMS: 103, StateMeta: map[string]any{"version": 4}})
	_, _ = client.Complete(context.Background(), CompleteOptions{ID: "f1", LeaseToken: "lease", FencingToken: 7, StateMeta: map[string]any{"version": 5}})
	_, _ = client.Retry(context.Background(), RetryOptions{ID: "f1", LeaseToken: "lease", FencingToken: 7, StateMeta: map[string]any{"version": 6}})
	_, _ = client.Fail(context.Background(), FailOptions{ID: "f1", LeaseToken: "lease", FencingToken: 7, StateMeta: map[string]any{"version": 7}})
	_, _ = client.Cancel(context.Background(), CancelOptions{ID: "f1", LeaseToken: "lease", FencingToken: 7, StateMeta: map[string]any{"version": 8}})
	_, _ = client.CompleteMany(context.Background(), CompleteManyOptions{PartitionKey: "tenant:1", Items: claimed, StateMeta: map[string]any{"version": 9}})
	_, _ = client.TransitionMany(context.Background(), TransitionManyOptions{PartitionKey: "tenant:1", FromState: "queued", ToState: "charged", Items: fenced, StateMeta: map[string]any{"version": 10}})
	_, _ = client.RetryMany(context.Background(), RetryManyOptions{PartitionKey: "tenant:1", Items: claimed, StateMeta: map[string]any{"version": 11}})
	_, _ = client.FailMany(context.Background(), FailManyOptions{PartitionKey: "tenant:1", Items: claimed, StateMeta: map[string]any{"version": 12}})
	_, _ = client.CancelMany(context.Background(), CancelManyOptions{PartitionKey: "tenant:1", Items: fenced, StateMeta: map[string]any{"version": 13}})

	for idx, call := range exec.calls {
		want := []any{"STATE_META", "version", idx + 1}
		if !containsSubsequence(call, want) {
			t.Fatalf("call %d missing state_meta %v in %#v", idx, want, call)
		}
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

func TestCreateManyBuildsSharedStateMeta(t *testing.T) {
	exec := &fakeExecutor{value: []any{[]byte("OK"), []byte("OK")}}
	client := NewClientWithExecutor(exec)

	_, err := client.CreateMany(context.Background(), CreateManyOptions{
		Type:      "order",
		State:     "queued",
		NowMS:     100,
		StateMeta: map[string]any{"version": 1, "owner": "risk"},
		Items: []CreateItem{
			{ID: "f1", Payload: []byte("a")},
			{ID: "f2", Payload: []byte("b")},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	got := exec.calls[0]
	if !containsSubsequence(got, []any{"STATE_META", "version", 1}) {
		t.Fatalf("missing version state_meta in %#v", got)
	}
	if !containsSubsequence(got, []any{"STATE_META", "owner", "risk"}) {
		t.Fatalf("missing owner state_meta in %#v", got)
	}
}

func TestStartAndClaimBuildsCommand(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{"id": "f1", "type": "order", "state": "running", "run_state": "reserve"}}
	client := NewClientWithExecutor(exec)

	record, err := client.StartAndClaim(context.Background(), StartAndClaimOptions{
		ID:            "f1",
		Type:          "order",
		InitialState:  "reserve",
		Worker:        "worker-1",
		LeaseMS:       45_000,
		Payload:       []byte("payload"),
		PartitionKey:  "tenant:1",
		ParentFlowID:  "parent-1",
		RootFlowID:    "root-1",
		CorrelationID: "corr-1",
		NowMS:         100,
		Priority:      Int64(2),
		StateMeta:     map[string]any{"version": 1},
		Attributes:    map[string]any{"tenant": "acme"},
	})

	if err != nil {
		t.Fatal(err)
	}
	if record == nil || record.RunState != "reserve" {
		t.Fatalf("unexpected record: %#v", record)
	}
	want := []any{
		"FLOW.START_AND_CLAIM", "f1", "TYPE", "order", "INITIAL_STATE", "reserve",
		"WORKER", "worker-1", "LEASE_MS", int64(45_000), "NOW", int64(100),
		"PARTITION", "tenant:1", "PAYLOAD", []byte("payload"),
		"PARENT_FLOW_ID", "parent-1", "ROOT_FLOW_ID", "root-1", "CORRELATION_ID", "corr-1",
		"PRIORITY", int64(2), "ATTRIBUTE", "tenant", "acme", "STATE_META", "version", 1,
	}
	assertCall(t, exec, want)
}

func TestStepContinueBuildsCommand(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{"id": "f1", "type": "order", "state": "running", "run_state": "charged"}}
	client := NewClientWithExecutor(exec)

	record, err := client.StepContinue(context.Background(), StepContinueOptions{
		ID:           "f1",
		LeaseToken:   "lease-1",
		FromState:    "reserve",
		ToState:      "charged",
		FencingToken: 7,
		LeaseMS:      60_000,
		PartitionKey: "tenant:1",
		Payload:      []byte("payload"),
		Worker:       "worker-1",
		NowMS:        101,
		StateMeta:    map[string]any{"version": 2},
		NamedValues:  NamedValues{AttributesMerge: map[string]any{"phase": "charge"}},
	})

	if err != nil {
		t.Fatal(err)
	}
	if record == nil || record.RunState != "charged" {
		t.Fatalf("unexpected record: %#v", record)
	}
	want := []any{
		"FLOW.STEP_CONTINUE", "f1", "lease-1", "reserve", "charged",
		"FENCING", int64(7), "LEASE_MS", int64(60_000), "NOW", int64(101),
		"PARTITION", "tenant:1", "WORKER", "worker-1", "PAYLOAD", []byte("payload"),
		"ATTRIBUTE_MERGE", "phase", "charge", "STATE_META", "version", 2,
	}
	assertCall(t, exec, want)
}

func TestRunStepsManyBuildsCommand(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)

	err := client.RunStepsMany(context.Background(), RunStepsManyOptions{
		Type:    "order",
		States:  []string{"reserve", "charge", "email"},
		Worker:  "worker-1",
		LeaseMS: 30_000,
		NowMS:   123,
		Result:  []byte("ok"),
		Items:   []RunStepsItem{{ID: "f1", PartitionKey: "p1"}, {ID: "f2"}},
	})

	if err != nil {
		t.Fatal(err)
	}
	want := []any{
		"FLOW.RUN_STEPS_MANY", "TYPE", "order", "STATES", []string{"reserve", "charge", "email"},
		"WORKER", "worker-1", "LEASE_MS", int64(30_000), "NOW", int64(123),
		"RESULT", []byte("ok"),
		"ITEMS", []map[string]string{{"id": "f1", "partition_key": "p1"}, {"id": "f2"}},
	}
	assertCall(t, exec, want)
}

func TestRunStepsManyRequiresStatesOrSteps(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{value: []byte("OK")})

	if err := client.RunStepsMany(context.Background(), RunStepsManyOptions{Type: "order", Worker: "worker-1", Items: []RunStepsItem{{ID: "f1"}}}); err == nil {
		t.Fatal("expected missing states/steps error")
	}
	if err := client.RunStepsMany(context.Background(), RunStepsManyOptions{Type: "order", States: []string{"a"}, Steps: 1, Worker: "worker-1", Items: []RunStepsItem{{ID: "f1"}}}); err == nil {
		t.Fatal("expected mutually exclusive states/steps error")
	}
}

func TestSearchBuildsCommand(t *testing.T) {
	exec := &fakeExecutor{value: []any{map[string]any{"id": "f1", "type": "order", "state": "completed"}}}
	client := NewClientWithExecutor(exec)
	consistent := true
	includeCold := true
	rev := true
	terminalOnly := true

	records, err := client.Search(context.Background(), SearchOptions{
		Type:                 "order",
		State:                "completed",
		PartitionKey:         "tenant:1",
		Count:                Int(10),
		FromMS:               Int64(100),
		ToMS:                 Int64(200),
		Rev:                  &rev,
		TerminalOnly:         &terminalOnly,
		IncludeCold:          &includeCold,
		ConsistentProjection: &consistent,
		Attributes:           map[string]any{"tenant": "acme"},
		StateMeta:            map[string]map[string]any{"completed": {"version": 3}},
	})

	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].ID != "f1" {
		t.Fatalf("unexpected search records: %#v", records)
	}
	want := []any{
		"FLOW.SEARCH",
		"TYPE", "order",
		"STATE", "completed",
		"COUNT", 10,
		"PARTITION", "tenant:1",
		"FROM_MS", int64(100),
		"TO_MS", int64(200),
		"REV", "true",
		"TERMINAL_ONLY", "true",
		"INCLUDE_COLD", "true",
		"CONSISTENT_PROJECTION", "true",
		"ATTRIBUTE", "tenant", "acme",
		"STATE_META", "completed", "version", 3,
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
				"state_meta":     map[string]any{"queued": map[string]any{"version": int64(1)}},
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
	if record.StateMeta["queued"].(map[string]any)["version"] != int64(1) {
		t.Fatalf("unexpected state_meta: %#v", record.StateMeta)
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

func TestClaimJobsBuildsPartitionKeysAndDecodesFencing(t *testing.T) {
	exec := &fakeExecutor{
		value: []any{
			map[string]any{
				"id":            "flow-1",
				"type":          "order",
				"state":         "running",
				"partition_key": "tenant:b",
				"lease_token":   "lease-1",
				"fencing_token": int64(42),
			},
		},
	}
	client := NewClientWithExecutor(exec)

	jobs, err := client.ClaimJobs(context.Background(), ClaimDueOptions{
		Type:          "order",
		State:         "queued",
		Worker:        "worker-1",
		PartitionKeys: []string{"tenant:a", "tenant:b"},
		Limit:         5,
		NowMS:         100,
	})

	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected one claimed job, got %#v", jobs)
	}
	job := jobs[0]
	if job.ID != "flow-1" || job.PartitionKey != "tenant:b" || job.LeaseToken != "lease-1" || job.FencingToken != 42 {
		t.Fatalf("unexpected claimed job: %#v", job)
	}
	want := []any{
		"FLOW.CLAIM_DUE", "order", "STATE", "queued", "WORKER", "worker-1",
		"LEASE_MS", int64(30000), "LIMIT", 5, "NOW", int64(100),
		"PARTITIONS", 2, "tenant:a", "tenant:b", "RETURN", "JOBS_COMPACT_ATTRS",
	}
	assertCall(t, exec, want)
}

func TestCompleteManyMixedBuildsItems(t *testing.T) {
	exec := &fakeExecutor{value: []any{[]byte("OK"), []byte("OK")}}
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
