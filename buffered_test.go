package ferricstore

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type mutableBufferedMapKey struct {
	value string
}

type bufferedPrivateBytes struct {
	value []byte
}

func TestBufferedExecutorQueuesCopiedCommands(t *testing.T) {
	exec := NewBufferedExecutor(nil)
	args := []any{"SET", "k", "v"}

	value, err := exec.Do(context.Background(), args...)
	args[0] = "GET"

	if err != nil {
		t.Fatal(err)
	}
	if string(value.([]byte)) != "QUEUED" {
		t.Fatalf("unexpected placeholder value: %#v", value)
	}
	want := [][]any{{"SET", "k", "v"}}
	if !reflect.DeepEqual(exec.commands, want) {
		t.Fatalf("unexpected buffered commands\n got: %#v\nwant: %#v", exec.commands, want)
	}
}

func TestBufferedExecutorRejectsCanceledEnqueue(t *testing.T) {
	exec := NewBufferedExecutor(nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := exec.Do(ctx, "SET", "key", "value"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled buffered enqueue error = %v", err)
	}
	if len(exec.commands) != 0 {
		t.Fatalf("canceled command was queued: %#v", exec.commands)
	}
}

func TestBufferedExecutorSnapshotsNestedMutableArguments(t *testing.T) {
	exec := NewBufferedExecutor(nil)
	bytesValue := []byte("before")
	nested := map[string]any{"items": []any{bytesValue, map[string]any{"state": "queued"}}}
	if _, err := exec.Do(context.Background(), "CUSTOM", nested); err != nil {
		t.Fatal(err)
	}
	bytesValue[0] = 'X'
	nested["items"].([]any)[1].(map[string]any)["state"] = "changed"
	nested["extra"] = true

	want := [][]any{{"CUSTOM", map[string]any{
		"items": []any{[]byte("before"), map[string]any{"state": "queued"}},
	}}}
	if !reflect.DeepEqual(exec.commands, want) {
		t.Fatalf("buffered mutable snapshot\n got: %#v\nwant: %#v", exec.commands, want)
	}
}

func TestBufferedExecutorSnapshotsMutableMapKeys(t *testing.T) {
	exec := NewBufferedExecutor(nil)
	key := &mutableBufferedMapKey{value: "before"}
	if _, err := exec.Do(context.Background(), "CUSTOM", map[*mutableBufferedMapKey]string{key: "value"}); err != nil {
		t.Fatal(err)
	}
	key.value = "after"

	snapshot := exec.commands[0][1].(map[*mutableBufferedMapKey]string)
	for snapshotKey := range snapshot {
		if snapshotKey == key {
			t.Fatal("buffered map retained the caller-owned pointer key")
		}
		if snapshotKey.value != "before" {
			t.Fatalf("buffered map key value = %q; want before", snapshotKey.value)
		}
	}
}

func TestBufferedExecutorRejectsUnexportedMutableState(t *testing.T) {
	exec := NewBufferedExecutor(nil)
	_, err := exec.Do(context.Background(), "CUSTOM", bufferedPrivateBytes{value: []byte("before")})
	if err == nil || !strings.Contains(err.Error(), "unexported mutable field") {
		t.Fatalf("unexported mutable state error = %v", err)
	}
	if len(exec.commands) != 0 {
		t.Fatalf("rejected command was buffered: %#v", exec.commands)
	}
}

func TestBufferedExecutorAcceptsImmutableTimeValues(t *testing.T) {
	exec := NewBufferedExecutor(nil)
	value := time.Date(2026, time.July, 14, 12, 30, 0, 0, time.FixedZone("test", 90*60))
	if _, err := exec.Do(context.Background(), "CUSTOM", value); err != nil {
		t.Fatal(err)
	}
	snapshot := exec.commands[0][1].(time.Time)
	if !snapshot.Equal(value) || snapshot.Location().String() != "test" {
		t.Fatalf("buffered time = %v (%s); want %v (test)", snapshot, snapshot.Location(), value)
	}
}

func TestBufferedFlushErrorRetainsUncertainCommands(t *testing.T) {
	wantErr := errors.New("transport failed")
	client := NewClientWithExecutor(&fakePipelineExecutor{err: wantErr})
	exec := NewBufferedExecutor(client)
	_, _ = exec.Do(context.Background(), "SET", "key", "value")
	_, err := exec.Flush(context.Background())
	var flushErr *BufferedFlushError
	if !errors.As(err, &flushErr) || !errors.Is(err, wantErr) {
		t.Fatalf("flush error = %v, want BufferedFlushError wrapping transport error", err)
	}
	want := [][]any{{"SET", "key", "value"}}
	if !reflect.DeepEqual(flushErr.Commands, want) {
		t.Fatalf("uncertain commands = %#v, want %#v", flushErr.Commands, want)
	}
}

func TestBufferedTypedStatusQueuesWithoutReportingFailure(t *testing.T) {
	exec := NewBufferedExecutor(nil)
	client := NewClientWithExecutor(exec, WithCodec(RawCodec{}))

	if err := client.KV().Set(context.Background(), "key", "value"); err != nil {
		t.Fatalf("queued SET error = %v", err)
	}
	want := [][]any{{"SET", "key", "value"}}
	if !reflect.DeepEqual(exec.commands, want) {
		t.Fatalf("queued commands = %#v; want %#v", exec.commands, want)
	}
}

func TestBufferedTypedReplyFailsBeforeEnqueue(t *testing.T) {
	exec := NewBufferedExecutor(nil)
	client := NewClientWithExecutor(exec)

	if _, err := client.KV().Incr(context.Background(), "counter"); !errors.Is(err, ErrTypedReplyBuffered) {
		t.Fatalf("buffered INCR error = %v; want typed reply error", err)
	}
	if len(exec.commands) != 0 {
		t.Fatalf("reply-producing command was queued: %#v", exec.commands)
	}
}

func TestBufferedFlowWithoutReturnRecordQueuesSafely(t *testing.T) {
	exec := NewBufferedExecutor(nil)
	client := NewClientWithExecutor(exec, WithCodec(RawCodec{}))
	ctx := context.Background()

	record, err := client.Create(ctx, CreateOptions{
		ID: "job-1", Type: "email", State: "queued", NowMS: 1, ReturnRecord: false,
	})
	if err != nil || record != nil {
		t.Fatalf("queued Create result = %#v, %v; want nil, nil", record, err)
	}
	record, err = client.Complete(ctx, CompleteOptions{
		ID: "job-1", LeaseToken: "lease-1", FencingToken: 1, NowMS: 2, ReturnRecord: false,
	})
	if err != nil || record != nil {
		t.Fatalf("queued Complete result = %#v, %v; want nil, nil", record, err)
	}
	noReplyCalls := []struct {
		name string
		call func() (*FlowRecord, error)
	}{
		{name: "Transition", call: func() (*FlowRecord, error) {
			return client.Transition(ctx, TransitionOptions{ID: "job-1", FromState: "queued", ToState: "running", NowMS: 3})
		}},
		{name: "Retry", call: func() (*FlowRecord, error) {
			return client.Retry(ctx, RetryOptions{ID: "job-1", LeaseToken: "lease-1", FencingToken: 1, NowMS: 4})
		}},
		{name: "Fail", call: func() (*FlowRecord, error) {
			return client.Fail(ctx, FailOptions{ID: "job-1", LeaseToken: "lease-1", FencingToken: 1, NowMS: 5})
		}},
		{name: "Cancel", call: func() (*FlowRecord, error) {
			return client.Cancel(ctx, CancelOptions{ID: "job-1", FencingToken: 1, NowMS: 6})
		}},
		{name: "Rewind", call: func() (*FlowRecord, error) {
			return client.Rewind(ctx, RewindOptions{ID: "job-1", ToEvent: "1-0", NowMS: 7})
		}},
	}
	for _, test := range noReplyCalls {
		record, err = test.call()
		if err != nil || record != nil {
			t.Fatalf("queued %s result = %#v, %v; want nil, nil", test.name, record, err)
		}
	}
	if len(exec.commands) != 2+len(noReplyCalls) {
		t.Fatalf("queued Flow commands = %#v; want all no-reply mutations", exec.commands)
	}

	_, err = client.Create(ctx, CreateOptions{
		ID: "job-2", Type: "email", State: "queued", NowMS: 3, ReturnRecord: true,
	})
	if !errors.Is(err, ErrTypedReplyBuffered) {
		t.Fatalf("record-returning buffered Create error = %v; want %v", err, ErrTypedReplyBuffered)
	}
	if len(exec.commands) != 2+len(noReplyCalls) {
		t.Fatalf("record-returning Create was queued: %#v", exec.commands)
	}
}

func TestBufferedFlushReturnsPartialPipelineResults(t *testing.T) {
	wantErr := errors.New("second command failed")
	client := NewClientWithExecutor(&itemErrorPipelineExecutor{wantErr: wantErr})
	exec := NewBufferedExecutor(client)
	_, _ = exec.Do(context.Background(), "SET", "first", "value")
	_, _ = exec.Do(context.Background(), "FAIL")

	values, err := exec.Flush(context.Background())
	var flushErr *BufferedFlushError
	if !errors.As(err, &flushErr) || !errors.Is(err, wantErr) {
		t.Fatalf("flush error = %v; want BufferedFlushError wrapping %v", err, wantErr)
	}
	if len(values) != 2 || asString(values[0]) != "OK" {
		t.Fatalf("partial flush values = %#v; want successful first result", values)
	}
	if itemErr, ok := values[1].(error); !ok || !errors.Is(itemErr, wantErr) {
		t.Fatalf("failed flush value = %#v; want %v", values[1], wantErr)
	}
}

func TestBufferedExecutorEmptyFlush(t *testing.T) {
	exec := NewBufferedExecutor(nil)

	results, err := exec.Flush(context.Background())

	if err != nil {
		t.Fatal(err)
	}
	if results != nil {
		t.Fatalf("expected nil results, got %#v", results)
	}
	stats := exec.Stats()
	if stats.Flushes != 0 || stats.CommandsSent != 0 || stats.MaxDepth != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestBufferedExecutorConcurrentDo(t *testing.T) {
	exec := NewBufferedExecutor(NewClientWithExecutor(&fakePipelineExecutor{}))
	const count = 64

	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := exec.Do(context.Background(), "SET", i, i); err != nil {
				t.Errorf("Do failed: %v", err)
			}
		}(i)
	}
	wg.Wait()

	results, err := exec.Flush(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != count {
		t.Fatalf("flush returned %d results; want %d", len(results), count)
	}
	stats := exec.Stats()
	if stats.Flushes != 1 || stats.CommandsSent != count || stats.MaxDepth != count {
		t.Fatalf("unexpected stats after concurrent flush: %+v", stats)
	}
}

func TestBufferedExecutorStatsAreSafeDuringWrites(t *testing.T) {
	exec := NewBufferedExecutor(NewClientWithExecutor(&fakePipelineExecutor{}))
	const count = 100
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = exec.Do(context.Background(), "SET", i, i)
			_ = exec.Stats()
		}(i)
	}
	wg.Wait()
	_, _ = exec.Flush(context.Background())
	if stats := exec.Stats(); stats.CommandsSent != count {
		t.Fatalf("commands sent = %d, want %d", stats.CommandsSent, count)
	}
}

func TestBufferedExecutorRetainsExportedCounters(t *testing.T) {
	exec := NewBufferedExecutor(NewClientWithExecutor(&fakePipelineExecutor{}))
	_, _ = exec.Do(context.Background(), "SET", "a", "1")
	_, _ = exec.Do(context.Background(), "SET", "b", "2")
	if _, err := exec.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if exec.Flushes != 1 || exec.CommandsSent != 2 || exec.MaxDepth != 2 {
		t.Fatalf("exported counters = %d/%d/%d, want 1/2/2", exec.Flushes, exec.CommandsSent, exec.MaxDepth)
	}
}

var benchmarkBufferedArgsSink []any

func BenchmarkSnapshotBufferedArgsImmutable(b *testing.B) {
	args := []any{"SET", "key", "value", int64(1)}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var err error
		benchmarkBufferedArgsSink, err = snapshotCommandArgs(args)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSnapshotBufferedArgsNestedMutable(b *testing.B) {
	args := []any{"CUSTOM", map[string]any{
		"payload": []byte("value"),
		"items":   []any{"one", int64(2)},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var err error
		benchmarkBufferedArgsSink, err = snapshotCommandArgs(args)
		if err != nil {
			b.Fatal(err)
		}
	}
}
