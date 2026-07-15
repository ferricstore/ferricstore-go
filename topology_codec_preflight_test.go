package ferricstore

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type topologyFailingCodec struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (c *topologyFailingCodec) Encode(value any) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.calls == 2 {
		return nil, c.err
	}
	return []byte(asString(value)), nil
}

func (*topologyFailingCodec) Decode(value any) (any, error) { return value, nil }

func (c *topologyFailingCodec) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func TestTopologyMSetPreflightsCustomCodecBeforeShardWrites(t *testing.T) {
	listenerA, framesA, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return []byte("OK") })
	listenerB, framesB, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return []byte("OK") })
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(
		t, listenerA, listenerB,
		WithTopologyCrossShardWritePolicy(CrossShardWritePerShard),
	)
	t.Cleanup(func() { _ = exec.Close() })
	want := errors.New("codec failed")
	codec := &topologyFailingCodec{err: want}
	client := NewClientWithExecutor(exec, WithCodec(codec))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := client.KV().MSet(ctx, map[string]any{keyA: "a", keyB: "b"})
	if !errors.Is(err, want) {
		t.Fatalf("MSET error = %v; want %v", err, want)
	}
	for endpoint, frames := range map[string]<-chan nativeFrame{"a": framesA, "b": framesB} {
		select {
		case frame := <-frames:
			t.Fatalf("codec failure occurred after endpoint %s write: %#v", endpoint, frame)
		default:
		}
	}
}

func TestAutoBatchCoalescedMSetPreflightsCodecBeforeShardWrites(t *testing.T) {
	response := func(frame nativeFrame, _ int) any {
		if frame.opcode == nativeOpPipeline {
			return []any{[]any{"ok", []any{[]byte("value")}}}
		}
		if frame.opcode == nativeOpMGet {
			return []any{[]byte("value")}
		}
		return []byte("OK")
	}
	listenerA, framesA, _ := startRoutedNativeEndpoint(t, response)
	listenerB, framesB, _ := startRoutedNativeEndpoint(t, response)
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(
		t, listenerA, listenerB,
		WithTopologyCrossShardWritePolicy(CrossShardWritePerShard),
	)
	t.Cleanup(func() { _ = exec.Close() })
	want := errors.New("codec failed")
	codec := &topologyFailingCodec{err: want}
	auto := &AutoBatchExecutor{client: NewClientWithExecutor(exec, WithCodec(codec))}
	deferred := func(value string) nativeDeferredCodecValue {
		return nativeDeferredCodecValue{codec: codec, value: value}
	}
	requests := []autoBatchRequest{
		{control: &autoBatchRequestControl{
			typedKind: autoBatchTypedKVMSet, typedKeys: []string{keyA, keyB},
			typedValues: []any{deferred("a"), deferred("b")}, allowQueued: true,
		}},
		{control: &autoBatchRequestControl{typedKind: autoBatchTypedKVMGet, typedKeys: []string{keyA}}},
	}

	results, err := auto.executeAutoBatchRequests(context.Background(), requests)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("coalesced results = %d; want 2", len(results))
	}
	if !errors.Is(results[0].err, want) {
		t.Fatalf("MSET error = %v; want %v", results[0].err, want)
	}
	if results[1].err != nil {
		t.Fatalf("coalesced MGET error = %v", results[1].err)
	}
	for endpoint, frames := range map[string]<-chan nativeFrame{"a": framesA, "b": framesB} {
		for {
			select {
			case frame := <-frames:
				if frame.opcode == nativeOpMSet {
					t.Fatalf("codec failure occurred after endpoint %s MSET write: %#v", endpoint, frame)
				}
			default:
				goto drained
			}
		}
	drained:
	}
}

func TestTopologyRejectedMSetDoesNotInvokeCodec(t *testing.T) {
	for _, tc := range []struct {
		name      string
		autoBatch bool
	}{
		{name: "direct topology"},
		{name: "through autobatch", autoBatch: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listenerA, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return []byte("OK") })
			listenerB, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return []byte("OK") })
			exec, keyA, keyB := topologyExecutorForTwoEndpoints(t, listenerA, listenerB)
			t.Cleanup(func() { _ = exec.Close() })
			codec := &topologyFailingCodec{err: errors.New("codec must not run")}
			client := NewClientWithExecutor(exec, WithCodec(codec))
			if tc.autoBatch {
				auto := NewAutoBatchExecutor(client, AutoBatchOptions{MaxSize: 1, FlushInterval: time.Hour})
				t.Cleanup(func() { _ = auto.Close() })
				client = NewClientWithExecutor(auto, WithCodec(codec))
			}

			err := client.KV().MSet(context.Background(), map[string]any{keyA: "a", keyB: "b"})
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "opt in") {
				t.Fatalf("rejected MSET error = %v; want cross-slot policy error", err)
			}
			if calls := codec.callCount(); calls != 0 {
				t.Fatalf("codec calls for rejected MSET = %d; want 0", calls)
			}
		})
	}
}

func TestAutoBatchRejectedMSetNXDoesNotInvokeCodec(t *testing.T) {
	listenerA, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return int64(1) })
	listenerB, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return int64(1) })
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(t, listenerA, listenerB)
	t.Cleanup(func() { _ = exec.Close() })
	codec := &topologyFailingCodec{err: errors.New("codec must not run")}
	base := NewClientWithExecutor(exec, WithCodec(codec))
	auto := NewAutoBatchExecutor(base, AutoBatchOptions{MaxSize: 1, FlushInterval: time.Hour})
	t.Cleanup(func() { _ = auto.Close() })
	client := NewClientWithExecutor(auto, WithCodec(codec))

	_, err := client.KV().MSetNX(context.Background(), map[string]any{keyA: "a", keyB: "b"})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "one hash slot") {
		t.Fatalf("rejected MSETNX error = %v; want hash-slot error", err)
	}
	if calls := codec.callCount(); calls != 0 {
		t.Fatalf("codec calls for rejected MSETNX = %d; want 0", calls)
	}
}
