package ferricstore

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

var _ keyValueBulkExecutor = (*NativeExecutor)(nil)
var _ keyValueBulkExecutor = (*TopologyNativeExecutor)(nil)
var _ keyValueMSetNXExecutor = (*NativeExecutor)(nil)
var _ keyValueMSetNXExecutor = (*TopologyNativeExecutor)(nil)
var _ keyValueDelExecutor = (*NativeExecutor)(nil)
var _ keyValueDelExecutor = (*TopologyNativeExecutor)(nil)
var _ keyValueExistsExecutor = (*NativeExecutor)(nil)
var _ keyValueExistsExecutor = (*TopologyNativeExecutor)(nil)

func TestTopologyTypedDelAndExistsPreserveTypedWirePaths(t *testing.T) {
	listenerA, framesA, errsA := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return int64(1) })
	listenerB, framesB, errsB := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return int64(1) })
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(
		t, listenerA, listenerB,
		WithTopologyCrossShardWritePolicy(CrossShardWritePerShard),
	)
	t.Cleanup(func() { _ = exec.Close() })
	store := NewClientWithExecutor(exec).KV()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if count, err := store.Exists(ctx, keyA, keyB); err != nil || count != 2 {
		t.Fatalf("typed topology EXISTS = %d, %v", count, err)
	}
	if count, err := store.Del(ctx, keyA, keyB); err != nil || count != 2 {
		t.Fatalf("typed topology DEL = %d, %v", count, err)
	}
	for endpoint, frames := range map[string]<-chan nativeFrame{"a": framesA, "b": framesB} {
		select {
		case frame := <-frames:
			if frame.opcode != nativeOpCommandExec {
				t.Fatalf("endpoint %s EXISTS opcode = %d; want %d", endpoint, frame.opcode, nativeOpCommandExec)
			}
		case <-ctx.Done():
			t.Fatalf("endpoint %s did not receive EXISTS", endpoint)
		}
		select {
		case frame := <-frames:
			if frame.opcode != nativeOpDel {
				t.Fatalf("endpoint %s DEL opcode = %d; want %d", endpoint, frame.opcode, nativeOpDel)
			}
		case <-ctx.Done():
			t.Fatalf("endpoint %s did not receive DEL", endpoint)
		}
	}
	if err := <-errsA; err != nil {
		t.Fatal(err)
	}
	if err := <-errsB; err != nil {
		t.Fatal(err)
	}
}

func TestTopologyKeyValueMGetPreservesCrossShardOrder(t *testing.T) {
	listenerA, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []any{[]byte("from-a")}
	})
	listenerB, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []any{[]byte("from-b")}
	})
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(t, listenerA, listenerB)
	t.Cleanup(func() { _ = exec.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	values, err := NewClientWithExecutor(exec).KV().MGet(ctx, keyB, keyA)
	if err != nil {
		t.Fatal(err)
	}
	want := []any{[]byte("from-b"), []byte("from-a")}
	if !reflect.DeepEqual(values, want) {
		t.Fatalf("cross-shard MGET = %#v; want %#v", values, want)
	}
}

func TestTopologyKeyValueMSetUsesOwningRoute(t *testing.T) {
	listenerA, framesA, errsA := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return []byte("OK") })
	listenerB, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return []byte("OK") })
	exec, keyA, _ := topologyExecutorForTwoEndpoints(t, listenerA, listenerB)
	t.Cleanup(func() { _ = exec.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := NewClientWithExecutor(exec).KV().MSet(ctx, map[string]any{keyA: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	select {
	case frame := <-framesA:
		if frame.opcode != nativeOpMSet {
			t.Fatalf("topology MSET opcode = %d; want %d", frame.opcode, nativeOpMSet)
		}
	case <-ctx.Done():
		t.Fatal("owning route did not receive MSET")
	}
	if err := <-errsA; err != nil {
		t.Fatal(err)
	}
}

func TestTopologyCrossSlotMSetRequiresExplicitPolicy(t *testing.T) {
	listenerA, framesA, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return []byte("OK") })
	listenerB, framesB, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return []byte("OK") })
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(t, listenerA, listenerB)
	t.Cleanup(func() { _ = exec.Close() })

	err := NewClientWithExecutor(exec).KV().MSet(context.Background(), map[string]any{keyA: "a", keyB: "b"})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "opt in") {
		t.Fatalf("default cross-slot MSET error = %v; want explicit policy rejection", err)
	}
	select {
	case frame := <-framesA:
		t.Fatalf("rejected MSET reached endpoint A: %#v", frame)
	default:
	}
	select {
	case frame := <-framesB:
		t.Fatalf("rejected MSET reached endpoint B: %#v", frame)
	default:
	}
}

func TestTopologyCrossSlotMSetPolicyAppliesToAllEntryPoints(t *testing.T) {
	tests := []struct {
		name string
		call func(context.Context, *TopologyNativeExecutor, string, string) error
	}{
		{name: "typed store", call: func(ctx context.Context, exec *TopologyNativeExecutor, keyA, keyB string) error {
			return NewClientWithExecutor(exec).KV().MSet(ctx, map[string]any{keyA: "a", keyB: "b"})
		}},
		{name: "command", call: func(ctx context.Context, exec *TopologyNativeExecutor, keyA, keyB string) error {
			_, err := exec.Do(ctx, "MSET", keyA, "a", keyB, "b")
			return err
		}},
		{name: "pipeline", call: func(ctx context.Context, exec *TopologyNativeExecutor, keyA, keyB string) error {
			_, err := exec.Pipeline(ctx, [][]any{{"MSET", keyA, "a", keyB, "b"}})
			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			listenerA, framesA, errsA := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return []byte("OK") })
			listenerB, framesB, errsB := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return []byte("OK") })
			exec, keyA, keyB := topologyExecutorForTwoEndpoints(
				t, listenerA, listenerB,
				WithTopologyCrossShardWritePolicy(CrossShardWritePerShard),
			)
			t.Cleanup(func() { _ = exec.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			if err := tc.call(ctx, exec, keyA, keyB); err != nil {
				t.Fatal(err)
			}
			for name, frames := range map[string]<-chan nativeFrame{"a": framesA, "b": framesB} {
				select {
				case frame := <-frames:
					if frame.opcode != nativeOpMSet {
						t.Fatalf("endpoint %s MSET opcode = %d; want %d", name, frame.opcode, nativeOpMSet)
					}
				case <-ctx.Done():
					t.Fatalf("endpoint %s did not receive its MSET", name)
				}
			}
			if err := <-errsA; err != nil {
				t.Fatal(err)
			}
			if err := <-errsB; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTopologyCrossSlotMSetReportsPartialWrites(t *testing.T) {
	listenerA, _, errsA := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return []byte("OK") })
	listenerB, errsB := startFailingRoutedNativeEndpoint(t)
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(
		t, listenerA, listenerB,
		WithTopologyCrossShardWritePolicy(CrossShardWritePerShard),
	)
	t.Cleanup(func() { _ = exec.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := NewClientWithExecutor(exec).KV().MSet(ctx, map[string]any{keyA: "a", keyB: "b"})
	var partial *TopologyPartialWriteError
	if !errors.As(err, &partial) {
		t.Fatalf("cross-slot MSET error = %T %v; want TopologyPartialWriteError", err, err)
	}
	if partial.Command != "MSET" || partial.Succeeded != 1 || len(partial.Failures) != 1 {
		t.Fatalf("partial MSET = %+v; want one successful key and one failure", partial)
	}
	assertTopologyWriteFailure(t, partial.Failures[0], keyB, listenerB)
	if err := <-errsA; err != nil {
		t.Fatal(err)
	}
	if err := <-errsB; err != nil {
		t.Fatal(err)
	}
}

func TestTopologyPipelineScattersExistsAcrossAllKeyRoutes(t *testing.T) {
	response := func(frame nativeFrame, _ int) any {
		if frame.opcode == nativeOpPipeline {
			return []any{[]any{"ok", int64(1)}}
		}
		return int64(1)
	}
	listenerA, _, _ := startRoutedNativeEndpoint(t, response)
	listenerB, _, _ := startRoutedNativeEndpoint(t, response)
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(t, listenerA, listenerB)
	t.Cleanup(func() { _ = exec.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	values, err := exec.Pipeline(ctx, [][]any{{"EXISTS", keyA, keyB}})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || asInt64(values[0]) != 2 {
		t.Fatalf("cross-shard pipeline EXISTS = %#v; want count 2", values)
	}
}

func TestTopologyPipelineDoesNotBypassCrossShardUnlinkPolicy(t *testing.T) {
	response := func(frame nativeFrame, _ int) any {
		if frame.opcode == nativeOpPipeline {
			return []any{[]any{"ok", int64(1)}}
		}
		return int64(1)
	}
	listenerA, _, _ := startRoutedNativeEndpoint(t, response)
	listenerB, _, _ := startRoutedNativeEndpoint(t, response)
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(t, listenerA, listenerB)
	t.Cleanup(func() { _ = exec.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := exec.Pipeline(ctx, [][]any{{"UNLINK", keyA, keyB}})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "cross-shard") {
		t.Fatalf("cross-shard pipeline UNLINK error = %v; want explicit policy rejection", err)
	}
}

func TestTopologyScatterRejectsImpossibleShardCounts(t *testing.T) {
	listenerA, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return int64(2) })
	listenerB, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return int64(2) })
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(t, listenerA, listenerB)
	t.Cleanup(func() { _ = exec.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := exec.Do(ctx, "EXISTS", keyA, keyB)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "count") {
		t.Fatalf("impossible scatter count error = %v", err)
	}
}

func TestTopologyPipelinePreservesSuccessfulShardResultsOnFailure(t *testing.T) {
	listenerA, _, errsA := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []any{[]any{"ok", "stored-a"}}
	})
	listenerB, errsB := startFailingRoutedNativeEndpoint(t)
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(t, listenerA, listenerB)
	t.Cleanup(func() { _ = exec.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	values, err := exec.Pipeline(ctx, [][]any{{"SET", keyA, "1"}, {"SET", keyB, "2"}})
	if err == nil {
		t.Fatal("expected one routed pipeline group to fail")
	}
	if len(values) != 2 || asString(values[0]) != "stored-a" {
		t.Fatalf("partial topology pipeline values = %#v; want successful first result", values)
	}
	if itemErr, ok := values[1].(error); !ok || !errors.Is(err, itemErr) {
		t.Fatalf("failed topology item = %#v, aggregate error = %v", values[1], err)
	}
	if endpointErr := <-errsA; endpointErr != nil {
		t.Fatal(endpointErr)
	}
	if endpointErr := <-errsB; endpointErr != nil {
		t.Fatal(endpointErr)
	}
}
