package ferricstore

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestGenericScatterUsesOneTopologySnapshot(t *testing.T) {
	listenerA, framesA, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []any{[]byte("old")}
	})
	listenerB, framesB, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []any{[]byte("new-a"), []byte("new-b")}
	})
	endpointA := topologyEndpointFromListener(t, listenerA)
	endpointB := topologyEndpointFromListener(t, listenerB)
	exec := topologySnapshotTestExecutor(topologyForEndpoint(endpointA, 1))
	t.Cleanup(func() { _ = exec.Close() })

	var validations atomic.Int32
	exec.endpointValidator = func(RoutingEndpoint) bool {
		if validations.Add(1) == 2 {
			if err := exec.installTopology(topologyForEndpoint(endpointB, 2)); err != nil {
				t.Errorf("install replacement topology: %v", err)
			}
		}
		return true
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	value, err := exec.Do(ctx, "MGET", "first", "second")
	if err != nil {
		t.Fatal(err)
	}
	items, ok := value.([]any)
	if !ok || len(items) != 2 || asString(items[0]) != "new-a" || asString(items[1]) != "new-b" {
		t.Fatalf("snapshot MGET response = %#v", value)
	}
	assertNoNativeFrame(t, framesA, "old topology endpoint")
	select {
	case frame := <-framesB:
		if frame.opcode != nativeOpMGet {
			t.Fatalf("new endpoint opcode = %d; want MGET", frame.opcode)
		}
	case <-ctx.Done():
		t.Fatal("new topology endpoint did not receive MGET")
	}
}

func TestTopologyPipelineUsesOneTopologySnapshot(t *testing.T) {
	listenerA, framesA, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []any{[]any{"ok", []byte("OK")}}
	})
	listenerB, framesB, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []any{
			[]any{"ok", []byte("OK")},
			[]any{"ok", []byte("new")},
		}
	})
	endpointA := topologyEndpointFromListener(t, listenerA)
	endpointB := topologyEndpointFromListener(t, listenerB)
	exec := topologySnapshotTestExecutor(topologyForEndpoint(endpointA, 1))
	t.Cleanup(func() { _ = exec.Close() })

	var validations atomic.Int32
	exec.endpointValidator = func(RoutingEndpoint) bool {
		if validations.Add(1) == 2 {
			if err := exec.installTopology(topologyForEndpoint(endpointB, 2)); err != nil {
				t.Errorf("install replacement topology: %v", err)
			}
		}
		return true
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	values, err := exec.Pipeline(ctx, [][]any{{"SET", "key", "value"}, {"GET", "key"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || !isOK(values[0]) || asString(values[1]) != "new" {
		t.Fatalf("snapshot pipeline response = %#v", values)
	}
	assertNoNativeFrame(t, framesA, "old topology endpoint")
	select {
	case frame := <-framesB:
		if frame.opcode != nativeOpPipeline {
			t.Fatalf("new endpoint opcode = %d; want PIPELINE", frame.opcode)
		}
	case <-ctx.Done():
		t.Fatal("new topology endpoint did not receive pipeline")
	}
}

func TestTopologyDoesNotCacheAdapterCreatedFromStaleRoute(t *testing.T) {
	listenerA, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []byte("value")
	})
	listenerB, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []byte("new")
	})
	endpointA := topologyEndpointFromListener(t, listenerA)
	endpointB := topologyEndpointFromListener(t, listenerB)
	exec := topologySnapshotTestExecutor(topologyForEndpoint(endpointA, 1))
	t.Cleanup(func() { _ = exec.Close() })

	var validations atomic.Int32
	exec.endpointValidator = func(RoutingEndpoint) bool {
		if validations.Add(1) == 2 {
			if err := exec.installTopology(topologyForEndpoint(endpointB, 2)); err != nil {
				t.Errorf("install replacement topology: %v", err)
			}
		}
		return true
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := exec.Do(ctx, "GET", "key"); err == nil {
		t.Fatal("request using a route replaced during adapter acquisition succeeded")
	} else if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stale route was dialed instead of rejected: %v", err)
	}

	oldKey := connectionKeyForEndpoint(endpointA, false)
	exec.mu.Lock()
	_, cached := exec.adapters[oldKey]
	exec.mu.Unlock()
	if cached {
		t.Fatal("adapter for stale route remained cached")
	}
}

func TestTopologyTypedKVDoesNotCacheAdapterCreatedFromStaleRoute(t *testing.T) {
	listenerA, framesA, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []any{[]byte("old")}
	})
	listenerB, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []any{[]byte("new")}
	})
	endpointA := topologyEndpointFromListener(t, listenerA)
	endpointB := topologyEndpointFromListener(t, listenerB)
	exec := topologySnapshotTestExecutor(topologyForEndpoint(endpointA, 1))
	t.Cleanup(func() { _ = exec.Close() })

	var validations atomic.Int32
	exec.endpointValidator = func(RoutingEndpoint) bool {
		if validations.Add(1) == 2 {
			if err := exec.installTopology(topologyForEndpoint(endpointB, 2)); err != nil {
				t.Errorf("install replacement topology: %v", err)
			}
		}
		return true
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := NewClientWithExecutor(exec).KV().MGet(ctx, "key"); err == nil {
		t.Fatal("typed MGET using a route replaced during adapter acquisition succeeded")
	} else if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stale typed route was dialed instead of rejected: %v", err)
	}
	assertNoNativeFrame(t, framesA, "old typed topology endpoint")

	oldKey := connectionKeyForEndpoint(endpointA, false)
	exec.mu.Lock()
	_, cached := exec.adapters[oldKey]
	exec.mu.Unlock()
	if cached {
		t.Fatal("typed KV adapter for stale route remained cached")
	}
}

func TestTopologyRoutePlanningDoesNotRefreshForInvalidCommand(t *testing.T) {
	exec := topologySnapshotTestExecutor(&RoutingTopology{})
	t.Cleanup(func() { _ = exec.Close() })

	_, err := exec.routeData(context.Background(), []any{"MSET", "key"})
	if err == nil || !strings.Contains(err.Error(), "MSET requires") {
		t.Fatalf("invalid MSET routing error = %v; want original command validation error", err)
	}
}

func topologySnapshotTestExecutor(topology *RoutingTopology) *TopologyNativeExecutor {
	return &TopologyNativeExecutor{
		adapters:         make(map[string]*NativeExecutor),
		retiringAdapters: make(map[*NativeExecutor]struct{}),
		seedEndpointKeys: make(map[string]struct{}),
		seedURLByKey:     make(map[string]string),
		endpointPolicy:   EndpointPolicyAny,
		topology:         topology,
		topologyVersion:  1,
		nativeOptions: []NativeOption{
			WithNativeTimeout(time.Second),
			WithNativeHeartbeat(0, 0),
			WithNativeReconnect(0),
		},
	}
}

func assertNoNativeFrame(t *testing.T, frames <-chan nativeFrame, endpoint string) {
	t.Helper()
	select {
	case frame := <-frames:
		t.Fatalf("%s received stale frame opcode %d", endpoint, frame.opcode)
	case <-time.After(25 * time.Millisecond):
	}
}
