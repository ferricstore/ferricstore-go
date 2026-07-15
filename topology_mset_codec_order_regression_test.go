package ferricstore

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
)

type topologyMSetOrderCodec struct {
	mu    sync.Mutex
	calls []string
}

func (c *topologyMSetOrderCodec) Encode(value any) (any, error) {
	c.mu.Lock()
	c.calls = append(c.calls, value.(string))
	c.mu.Unlock()
	return []byte(value.(string)), nil
}

func (*topologyMSetOrderCodec) Decode(value any) (any, error) { return value, nil }

func (c *topologyMSetOrderCodec) takeCalls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := append([]string(nil), c.calls...)
	c.calls = c.calls[:0]
	return out
}

func TestTopologyMSetDeferredCodecOrderIsDeterministicAcrossSlotGroups(t *testing.T) {
	exec, route := singleRouteTopologyBenchmarkFixture()
	exec.crossShardWrites = CrossShardWritePerShard
	closedAdapter := NewNativeExecutor("127.0.0.1:6388")
	if err := closedAdapter.Close(); err != nil {
		t.Fatalf("close adapter: %v", err)
	}
	exec.adapters = map[string]*NativeExecutor{
		connectionKeyForEndpoint(route.Endpoint, false): closedAdapter,
	}
	exec.seedURLByKey = make(map[string]string)
	t.Cleanup(func() { _ = exec.Close() })
	codec := &topologyMSetOrderCodec{}
	want := make([]string, 0, 8)
	seenSlots := make(map[int]struct{}, 8)
	for candidate := 0; len(want) < cap(want); candidate++ {
		key := "key:" + strconv.Itoa(10_000+candidate)
		slot := routeSlotForString(key)
		if _, exists := seenSlots[slot]; exists {
			continue
		}
		seenSlots[slot] = struct{}{}
		want = append(want, key)
	}

	for range 32 {
		groups := make(map[int]*topologyMSetGroup, len(want))
		for _, key := range want {
			slot := routeSlotForString(key)
			groups[slot] = &topologyMSetGroup{
				stringKeys: []string{key},
				values: []any{nativeDeferredCodecValue{
					codec: codec,
					value: key,
				}},
			}
		}
		_, _ = exec.executeTopologyMSet(context.Background(), groups, nil, false)
		if got := codec.takeCalls(); !reflect.DeepEqual(got, want) {
			t.Fatalf("deferred codec order = %v; want %v", got, want)
		}
	}
}

func TestTopologyMSetCanceledContextDoesNotInvokeDeferredCodec(t *testing.T) {
	exec, _ := singleRouteTopologyBenchmarkFixture()
	exec.crossShardWrites = CrossShardWritePerShard
	codec := &topologyMSetOrderCodec{}
	key := "account:cancelled"
	groups := map[int]*topologyMSetGroup{
		routeSlotForString(key): {
			stringKeys: []string{key},
			values: []any{nativeDeferredCodecValue{
				codec: codec,
				value: key,
			}},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := exec.executeTopologyMSet(ctx, groups, nil, false)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("MSET error = %v; want context.Canceled", err)
	}
	if got := codec.takeCalls(); len(got) != 0 {
		t.Fatalf("codec calls = %v; want none for canceled request", got)
	}
}
