package ferricstore

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func TestTopologyKeyValueCountsRejectNonIntegerWireTypes(t *testing.T) {
	listenerA, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return float64(1) })
	listenerB, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return float64(1) })
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(t, listenerA, listenerB)
	t.Cleanup(func() { _ = exec.Close() })
	store := NewClientWithExecutor(exec).KV()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for _, keys := range [][]string{{keyA}, {keyA, keyB}} {
		t.Run(strconv.Itoa(len(keys))+" keys", func(t *testing.T) {
			if count, err := store.Exists(ctx, keys...); err == nil {
				t.Fatalf("topology EXISTS accepted float reply as count %d", count)
			}
		})
	}
}
