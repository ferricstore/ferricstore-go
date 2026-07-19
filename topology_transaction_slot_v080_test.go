package ferricstore

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestV080TopologyTransactionRejectsCommandOutsidePinnedSlot(t *testing.T) {
	seed, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return []byte("OK") })
	routed, frames, errCh := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return []byte("OK") })
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(t, seed, routed)
	t.Cleanup(func() { _ = exec.Close() })
	client := NewClientWithExecutor(exec)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	tx, err := client.TransactionForKeys(ctx, keyB)
	if err != nil {
		t.Fatal(err)
	}
	_, commandErr := tx.Command(ctx, "SET", keyA, "wrong shard")
	if err := tx.Discard(ctx); err != nil {
		t.Fatal(err)
	}
	if commandErr == nil || !strings.Contains(strings.ToLower(commandErr.Error()), "slot") {
		t.Fatalf("cross-slot transaction command error = %v; want local slot rejection", commandErr)
	}

	assertTopologyTransactionFrames(t, frames, 2)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestV080TopologyWatchRejectsCommandOutsidePinnedSlot(t *testing.T) {
	seed, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return []byte("OK") })
	routed, frames, errCh := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return []byte("OK") })
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(t, seed, routed)
	t.Cleanup(func() { _ = exec.Close() })
	client := NewClientWithExecutor(exec)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := client.Watch(ctx, keyB); err != nil {
		t.Fatal(err)
	}
	_, commandErr := client.Command(ctx, "GET", keyA)
	if err := client.Unwatch(ctx); err != nil {
		t.Fatal(err)
	}
	if commandErr == nil || !strings.Contains(strings.ToLower(commandErr.Error()), "slot") {
		t.Fatalf("cross-slot watched command error = %v; want local slot rejection", commandErr)
	}

	assertTopologyTransactionFrames(t, frames, 2)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestV080TopologyTransactionAllowsLocalNoKeyCommand(t *testing.T) {
	seed, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return []byte("OK") })
	routed, frames, errCh := startRoutedNativeEndpoint(t, func(_ nativeFrame, request int) any {
		if request == 1 {
			return []byte("QUEUED")
		}
		return []byte("OK")
	})
	exec, _, keyB := topologyExecutorForTwoEndpoints(t, seed, routed)
	t.Cleanup(func() { _ = exec.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	tx, err := NewClientWithExecutor(exec).TransactionForKeys(ctx, keyB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Command(ctx, "PING"); err != nil {
		t.Fatalf("topology transaction PING failed: %v", err)
	}
	if err := tx.Discard(ctx); err != nil {
		t.Fatal(err)
	}
	assertTopologyTransactionFrames(t, frames, 3)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func assertTopologyTransactionFrames(t *testing.T, frames <-chan nativeFrame, count int) {
	t.Helper()
	for index := 0; index < count; index++ {
		select {
		case frame := <-frames:
			if frame.opcode != nativeOpCommandExec || frame.laneID != 2 {
				t.Fatalf("frame %d = opcode %d lane %d; want opcode %d lane 2", index, frame.opcode, frame.laneID, nativeOpCommandExec)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for transaction frame %d", index)
		}
	}
	select {
	case frame := <-frames:
		t.Fatalf("cross-slot command reached pinned transaction lane: opcode %d lane %d", frame.opcode, frame.laneID)
	case <-time.After(300 * time.Millisecond):
	}
}
