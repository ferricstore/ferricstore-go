package ferricstore

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

func TestTopologyMSetPreflightsNativeWireEncodingBeforeShardWrites(t *testing.T) {
	listenerA, framesA, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []byte("OK")
	})
	listenerB, framesB, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []byte("OK")
	})
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(
		t,
		listenerA,
		listenerB,
		WithTopologyCrossShardWritePolicy(CrossShardWritePerShard),
	)
	t.Cleanup(func() { _ = exec.Close() })

	cycle := map[string]any{}
	cycle["self"] = cycle
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := NewClientWithExecutor(exec).KV().MSet(ctx, map[string]any{
		keyA: []byte("valid"),
		keyB: cycle,
	})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "cycle") {
		t.Fatalf("MSET wire preflight error = %v; want reference-cycle error", err)
	}
	assertNoNativeFrame(t, framesA, "valid-value shard")
	assertNoNativeFrame(t, framesB, "invalid-value shard")
}

func TestTopologyMSetPreflightsNegotiatedFrameLimitsBeforeShardWrites(t *testing.T) {
	listenerA, framesA := startFrameLimitedRoutedEndpoint(t, 1024)
	listenerB, framesB := startFrameLimitedRoutedEndpoint(t, 64)
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(
		t,
		listenerA,
		listenerB,
		WithTopologyCrossShardWritePolicy(CrossShardWritePerShard),
	)
	t.Cleanup(func() { _ = exec.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := NewClientWithExecutor(exec, WithCodec(RawCodec{})).KV().MSet(ctx, map[string]any{
		keyA: []byte("valid"),
		keyB: make([]byte, 256),
	})
	if !errorTreeContainsText(err, "server-advertised 64-byte frame limit") {
		t.Fatalf("MSET negotiated-limit preflight error = %v; want 64-byte limit error", err)
	}
	assertNoNativeFrame(t, framesA, "valid-size shard")
	assertNoNativeFrame(t, framesB, "oversized-value shard")
}

func TestTopologyMSetRejectsAggregatePreflightPayloadBeforeShardWrites(t *testing.T) {
	listenerA, framesA, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []byte("OK")
	})
	listenerB, framesB, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []byte("OK")
	})
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(
		t,
		listenerA,
		listenerB,
		WithTopologyCrossShardWritePolicy(CrossShardWritePerShard),
	)
	t.Cleanup(func() { _ = exec.Close() })
	valueA, valueB := []byte("first"), []byte("second")
	exec.maxMSetPreflightBytes = encodedSingleMSetSize(t, keyA, valueA) + encodedSingleMSetSize(t, keyB, valueB) - 1

	err := NewClientWithExecutor(exec, WithCodec(RawCodec{})).KV().MSet(context.Background(), map[string]any{
		keyA: valueA,
		keyB: valueB,
	})
	if !errorTreeContainsText(err, "aggregate MSET preflight payload") {
		t.Fatalf("MSET aggregate preflight error = %v; want aggregate payload limit error", err)
	}
	assertNoNativeFrame(t, framesA, "first aggregate-limit shard")
	assertNoNativeFrame(t, framesB, "second aggregate-limit shard")
}

func encodedSingleMSetSize(t *testing.T, key string, value any) int {
	t.Helper()
	command, err := newNativeMSetCommand([]string{key}, []any{value})
	if err != nil {
		t.Fatal(err)
	}
	body, err := encodeNativeValueWithLimit(command.payload, nativeMaxFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	return len(body)
}

func startFrameLimitedRoutedEndpoint(t *testing.T, frameLimit int64) (net.Listener, <-chan nativeFrame) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	frames := make(chan nativeFrame, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{
			"max_frame_bytes": frameLimit,
		}); err != nil {
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		frame, err := readNativeRequestFrame(reader)
		if err != nil {
			return
		}
		frames <- frame
		_ = writeNativeTestResponse(writer, frame, nativeStatusOK, []byte("OK"))
	}()
	return listener, frames
}

func errorTreeContainsText(err error, text string) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), text) {
		return true
	}
	if many, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range many.Unwrap() {
			if errorTreeContainsText(child, text) {
				return true
			}
		}
		return false
	}
	if one, ok := err.(interface{ Unwrap() error }); ok {
		return errorTreeContainsText(one.Unwrap(), text)
	}
	return false
}
