package ferricstore

import (
	"bufio"
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestV080ServerRetryDispositionRequiresBothFlags(t *testing.T) {
	tests := []struct {
		name      string
		value     map[string]any
		retryable bool
	}{
		{name: "both", value: map[string]any{"code": "busy", "retryable": true, "safe_to_retry": true}, retryable: true},
		{name: "missing retryable", value: map[string]any{"code": "busy", "safe_to_retry": true}},
		{name: "missing safe", value: map[string]any{"code": "busy", "retryable": true}},
		{name: "retryable false", value: map[string]any{"code": "busy", "retryable": false, "safe_to_retry": true}},
		{name: "safe false", value: map[string]any{"code": "busy", "retryable": true, "safe_to_retry": false}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			disposition := nativeServerRetryDisposition(NativeError{Status: nativeStatusBusy, Value: tc.value})
			if disposition.retryable != tc.retryable {
				t.Fatalf("retryable = %v, want %v", disposition.retryable, tc.retryable)
			}
		})
	}
}

func TestV080ServerRetryDispositionUsesServerRetryAfter(t *testing.T) {
	disposition := nativeServerRetryDisposition(NativeError{Status: nativeStatusReroute, Value: map[string]any{
		"retryable": true, "safe_to_retry": true, "retry_after_ms": int64(75),
	}})
	if !disposition.retryable || !disposition.reroute || disposition.retryAfter != 75*time.Millisecond {
		t.Fatalf("disposition = %+v", disposition)
	}

	disposition = nativeServerRetryDisposition(NativeError{Status: nativeStatusBusy, Value: map[string]any{
		"retryable": true, "safe_to_retry": true, "retry_after_ms": int64(120_000),
	}})
	if disposition.retryAfter != 120*time.Second {
		t.Fatalf("retry_after = %v, want %v", disposition.retryAfter, 120*time.Second)
	}
}

func TestV080NativeRetriesExplicitlySafeBusyAfterServerDelay(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	serverErr := make(chan error, 1)
	requestGap := make(chan time.Duration, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		if err := serveNativeStartup(reader, writer); err != nil {
			serverErr <- err
			return
		}
		first, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		firstAt := time.Now()
		if err := writeNativeTestResponse(writer, first, nativeStatusBusy, map[string]any{
			"code": "busy", "retryable": true, "safe_to_retry": true, "retry_after_ms": int64(30),
		}); err != nil {
			serverErr <- err
			return
		}
		second, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		requestGap <- time.Since(firstAt)
		serverErr <- writeNativeTestResponse(writer, second, nativeStatusOK, []byte("fresh"))
	}()

	exec := NewNativeExecutor(
		listener.Addr().String(),
		WithNativeTimeout(time.Second),
		WithNativeHeartbeat(0, 0),
		WithNativeReconnect(0),
	)
	t.Cleanup(func() { _ = exec.Close() })
	value, err := exec.Do(context.Background(), "GET", "key")
	if err != nil {
		t.Fatal(err)
	}
	if string(value.([]byte)) != "fresh" {
		t.Fatalf("GET = %#v", value)
	}
	if gap := <-requestGap; gap < 25*time.Millisecond {
		t.Fatalf("retry gap = %v, want server delay", gap)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestV080NativeErrorCanBeUsedWithErrorsIs(t *testing.T) {
	err := NativeError{Status: nativeStatusBusy, Value: map[string]any{"code": "busy"}}
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("errors.Is panicked for NativeError: %v", recovered)
		}
	}()
	if errors.Is(err, NativeError{Status: nativeStatusBusy, Value: map[string]any{"code": "busy"}}) {
		t.Fatal("distinct native errors unexpectedly matched")
	}
}

func TestV080PipelineRerouteHonorsLargestServerRetryAfter(t *testing.T) {
	items := []pipelineItemResult{
		{err: NativeError{Status: nativeStatusReroute, Value: map[string]any{
			"retryable": true, "safe_to_retry": true, "retry_after_ms": int64(5),
		}}},
		{err: NativeError{Status: nativeStatusReroute, Value: map[string]any{
			"retryable": true, "safe_to_retry": true, "retry_after_ms": int64(25),
		}}},
	}
	routeErr, safe := topologyPipelineRouteDisposition(items)
	if !safe {
		t.Fatal("safe reroute pipeline was not retryable")
	}
	if delay := nativeServerRetryDisposition(routeErr).retryAfter; delay != 25*time.Millisecond {
		t.Fatalf("pipeline retry_after = %v; want largest server delay", delay)
	}
}
