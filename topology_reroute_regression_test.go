package ferricstore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestTopologyRetriesExplicitlySafeStructuredRerouteOnce(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	endpoint := topologyEndpointFromListener(t, listener)
	serverErr := make(chan error, 1)
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
		if first.opcode != nativeOpGet {
			serverErr <- fmt.Errorf("first opcode = %d, want GET", first.opcode)
			return
		}
		if err := writeNativeTestResponse(writer, first, 5, map[string]any{
			"code":          "reroute",
			"message":       "stale epoch",
			"retryable":     true,
			"safe_to_retry": true,
		}); err != nil {
			serverErr <- err
			return
		}

		refresh, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if refresh.opcode != nativeOpShards {
			serverErr <- fmt.Errorf("second opcode = %d, want SHARDS", refresh.opcode)
			return
		}
		if err := writeNativeTestResponse(writer, refresh, nativeStatusOK, topologyResponseForEndpoint(endpoint, 2)); err != nil {
			serverErr <- err
			return
		}

		retry, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if retry.opcode != nativeOpGet {
			serverErr <- fmt.Errorf("third opcode = %d, want retried GET", retry.opcode)
			return
		}
		serverErr <- writeNativeTestResponse(writer, retry, nativeStatusOK, []byte("fresh"))
	}()

	exec, err := NewTopologyNativeExecutor(
		[]string{"ferric://" + listener.Addr().String()},
		WithTopologyEndpointPolicy(EndpointPolicyAny),
		WithTopologyNativeOptions(WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(0)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	if err := exec.installTopology(topologyForEndpoint(endpoint, 1)); err != nil {
		t.Fatal(err)
	}

	value, err := exec.Do(context.Background(), "GET", "key")
	if err != nil {
		t.Fatal(err)
	}
	if string(value.([]byte)) != "fresh" {
		t.Fatalf("GET = %#v, want fresh", value)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestTopologyRerouteBackoffReturnsCallerCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	endpoint := topologyEndpointFromListener(t, listener)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serverErr := make(chan error, 1)
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
		request, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		writeErr := writeNativeTestResponse(writer, request, nativeStatusReroute, map[string]any{
			"code": "reroute", "retryable": true, "safe_to_retry": true,
			"retry_after_ms": int64(500),
		})
		cancel()
		serverErr <- writeErr
	}()

	exec, err := NewTopologyNativeExecutor(
		[]string{"ferric://" + listener.Addr().String()},
		WithTopologyEndpointPolicy(EndpointPolicyAny),
		WithTopologyNativeOptions(WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(0)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = exec.Close() })
	if err := exec.installTopology(topologyForEndpoint(endpoint, 1)); err != nil {
		t.Fatal(err)
	}

	_, err = exec.Do(ctx, "GET", "key")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("reroute backoff error = %v; want context cancellation", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestTopologyRerouteBackoffHonorsNativeTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	endpoint := topologyEndpointFromListener(t, listener)
	serverErr := make(chan error, 1)
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
		request, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		serverErr <- writeNativeTestResponse(writer, request, nativeStatusReroute, map[string]any{
			"code": "reroute", "retryable": true, "safe_to_retry": true,
			"retry_after_ms": int64(100),
		})
		time.Sleep(200 * time.Millisecond)
	}()

	exec, err := NewTopologyNativeExecutor(
		[]string{"ferric://" + listener.Addr().String()},
		WithTopologyEndpointPolicy(EndpointPolicyAny),
		WithTopologyNativeOptions(WithNativeTimeout(20*time.Millisecond), WithNativeHeartbeat(0, 0), WithNativeReconnect(0)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = exec.Close() })
	if err := exec.installTopology(topologyForEndpoint(endpoint, 1)); err != nil {
		t.Fatal(err)
	}

	_, err = exec.Do(context.Background(), "GET", "key")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reroute backoff error = %v; want native timeout deadline", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestTopologyRouteErrorClassificationUsesDispositionNotMessage(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		refresh     bool
		safeToRetry bool
	}{
		{
			name: "status reroute",
			err: NativeError{Status: 5, Value: map[string]any{
				"message": "stale epoch", "retryable": true, "safe_to_retry": true,
			}},
			refresh:     true,
			safeToRetry: true,
		},
		{
			name: "code reroute",
			err: NativeError{Status: 1, Value: map[string]any{
				"code": "reroute", "retryable": true, "safe_to_retry": false,
			}},
			refresh: true,
		},
		{name: "application message", err: NativeError{Status: 1, Value: "route leader validation failed"}},
		{name: "connection closed", err: net.ErrClosed, refresh: true},
		{name: "canceled", err: context.Canceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			refresh, safeToRetry := topologyRouteErrorDisposition(test.err)
			if refresh != test.refresh || safeToRetry != test.safeToRetry {
				t.Fatalf("disposition = refresh:%v retry:%v, want refresh:%v retry:%v", refresh, safeToRetry, test.refresh, test.safeToRetry)
			}
		})
	}
}

func TestTopologyTypedKVRetriesExplicitlySafeReroute(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	endpoint := topologyEndpointFromListener(t, listener)
	serverErr := make(chan error, 1)
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
		if first.opcode != nativeOpMSet {
			serverErr <- fmt.Errorf("first opcode = %d, want MSET", first.opcode)
			return
		}
		if err := writeNativeTestResponse(writer, first, nativeStatusReroute, map[string]any{
			"code":          "reroute",
			"message":       "stale epoch",
			"retryable":     true,
			"safe_to_retry": true,
		}); err != nil {
			serverErr <- err
			return
		}

		refresh, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if refresh.opcode != nativeOpShards {
			serverErr <- fmt.Errorf("second opcode = %d, want SHARDS", refresh.opcode)
			return
		}
		if err := writeNativeTestResponse(writer, refresh, nativeStatusOK, topologyResponseForEndpoint(endpoint, 2)); err != nil {
			serverErr <- err
			return
		}

		retry, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if retry.opcode != nativeOpMSet {
			serverErr <- fmt.Errorf("third opcode = %d, want retried MSET", retry.opcode)
			return
		}
		serverErr <- writeNativeTestResponse(writer, retry, nativeStatusOK, []byte("OK"))
	}()

	exec, err := NewTopologyNativeExecutor(
		[]string{"ferric://" + listener.Addr().String()},
		WithTopologyEndpointPolicy(EndpointPolicyAny),
		WithTopologyNativeOptions(WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(0)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	if err := exec.installTopology(topologyForEndpoint(endpoint, 1)); err != nil {
		t.Fatal(err)
	}

	client := NewClientWithExecutor(exec)
	if err := client.KV().MSet(context.Background(), map[string]any{"key": "value"}); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestTopologySingleRoutePipelineRetriesExplicitlySafeReroute(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	endpoint := topologyEndpointFromListener(t, listener)
	serverErr := make(chan error, 1)
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
		if first.opcode != nativeOpPipeline {
			serverErr <- fmt.Errorf("first opcode = %d, want PIPELINE", first.opcode)
			return
		}
		if err := writeNativeTestResponse(writer, first, nativeStatusReroute, map[string]any{
			"code":          "reroute",
			"message":       "stale epoch",
			"retryable":     true,
			"safe_to_retry": true,
		}); err != nil {
			serverErr <- err
			return
		}

		refresh, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if refresh.opcode != nativeOpShards {
			serverErr <- fmt.Errorf("second opcode = %d, want SHARDS", refresh.opcode)
			return
		}
		if err := writeNativeTestResponse(writer, refresh, nativeStatusOK, topologyResponseForEndpoint(endpoint, 2)); err != nil {
			serverErr <- err
			return
		}

		retry, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if retry.opcode != nativeOpPipeline {
			serverErr <- fmt.Errorf("third opcode = %d, want retried PIPELINE", retry.opcode)
			return
		}
		serverErr <- writeNativeTestResponse(writer, retry, nativeStatusOK, []any{
			[]any{"ok", []byte("fresh")},
		})
	}()

	exec, err := NewTopologyNativeExecutor(
		[]string{"ferric://" + listener.Addr().String()},
		WithTopologyEndpointPolicy(EndpointPolicyAny),
		WithTopologyNativeOptions(WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(0)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	if err := exec.installTopology(topologyForEndpoint(endpoint, 1)); err != nil {
		t.Fatal(err)
	}

	values, err := exec.Pipeline(context.Background(), [][]any{{"GET", "key"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || string(values[0].([]byte)) != "fresh" {
		t.Fatalf("pipeline = %#v, want fresh", values)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestTopologySafeRerouteIsRetriedAtMostOnce(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	endpoint := topologyEndpointFromListener(t, listener)
	serverErr := make(chan error, 1)
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
		reroute := map[string]any{
			"code": "reroute", "message": "stale epoch", "retryable": true, "safe_to_retry": true,
		}
		if err := writeNativeTestResponse(writer, first, nativeStatusReroute, reroute); err != nil {
			serverErr <- err
			return
		}
		refresh, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if err := writeNativeTestResponse(writer, refresh, nativeStatusOK, topologyResponseForEndpoint(endpoint, 2)); err != nil {
			serverErr <- err
			return
		}
		retry, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if err := writeNativeTestResponse(writer, retry, nativeStatusReroute, reroute); err != nil {
			serverErr <- err
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		if unexpected, err := readNativeRequestFrame(reader); err == nil {
			serverErr <- fmt.Errorf("safe reroute was replayed more than once: opcode %d", unexpected.opcode)
			return
		} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	exec, err := NewTopologyNativeExecutor(
		[]string{"ferric://" + listener.Addr().String()},
		WithTopologyEndpointPolicy(EndpointPolicyAny),
		WithTopologyNativeOptions(WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(0)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	if err := exec.installTopology(topologyForEndpoint(endpoint, 1)); err != nil {
		t.Fatal(err)
	}
	_, err = exec.Do(context.Background(), "GET", "key")
	var nativeErr NativeError
	if !errors.As(err, &nativeErr) || nativeErr.Status != nativeStatusReroute {
		t.Fatalf("second reroute error = %v, want status-%d NativeError", err, nativeStatusReroute)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func serveNativeStartup(reader *bufio.Reader, writer *bufio.Writer) error {
	startup, err := readNativeRequestFrame(reader)
	if err != nil {
		return err
	}
	return writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true})
}
