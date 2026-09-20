package ferricstore

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

type flipDeadlineContext struct {
	context.Context
	mu      sync.RWMutex
	expired bool
}

func (ctx *flipDeadlineContext) Deadline() (time.Time, bool) {
	ctx.mu.RLock()
	expired := ctx.expired
	ctx.mu.RUnlock()
	if expired {
		return time.Unix(0, 0), true
	}
	return time.Now().Add(time.Hour), true
}

func (ctx *flipDeadlineContext) expire() {
	ctx.mu.Lock()
	ctx.expired = true
	ctx.mu.Unlock()
}

func TestNativeDecodedResponseAfterDeadlineIsRejected(t *testing.T) {
	exec, clientConn, serverConn := newNativeDeadlinePipe(t)

	ctx := &flipDeadlineContext{Context: context.Background()}
	result := make(chan struct {
		value any
		err   error
	}, 1)
	go func() {
		value, err, _ := exec.requestOnceOnConnection(
			ctx, nativeOpPing, 0, map[string]any{}, 0, clientConn, true,
		)
		result <- struct {
			value any
			err   error
		}{value: value, err: err}
	}()

	request, err := readNativeRequestFrame(bufio.NewReader(serverConn))
	if err != nil {
		t.Fatal(err)
	}
	ctx.expire()
	if err := writeNativeTestResponse(bufio.NewWriter(serverConn), request, nativeStatusOK, []byte("late")); err != nil {
		t.Fatal(err)
	}
	response := <-result
	if !errors.Is(response.err, context.DeadlineExceeded) {
		t.Fatalf("late native response error = %v, want context deadline", response.err)
	}
	if response.value != nil {
		t.Fatalf("late native response value = %#v, want nil", response.value)
	}

	secondResult := make(chan struct {
		value any
		err   error
	}, 1)
	go func() {
		value, err, _ := exec.requestOnceOnConnection(
			context.Background(), nativeOpPing, 0, map[string]any{}, 0, clientConn, true,
		)
		secondResult <- struct {
			value any
			err   error
		}{value: value, err: err}
	}()
	secondRequest, err := readNativeRequestFrame(bufio.NewReader(serverConn))
	if err != nil {
		t.Fatal(err)
	}
	if err := writeNativeTestResponse(bufio.NewWriter(serverConn), secondRequest, nativeStatusOK, []byte("reused")); err != nil {
		t.Fatal(err)
	}
	response = <-secondResult
	if response.err != nil || string(response.value.([]byte)) != "reused" {
		t.Fatalf("connection reuse after late native response = %#v, %v", response.value, response.err)
	}
}

func TestNativeDeadlineStopsBusyRetry(t *testing.T) {
	exec, _, serverConn := newNativeDeadlinePipe(t)
	ctx := &flipDeadlineContext{Context: context.Background()}
	result := make(chan error, 1)
	go func() {
		_, err := exec.requestWithoutSessionGateWithReplayPolicy(
			ctx, nativeOpPing, 0, map[string]any{}, 0, nativeRequestBudget{}, nativeReplayDefault,
		)
		result <- err
	}()

	request, err := readNativeRequestFrame(bufio.NewReader(serverConn))
	if err != nil {
		t.Fatal(err)
	}
	ctx.expire()
	if err := writeNativeTestResponse(bufio.NewWriter(serverConn), request, nativeStatusBusy, map[string]any{
		"retryable":     true,
		"safe_to_retry": true,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		var nativeErr NativeError
		if !errors.As(err, &nativeErr) || nativeErr.Status != nativeStatusBusy {
			t.Fatalf("busy response after deadline = %v, want original busy error without retry", err)
		}
	case <-time.After(time.Second):
		t.Fatal("native busy retry ignored the expired absolute deadline")
	}
}

func TestNativeDeadlineStopsReconnectBeforeDial(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan bool, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			accepted <- false
			return
		}
		_ = conn.Close()
		accepted <- true
	}()

	exec := NewNativeExecutor(listener.Addr().String(), WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0))
	defer func() { _ = exec.Close() }()
	ctx := &flipDeadlineContext{Context: context.Background()}
	ctx.expire()
	result := make(chan error, 1)
	go func() { result <- exec.ensureConnectedLocked(ctx) }()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expired reconnect error = %v, want context deadline", err)
		}
	case <-time.After(time.Second):
		_ = listener.Close()
		t.Fatal("expired reconnect did not stop before dialing")
	}
	_ = listener.Close()
	if dialed := <-accepted; dialed {
		t.Fatal("expired reconnect dialed a native endpoint")
	}
}

func newNativeDeadlinePipe(t *testing.T) (*NativeExecutor, net.Conn, net.Conn) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	exec := NewNativeExecutor(
		"unused", WithNativeTimeout(0), WithNativeHeartbeat(0, 0), WithNativeReconnect(0),
	)
	exec.mu.Lock()
	exec.conn = clientConn
	exec.reader = bufio.NewReader(clientConn)
	exec.writer = bufio.NewWriter(clientConn)
	exec.connectionDone = make(chan struct{})
	exec.mu.Unlock()
	readerDone := make(chan struct{})
	go func() {
		exec.readerLoop(clientConn, exec.reader)
		close(readerDone)
	}()
	t.Cleanup(func() {
		_ = exec.Close()
		_ = serverConn.Close()
		select {
		case <-readerDone:
		case <-time.After(time.Second):
			t.Error("native reader did not stop during cleanup")
		}
	})
	return exec, clientConn, serverConn
}
