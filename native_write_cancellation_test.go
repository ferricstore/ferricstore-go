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

type signalingWriteConn struct {
	net.Conn
	started chan struct{}
	once    sync.Once
}

func (c *signalingWriteConn) Write(payload []byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	return c.Conn.Write(payload)
}

func TestNativeWriteCanceledWhileWaitingForEncodeAdmissionIsNotSent(t *testing.T) {
	testNativeCanceledQueuedWriteIsNotSent(t, func(exec *NativeExecutor) *contextMutex {
		return &exec.writeEncodeMu
	})
}

func TestNativeWriteCanceledWhileWaitingForTransportAdmissionIsNotSent(t *testing.T) {
	testNativeCanceledQueuedWriteIsNotSent(t, func(exec *NativeExecutor) *contextMutex {
		return &exec.writeMu
	})
}

func TestNativeWriteCancellationInterruptsBlockedFlush(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	conn := &signalingWriteConn{Conn: clientConn, started: make(chan struct{})}
	t.Cleanup(func() { _ = conn.Close() })
	t.Cleanup(func() { _ = serverConn.Close() })

	exec := NewNativeExecutor("unused", WithNativeTimeout(0))
	exec.mu.Lock()
	exec.conn = conn
	exec.writer = bufio.NewWriter(conn)
	exec.maxRequestFrameBytes = nativeDefaultRequestFrameBytes
	exec.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := exec.writeRequest(
			ctx, nativeOpGet, 1, 1, map[string]any{"key": "value"}, 0, conn, true,
		)
		done <- err
	}()
	select {
	case <-conn.started:
	case <-time.After(time.Second):
		t.Fatal("native write did not reach the blocked transport")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked write error = %v; want context.Canceled", err)
		}
	case <-time.After(100 * time.Millisecond):
		_ = conn.Close()
		<-done
		t.Fatal("canceled native write remained blocked in Flush")
	}
}

func testNativeCanceledQueuedWriteIsNotSent(t *testing.T, gate func(*NativeExecutor) *contextMutex) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close() })
	t.Cleanup(func() { _ = serverConn.Close() })

	exec := NewNativeExecutor("unused", WithNativeTimeout(time.Second))
	exec.mu.Lock()
	exec.conn = clientConn
	exec.writer = bufio.NewWriter(clientConn)
	exec.maxRequestFrameBytes = nativeDefaultRequestFrameBytes
	exec.mu.Unlock()

	lockedGate := gate(exec)
	lockedGate.Lock()
	locked := true
	t.Cleanup(func() {
		if locked {
			lockedGate.Unlock()
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := exec.writeRequest(
			ctx, nativeOpGet, 1, 1, map[string]any{"key": "value"}, 0, clientConn, true,
		)
		done <- err
	}()

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("queued write error = %v; want context.Canceled", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("canceled native write remained blocked on encode admission")
	}

	lockedGate.Unlock()
	locked = false
	if err := serverConn.SetReadDeadline(time.Now().Add(25 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if n, err := serverConn.Read(one[:]); n != 0 || err == nil {
		t.Fatalf("canceled queued write reached transport: bytes=%d err=%v", n, err)
	}
}
