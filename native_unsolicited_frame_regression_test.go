package ferricstore

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func TestNativeHandshakeReturnsConnectionLevelServerError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		if _, err := readNativeRequestFrame(reader); err != nil {
			serverErr <- err
			return
		}
		serverErr <- writeNativeTestResponse(writer, nativeFrame{}, 1, []byte("ERR TLS required"))
	}()

	client := NewClient(listener.Addr().String(), WithNativeOptions(
		WithNativeHeartbeat(0, 0),
		WithNativeReconnect(0),
		WithNativeTimeout(time.Second),
	))
	defer func() { _ = client.Close() }()
	_, err = client.Ping(context.Background())
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "tls required") {
		t.Fatalf("connection-level error = %v", err)
	}
	var nativeErr NativeError
	if !errors.As(err, &nativeErr) || nativeErr.Status != 1 {
		t.Fatalf("connection-level error type = %T, %v", err, err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestNativeReaderRejectsInvalidReservedRequestIDFrame(t *testing.T) {
	client, server := net.Pipe()
	exec := NewNativeExecutor("unused")
	exec.mu.Lock()
	exec.conn = client
	exec.reader = bufio.NewReader(client)
	exec.writer = bufio.NewWriter(client)
	exec.connectionDone = make(chan struct{})
	exec.pending = make(map[uint64]*nativePendingRequest)
	exec.mu.Unlock()

	done := make(chan struct{})
	go func() {
		exec.readerLoop(client, exec.reader)
		close(done)
	}()
	t.Cleanup(func() {
		_ = server.Close()
		_ = exec.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("native reader did not stop during cleanup")
		}
	})

	writer := bufio.NewWriter(server)
	err := writeNativeTestResponse(writer, nativeFrame{
		laneID: 1, opcode: nativeOpGet, requestID: 0,
	}, nativeStatusOK, []byte("invalid unsolicited response"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("native reader accepted a data-lane GET with reserved request_id 0")
	}
}
