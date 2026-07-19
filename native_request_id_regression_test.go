package ferricstore

import (
	"bufio"
	"context"
	"errors"
	"net"
	"testing"
)

func TestNativeRequestIDsSkipReservedEventIDOnWrap(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = serverConn.Close() }()

	exec := &NativeExecutor{
		conn:   clientConn,
		writer: bufio.NewWriter(clientConn),
		nextID: ^uint64(0),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err, _ := exec.requestOnceOnConnection(
			ctx, nativeOpPing, 0, map[string]any{}, 0, clientConn, true,
		)
		done <- err
	}()

	frame, err := readNativeRequestFrame(bufio.NewReader(serverConn))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wrapped request error = %v", err)
	}
	if frame.requestID == 0 {
		t.Fatal("native request used ID 0 reserved for server events")
	}
}

func TestNativeHandshakeRequestIDsSkipReservedEventIDOnWrap(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = serverConn.Close() }()

	exec := &NativeExecutor{nextID: ^uint64(0)}
	clientReader := bufio.NewReader(clientConn)
	clientWriter := bufio.NewWriter(clientConn)
	done := make(chan error, 1)
	go func() {
		_, err := exec.nativeHandshakeRequest(
			context.Background(), 0, clientConn, clientReader, clientWriter,
			nativeUnauthenticatedFrameBytes, nativeOpHello, map[string]any{},
		)
		done <- err
	}()

	request, err := readNativeRequestFrame(bufio.NewReader(serverConn))
	if err != nil {
		t.Fatal(err)
	}
	serverWriter := bufio.NewWriter(serverConn)
	if err := writeNativeTestResponse(serverWriter, request, nativeStatusOK, map[string]any{"ready": true}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if request.requestID == 0 {
		t.Fatal("native handshake used ID 0 reserved for server events")
	}
}

func TestNativeRequestIDsDoNotCollideWithPendingRequestsAfterWrap(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = serverConn.Close() }()

	original := &nativePendingRequest{responseCh: make(chan nativeResponse, 1)}
	exec := &NativeExecutor{
		conn:    clientConn,
		writer:  bufio.NewWriter(clientConn),
		nextID:  ^uint64(0),
		pending: map[uint64]*nativePendingRequest{1: original},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err, _ := exec.requestOnceOnConnection(
			ctx, nativeOpPing, 0, map[string]any{}, 0, clientConn, true,
		)
		done <- err
	}()

	request, err := readNativeRequestFrame(bufio.NewReader(serverConn))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wrapped request error = %v", err)
	}
	if request.requestID == 1 {
		t.Fatal("wrapped native request reused a live pending request ID")
	}
	exec.mu.Lock()
	retained := exec.pending[1]
	exec.mu.Unlock()
	if retained != original {
		t.Fatal("wrapped native request replaced the existing pending request")
	}
}
