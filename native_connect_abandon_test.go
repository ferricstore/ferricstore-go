package ferricstore

import (
	"bufio"
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestNativeAbandonedUnlimitedConnectAttemptIsReplaced(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	startupRead := make(chan struct{})
	serverErr := make(chan error, 1)
	go func() {
		first, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = first.Close() }()
		firstReader := bufio.NewReader(first)
		if _, err := readNativeRequestFrame(firstReader); err != nil {
			serverErr <- err
			return
		}
		close(startupRead)
		_ = first.SetReadDeadline(time.Now().Add(750 * time.Millisecond))
		if _, err := firstReader.ReadByte(); err == nil {
			serverErr <- errors.New("abandoned startup connection remained readable")
			return
		} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
			serverErr <- errors.New("abandoned startup connection was not closed")
			return
		}
		_ = first.Close()

		second, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = second.Close() }()
		reader, writer := bufio.NewReader(second), bufio.NewWriter(second)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			serverErr <- err
			return
		}
		request, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		serverErr <- writeNativeTestResponse(writer, request, nativeStatusOK, []byte("PONG"))
	}()

	exec := NewNativeExecutor(
		listener.Addr().String(),
		WithNativeTimeout(0),
		WithNativeHeartbeat(0, 0),
		WithNativeReconnect(0),
	)
	defer func() { _ = exec.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	firstResult := make(chan error, 1)
	go func() {
		_, err := exec.Do(ctx, "PING")
		firstResult <- err
	}()
	select {
	case <-startupRead:
	case <-time.After(time.Second):
		t.Fatal("server did not receive first STARTUP")
	}
	if err := <-firstResult; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first request error = %v; want deadline exceeded", err)
	}

	retryCtx, retryCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer retryCancel()
	value, err := exec.Do(retryCtx, "PING")
	if err != nil || asString(value) != "PONG" {
		t.Fatalf("replacement request = %#v, %v; want PONG", value, err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestNativeAbandonedEventConnectAttemptIsReplaced(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	startupRead := make(chan struct{})
	serverErr := make(chan error, 1)
	go func() {
		first, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		firstReader := bufio.NewReader(first)
		if _, err := readNativeRequestFrame(firstReader); err != nil {
			serverErr <- err
			return
		}
		close(startupRead)
		_ = first.SetReadDeadline(time.Now().Add(750 * time.Millisecond))
		if _, err := firstReader.ReadByte(); err == nil {
			serverErr <- errors.New("abandoned event startup connection remained readable")
			return
		} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
			serverErr <- errors.New("abandoned event startup connection was not closed")
			return
		}
		_ = first.Close()

		second, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = second.Close() }()
		reader, writer := bufio.NewReader(second), bufio.NewWriter(second)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			serverErr <- err
			return
		}
		request, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		serverErr <- writeNativeTestResponse(writer, request, nativeStatusOK, []byte("PONG"))
	}()

	exec := NewNativeExecutor(
		listener.Addr().String(),
		WithNativeTimeout(0),
		WithNativeHeartbeat(0, 0),
		WithNativeReconnect(0),
	)
	defer func() { _ = exec.Close() }()
	exec.enableEventDelivery()
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	firstResult := make(chan error, 1)
	go func() {
		_, err := exec.nextEvent(ctx)
		firstResult <- err
	}()
	select {
	case <-startupRead:
	case <-time.After(time.Second):
		t.Fatal("server did not receive event STARTUP")
	}
	if err := <-firstResult; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("event wait error = %v; want deadline exceeded", err)
	}

	retryCtx, retryCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer retryCancel()
	value, err := exec.Do(retryCtx, "PING")
	if err != nil || asString(value) != "PONG" {
		t.Fatalf("replacement request = %#v, %v; want PONG", value, err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}
