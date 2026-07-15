package ferricstore

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestPubSubReplayFinishesOnOneConnectionGeneration(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	errCh := make(chan error, 1)
	go func() {
		first, firstReader, firstWriter, err := acceptNativePubSubTestConnection(listener)
		if err != nil {
			errCh <- err
			return
		}
		jobs, err := readExpectedNativeSubscription(firstReader, "jobs")
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(firstWriter, jobs, nativeStatusOK, []any{"subscribe", "jobs", int64(1)}); err != nil {
			errCh <- err
			return
		}
		if tcp, ok := first.(*net.TCPConn); ok {
			_ = tcp.SetLinger(0)
		}
		_ = first.Close()

		second, secondReader, secondWriter, err := acceptNativePubSubTestConnection(listener)
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = second.Close() }()

		patterns, err := readExpectedNativeSubscription(secondReader, "jobs:*")
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(secondWriter, patterns, nativeStatusOK, []any{"psubscribe", "jobs:*", int64(1)}); err != nil {
			errCh <- err
			return
		}

		jobs, err = readExpectedNativeSubscription(secondReader, "jobs")
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(secondWriter, jobs, nativeStatusOK, []any{"subscribe", "jobs", int64(2)}); err != nil {
			errCh <- err
			return
		}
		patterns, err = readExpectedNativeSubscription(secondReader, "jobs:*")
		if err != nil {
			errCh <- err
			return
		}
		errCh <- writeNativeTestResponse(secondWriter, patterns, nativeStatusOK, []any{"psubscribe", "jobs:*", int64(2)})
	}()

	pubsub := NewPubSub(
		listener.Addr().String(),
		WithNativeTimeout(time.Second),
		WithNativeHeartbeat(0, 0),
		WithNativeReconnect(1),
	)
	t.Cleanup(func() { _ = pubsub.Close() })
	pubsub.mu.Lock()
	pubsub.channels = map[string]struct{}{"jobs": {}}
	pubsub.patterns = map[string]struct{}{"jobs:*": {}}
	pubsub.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := pubsub.reconnectAndReplay(ctx); err != nil {
		t.Fatal(err)
	}
	current := pubsub.exec.currentConnectionGeneration()
	pubsub.mu.Lock()
	replayed := pubsub.lastGeneration
	pubsub.mu.Unlock()
	if replayed != current {
		t.Fatalf("replay generation = %d, current connection generation = %d", replayed, current)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}
