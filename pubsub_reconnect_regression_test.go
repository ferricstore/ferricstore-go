package ferricstore

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestPubSubReplaysBeforeRetryingWrittenSubscriptionAfterEOF(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
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
		if _, err := readExpectedNativeSubscription(firstReader, "alerts"); err != nil {
			errCh <- err
			return
		}
		_ = first.Close()

		second, secondReader, secondWriter, err := acceptNativePubSubTestConnection(listener)
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = second.Close() }()
		replay, err := readExpectedNativeSubscription(secondReader, "jobs")
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(secondWriter, replay, nativeStatusOK, []any{"subscribe", "jobs", int64(1)}); err != nil {
			errCh <- err
			return
		}
		alerts, err := readExpectedNativeSubscription(secondReader, "alerts")
		if err != nil {
			errCh <- err
			return
		}
		errCh <- writeNativeTestResponse(secondWriter, alerts, nativeStatusOK, []any{"subscribe", "alerts", int64(2)})
	}()

	pubsub := NewPubSub(listener.Addr().String(), WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(1))
	defer func() { _ = pubsub.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := pubsub.Subscribe(ctx, "jobs"); err != nil {
		t.Fatal(err)
	}
	message, err := pubsub.Subscribe(ctx, "alerts")
	if err != nil {
		t.Fatal(err)
	}
	if message.Count != 2 {
		t.Fatalf("subscription count = %d, want 2", message.Count)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func acceptNativePubSubTestConnection(listener net.Listener) (net.Conn, *bufio.Reader, *bufio.Writer, error) {
	conn, err := listener.Accept()
	if err != nil {
		return nil, nil, nil, err
	}
	reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
	startup, err := readNativeRequestFrame(reader)
	if err != nil {
		_ = conn.Close()
		return nil, nil, nil, err
	}
	if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
		_ = conn.Close()
		return nil, nil, nil, err
	}
	return conn, reader, writer, nil
}

func readExpectedNativeSubscription(reader *bufio.Reader, want string) (nativeFrame, error) {
	frame, err := readNativeRequestFrame(reader)
	if err != nil {
		return nativeFrame{}, err
	}
	channel, err := nativeTestCommandFirstArg(frame)
	if err != nil {
		return nativeFrame{}, err
	}
	if channel != want {
		return nativeFrame{}, fmt.Errorf("subscription channel = %q, want %q", channel, want)
	}
	return frame, nil
}

func TestEventReplayPreservesPartiallyUnsubscribedEvents(t *testing.T) {
	p := newPubSub(&NativeExecutor{}, false)
	p.trackEventSubscription(nativeOpSubscribeEvents, map[string]any{
		"events": []any{"FLOW_WAKE", "TOPOLOGY_CHANGED"},
	})
	p.trackEventSubscription(nativeOpUnsubscribeEvents, map[string]any{
		"events": []any{"FLOW_WAKE"},
	})

	if len(p.eventReplays) != 1 {
		t.Fatalf("event replay count = %d, want 1", len(p.eventReplays))
	}
	got := stringList(p.eventReplays[0].payload["events"])
	if !reflect.DeepEqual(got, []string{"TOPOLOGY_CHANGED"}) {
		t.Fatalf("remaining replay events = %#v, want [TOPOLOGY_CHANGED]", got)
	}
}

func TestEventReplayDeduplicatesIdenticalSubscriptions(t *testing.T) {
	p := newPubSub(&NativeExecutor{}, false)
	payload := map[string]any{
		"events": []any{"FLOW_WAKE"},
		"flow_wake": map[string]any{
			"type": "email", "state": "queued", "limit": int64(25),
		},
	}
	for range 100 {
		p.trackEventSubscription(nativeOpSubscribeEvents, payload)
	}
	if len(p.eventReplays) != 1 {
		t.Fatalf("identical event subscriptions retained %d replay requests; want 1", len(p.eventReplays))
	}
}

func TestEventReplayMatchesServerNormalizedEventNames(t *testing.T) {
	p := newPubSub(&NativeExecutor{}, false)
	p.trackEventSubscription(nativeOpSubscribeEvents, map[string]any{
		"events": []any{"flow_wake"},
	})
	p.trackEventSubscription(nativeOpUnsubscribeEvents, map[string]any{
		"events": []any{"FLOW_WAKE"},
	})

	if len(p.eventReplays) != 0 {
		t.Fatalf("case-insensitive unsubscribe retained replay state: %#v", p.eventReplays)
	}
}

func TestEventReplayEmptyUnsubscribeMatchesServerNoop(t *testing.T) {
	p := newPubSub(&NativeExecutor{}, false)
	p.trackEventSubscription(nativeOpSubscribeEvents, map[string]any{
		"events": []any{"FLOW_WAKE", "TOPOLOGY_CHANGED"},
	})
	p.trackEventSubscription(nativeOpUnsubscribeEvents, map[string]any{
		"events": []any{},
	})

	if len(p.eventReplays) != 1 {
		t.Fatalf("empty server-side no-op removed replay state: %#v", p.eventReplays)
	}
	got := normalizedNativeEventNames(p.eventReplays[0].payload["events"])
	if !reflect.DeepEqual(got, []string{"FLOW_WAKE", "TOPOLOGY_CHANGED"}) {
		t.Fatalf("events after empty unsubscribe = %#v", got)
	}
}
