package ferricstore

import (
	"bufio"
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

func TestNativePubSubReceivesMessages(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	errc := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errc <- err
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		writer := bufio.NewWriter(conn)

		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			errc <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			errc <- err
			return
		}

		subscribe, err := readNativeRequestFrame(reader)
		if err != nil {
			errc <- err
			return
		}
		if err := writeNativeTestResponse(writer, nativeFrame{opcode: nativeOpEvent, requestID: 0}, nativeStatusOK, []any{[]byte("message"), []byte("jobs"), []byte("hello")}); err != nil {
			errc <- err
			return
		}
		if err := writeNativeTestResponse(writer, subscribe, nativeStatusOK, []any{[]byte("subscribe"), []byte("jobs"), int64(1)}); err != nil {
			errc <- err
			return
		}
		errc <- nil
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	client := NewClient(listener.Addr().String())
	defer client.Close()
	pubsub, err := client.OpenPubSub()
	if err != nil {
		t.Fatal(err)
	}

	ack, err := pubsub.Subscribe(ctx, "jobs")
	if err != nil {
		t.Fatal(err)
	}
	if ack.Kind != "subscribe" || ack.Channel != "jobs" || ack.Count != 1 {
		t.Fatalf("unexpected subscribe ack: %#v", ack)
	}

	message, err := pubsub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if message.Kind != "message" || message.Channel != "jobs" || asString(message.Payload) != "hello" {
		t.Fatalf("unexpected message: %#v", message)
	}

	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestNativeSubscribeFlowWakeUsesMultiplexedEvents(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	errc := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errc <- err
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		writer := bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			errc <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			errc <- err
			return
		}
		frame, err := readNativeRequestFrame(reader)
		if err != nil {
			errc <- err
			return
		}
		value, _, err := decodeNativeValue(frame.body)
		if err != nil {
			errc <- err
			return
		}
		payload, ok := value.(map[string]any)
		if !ok {
			errc <- errUnexpectedValue("subscribe payload", value)
			return
		}
		if frame.opcode != nativeOpSubscribeEvents || frame.laneID != 0 || asString(payload["events"].([]any)[0]) != "FLOW_WAKE" {
			errc <- errUnexpectedFrame(frame)
			return
		}
		flowWake := payload["flow_wake"].(map[string]any)
		if asString(flowWake["type"]) != "email" || asString(flowWake["state"]) != "queued" || asInt64(flowWake["priority"]) != 0 || asInt64(flowWake["limit"]) != 25 {
			errc <- errUnexpectedValue("flow_wake", flowWake)
			return
		}
		if err := writeNativeTestResponse(writer, frame, nativeStatusOK, map[string]any{
			"subscribed": []any{"FLOW_WAKE"},
			"supported":  []any{"FLOW_WAKE"},
		}); err != nil {
			errc <- err
			return
		}
		if err := writeNativeTestResponse(writer, nativeFrame{opcode: nativeOpEvent, requestID: 0}, nativeStatusOK, map[string]any{
			"event": "FLOW_WAKE",
			"payload": map[string]any{
				"type":   "email",
				"credit": int64(25),
				"reason": "ready",
			},
			"at_ms": int64(1234),
		}); err != nil {
			errc <- err
			return
		}
		errc <- nil
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client := NewClient(listener.Addr().String())
	defer client.Close()
	pubsub, err := client.OpenPubSub()
	if err != nil {
		t.Fatal(err)
	}

	sub, err := pubsub.SubscribeFlowWake(ctx, FlowWakeSubscriptionOptions{
		Type:  "email",
		State: "queued",
		Limit: Int(25),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sub.Subscribed) != 1 || sub.Subscribed[0] != "FLOW_WAKE" {
		t.Fatalf("unexpected subscription: %#v", sub)
	}

	event, err := pubsub.NextEvent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if event.Name != "FLOW_WAKE" || event.AtMS != 1234 || asInt64(event.Payload["credit"]) != 25 {
		t.Fatalf("unexpected event: %#v", event)
	}

	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestEventSubscriptionHelpersValidateOptions(t *testing.T) {
	pubsub := &PubSub{exec: &NativeExecutor{}}
	if _, err := pubsub.SubscribeFlowWake(context.Background(), FlowWakeSubscriptionOptions{
		Type:   "email",
		State:  "queued",
		States: []string{"ready"},
	}); err == nil {
		t.Fatal("expected state/states validation error")
	}
	if _, err := pubsub.SubscribeFlowWake(context.Background(), FlowWakeSubscriptionOptions{
		Type:          "email",
		PartitionKey:  "tenant-a",
		PartitionKeys: []string{"tenant-b"},
	}); err == nil {
		t.Fatal("expected partition validation error")
	}

	msg := pubSubMessageFromNative(map[string]any{
		"event": "PUBSUB_MESSAGE",
		"payload": map[string]any{
			"kind":    "pmessage",
			"pattern": "jobs:*",
			"channel": "jobs:1",
			"message": []byte("body"),
		},
	})
	if msg.Kind != "pmessage" || msg.Pattern != "jobs:*" || msg.Channel != "jobs:1" || asString(msg.Payload) != "body" {
		t.Fatalf("unexpected native pubsub message: %#v", msg)
	}

	if got := stringList([]string{"FLOW_WAKE", "PUBSUB_MESSAGE"}); len(got) != 2 || got[0] != "FLOW_WAKE" || got[1] != "PUBSUB_MESSAGE" {
		t.Fatalf("unexpected string subscription list: %#v", got)
	}
}

func TestClientOpenPubSubRequiresNativeExecutor(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{})

	if _, err := client.OpenPubSub(); err == nil {
		t.Fatal("expected error for non-native executor")
	}
}

func TestNativeExecutorMultiplexesConcurrentRequests(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	errc := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errc <- err
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		writer := bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			errc <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			errc <- err
			return
		}

		first, err := readNativeRequestFrame(reader)
		if err != nil {
			errc <- err
			return
		}
		second, err := readNativeRequestFrame(reader)
		if err != nil {
			errc <- err
			return
		}
		if err := writeNativeTestResponse(writer, second, nativeStatusOK, []byte("second")); err != nil {
			errc <- err
			return
		}
		if err := writeNativeTestResponse(writer, first, nativeStatusOK, []byte("first")); err != nil {
			errc <- err
			return
		}
		errc <- nil
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client := NewClient(listener.Addr().String())
	defer client.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := client.Ping(ctx); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestPubSubParsesPatternMessagesAndClosedState(t *testing.T) {
	pattern := pubSubMessageFromNative([]any{[]byte("pmessage"), []byte("jobs:*"), []byte("jobs:1"), []byte("payload")})
	if pattern.Kind != "pmessage" || pattern.Pattern != "jobs:*" || pattern.Channel != "jobs:1" || asString(pattern.Payload) != "payload" {
		t.Fatalf("unexpected pattern message: %#v", pattern)
	}

	ack := pubSubMessageFromNative([]any{[]byte("punsubscribe"), []byte("jobs:*"), int64(0)})
	if ack.Kind != "punsubscribe" || ack.Pattern != "jobs:*" || ack.Count != 0 {
		t.Fatalf("unexpected pattern ack: %#v", ack)
	}

	nestedAck := pubSubMessageFromNative([]any{[]any{[]byte("subscribe"), []byte("jobs"), int64(1)}})
	if nestedAck.Kind != "subscribe" || nestedAck.Channel != "jobs" || nestedAck.Count != 1 {
		t.Fatalf("unexpected nested subscribe ack: %#v", nestedAck)
	}

	closed := &PubSub{}
	if _, err := closed.Next(context.Background()); err == nil {
		t.Fatal("expected closed pubsub error")
	}
}
