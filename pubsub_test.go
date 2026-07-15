package ferricstore

import (
	"bufio"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNativePubSubReceivesMessages(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	errc := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errc <- err
			return
		}
		defer func() { _ = conn.Close() }()

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
	defer func() { _ = client.Close() }()
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

func TestPubSubReconnectsAndReplaysSubscriptions(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	errCh := make(chan error, 1)
	go func() {
		first, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		reader, writer := bufio.NewReader(first), bufio.NewWriter(first)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			errCh <- err
			return
		}
		subscribe, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, subscribe, nativeStatusOK, []any{"subscribe", "jobs", int64(1)}); err != nil {
			errCh <- err
			return
		}
		_ = first.Close()

		second, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = second.Close() }()
		reader, writer = bufio.NewReader(second), bufio.NewWriter(second)
		startup, err = readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			errCh <- err
			return
		}
		ping, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if ping.opcode != nativeOpPing {
			errCh <- errUnexpectedFrame(ping)
			return
		}
		if err := writeNativeTestResponse(writer, ping, nativeStatusOK, []byte("PONG")); err != nil {
			errCh <- err
			return
		}
		replay, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		value, _, err := decodeNativeValue(replay.body)
		if err != nil {
			errCh <- err
			return
		}
		payload := value.(map[string]any)
		if replay.opcode != nativeOpCommandExec || asString(payload["command"]) != "SUBSCRIBE" {
			errCh <- errUnexpectedFrame(replay)
			return
		}
		if err := writeNativeTestResponse(writer, replay, nativeStatusOK, []any{"subscribe", "jobs", int64(1)}); err != nil {
			errCh <- err
			return
		}
		errCh <- writeNativeTestResponse(writer, nativeFrame{opcode: nativeOpEvent, requestID: 0}, nativeStatusOK, []any{"message", "jobs", "after-reconnect"})
	}()

	pubsub := NewPubSub(listener.Addr().String(), WithNativeTimeout(500*time.Millisecond), WithNativeHeartbeat(0, 0))
	defer func() { _ = pubsub.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := pubsub.Subscribe(ctx, "jobs"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		pubsub.exec.mu.Lock()
		disconnected := pubsub.exec.conn == nil
		pubsub.exec.mu.Unlock()
		if disconnected {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("native reader did not observe the closed subscription connection")
		}
		time.Sleep(time.Millisecond)
	}
	if got, err := pubsub.exec.command(ctx, "PING"); err != nil || asString(got) != "PONG" {
		t.Fatalf("ordinary reconnecting command = %#v, %v", got, err)
	}
	message, err := pubsub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if message.Kind != "message" || message.Channel != "jobs" || asString(message.Payload) != "after-reconnect" {
		t.Fatalf("unexpected replayed subscription message: %#v", message)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestPubSubReplaysExistingSubscriptionsBeforeNewSubscriptionAfterReconnect(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	errCh := make(chan error, 1)
	go func() {
		first, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		reader, writer := bufio.NewReader(first), bufio.NewWriter(first)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			errCh <- err
			return
		}
		subscribe, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, subscribe, nativeStatusOK, []any{"subscribe", "jobs", int64(1)}); err != nil {
			errCh <- err
			return
		}
		_ = first.Close()

		second, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = second.Close() }()
		reader, writer = bufio.NewReader(second), bufio.NewWriter(second)
		startup, err = readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			errCh <- err
			return
		}

		replay, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		replayedChannel, err := nativeTestCommandFirstArg(replay)
		if err != nil {
			errCh <- err
			return
		}
		if replayedChannel != "jobs" {
			_ = writeNativeTestResponse(writer, replay, nativeStatusOK, []any{"subscribe", replayedChannel, int64(1)})
			errCh <- errUnexpectedValue("replayed subscription", replayedChannel)
			return
		}
		if err := writeNativeTestResponse(writer, replay, nativeStatusOK, []any{"subscribe", "jobs", int64(1)}); err != nil {
			errCh <- err
			return
		}

		newSubscription, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		newChannel, err := nativeTestCommandFirstArg(newSubscription)
		if err != nil {
			errCh <- err
			return
		}
		if newChannel != "alerts" {
			errCh <- errUnexpectedValue("new subscription", newChannel)
			return
		}
		errCh <- writeNativeTestResponse(writer, newSubscription, nativeStatusOK, []any{"subscribe", "alerts", int64(2)})
	}()

	pubsub := NewPubSub(listener.Addr().String(), WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(1))
	defer func() { _ = pubsub.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := pubsub.Subscribe(ctx, "jobs"); err != nil {
		t.Fatal(err)
	}
	if _, err := pubsub.Subscribe(ctx, "alerts"); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestPubSubReplaysDesiredStateWhenSubscriptionCommandReconnects(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	firstReady := make(chan struct{})
	closeFirst := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		first, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		reader, writer := bufio.NewReader(first), bufio.NewWriter(first)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			errCh <- err
			return
		}
		subscribe, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, subscribe, nativeStatusOK, []any{"subscribe", "jobs", int64(1)}); err != nil {
			errCh <- err
			return
		}
		close(firstReady)
		<-closeFirst
		_ = first.Close()

		second, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = second.Close() }()
		reader, writer = bufio.NewReader(second), bufio.NewWriter(second)
		startup, err = readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			errCh <- err
			return
		}
		retried, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		channel, err := nativeTestCommandFirstArg(retried)
		if err != nil || channel != "alerts" {
			errCh <- errUnexpectedValue("retried subscription", channel)
			return
		}
		if err := writeNativeTestResponse(writer, retried, nativeStatusOK, []any{"subscribe", "alerts", int64(1)}); err != nil {
			errCh <- err
			return
		}

		_ = second.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		replay, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		args, err := nativeTestCommandArgs(replay)
		if err != nil {
			errCh <- err
			return
		}
		got := map[string]bool{}
		for _, arg := range args {
			got[asString(arg)] = true
		}
		if !got["jobs"] || !got["alerts"] || len(got) != 2 {
			errCh <- errUnexpectedValue("replayed desired subscriptions", args)
			return
		}
		errCh <- writeNativeTestResponse(writer, replay, nativeStatusOK, []any{"subscribe", "alerts", int64(2)})
	}()

	pubsub := NewPubSub(listener.Addr().String(), WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(1))
	defer func() { _ = pubsub.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := pubsub.Subscribe(ctx, "jobs"); err != nil {
		t.Fatal(err)
	}
	<-firstReady
	pubsub.exec.writeMu.Lock()
	ackResult := make(chan struct {
		ack PubSubMessage
		err error
	}, 1)
	go func() {
		ack, err := pubsub.Subscribe(ctx, "alerts")
		ackResult <- struct {
			ack PubSubMessage
			err error
		}{ack: ack, err: err}
	}()
	deadline := time.Now().Add(time.Second)
	for {
		pubsub.exec.mu.Lock()
		pending := len(pubsub.exec.pending)
		pubsub.exec.mu.Unlock()
		if pending > 0 {
			break
		}
		if time.Now().After(deadline) {
			pubsub.exec.writeMu.Unlock()
			t.Fatal("subscription command did not reach the blocked write")
		}
		time.Sleep(time.Millisecond)
	}
	close(closeFirst)
	for {
		pubsub.exec.mu.Lock()
		disconnected := pubsub.exec.conn == nil
		pubsub.exec.mu.Unlock()
		if disconnected {
			break
		}
		if time.Now().After(deadline) {
			pubsub.exec.writeMu.Unlock()
			t.Fatal("native reader did not observe the forced disconnect")
		}
		time.Sleep(time.Millisecond)
	}
	pubsub.exec.writeMu.Unlock()
	result := <-ackResult
	ack, err := result.ack, result.err
	if err != nil {
		t.Fatal(err)
	}
	if ack.Count != 2 {
		t.Fatalf("subscription count after reconnect replay = %d, want 2", ack.Count)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func nativeTestCommandFirstArg(frame nativeFrame) (string, error) {
	args, err := nativeTestCommandArgs(frame)
	if err != nil {
		return "", err
	}
	return asString(args[0]), nil
}

func nativeTestCommandArgs(frame nativeFrame) ([]any, error) {
	value, rest, err := decodeNativeValue(frame.body)
	if err != nil || len(rest) != 0 {
		return nil, errUnexpectedValue("native command payload", value)
	}
	payload, err := nativeMap(value)
	if err != nil {
		return nil, err
	}
	args, ok := payload["args"].([]any)
	if !ok || len(args) == 0 {
		return nil, errUnexpectedValue("native command args", payload["args"])
	}
	return args, nil
}

func TestNativeSubscribeFlowWakeUsesMultiplexedEvents(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	errc := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errc <- err
			return
		}
		defer func() { _ = conn.Close() }()

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
	defer func() { _ = client.Close() }()
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

func TestPubSubSubscribeRequiresAtLeastOneTarget(t *testing.T) {
	pubsub := NewPubSub("unused", WithNativeHeartbeat(0, 0))
	defer func() { _ = pubsub.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for name, subscribe := range map[string]func(context.Context) error{
		"channels": func(ctx context.Context) error {
			_, err := pubsub.Subscribe(ctx)
			return err
		},
		"patterns": func(ctx context.Context) error {
			_, err := pubsub.PSubscribe(ctx)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := subscribe(ctx); err == nil || !strings.Contains(err.Error(), "at least one") {
				t.Fatalf("empty subscription error = %v; want local target validation", err)
			}
		})
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
	defer func() { _ = listener.Close() }()

	errc := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errc <- err
			return
		}
		defer func() { _ = conn.Close() }()

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
	defer func() { _ = client.Close() }()

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
