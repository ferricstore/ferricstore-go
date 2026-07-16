package ferricstore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestNativeExecutorPipelineRejectsEmptyCommandWithoutPanic(t *testing.T) {
	exec := NewNativeExecutor("127.0.0.1:1")

	_, err := exec.Pipeline(context.Background(), [][]any{{}})

	if err == nil {
		t.Fatal("expected empty pipeline command to fail")
	}
}

func TestNativePipelineRejectsConnectionLocalStateMutations(t *testing.T) {
	exec := NewNativeExecutor("127.0.0.1:1")
	defer func() { _ = exec.Close() }()
	for _, command := range [][]any{
		{"CLIENT", "SETNAME", "pipeline-name"},
		{"WINDOW_UPDATE", "MAX_INFLIGHT_PER_CONNECTION", 1},
		{"AUTH", "secret"},
	} {
		if _, err := exec.Pipeline(context.Background(), [][]any{command}); err == nil || !strings.Contains(err.Error(), "connection-local") {
			t.Fatalf("pipeline command %#v error = %v", command, err)
		}
	}
}

func TestNativeResponseChunkCumulativeLimit(t *testing.T) {
	body, err := appendNativeResponseChunk([]byte("1234"), []byte("5"), 5)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "12345" {
		t.Fatalf("unexpected chunk body: %q", body)
	}
	if _, err := appendNativeResponseChunk(body, []byte("6"), 5); err == nil {
		t.Fatal("expected cumulative chunk size limit error")
	}
}

func TestNativeResponseAssemblerBoundsChunkFrames(t *testing.T) {
	assembler := newNativeResponseAssembler(1024, 2)
	frame := nativeFrame{flags: nativeFlagMoreChunks, laneID: 1, opcode: nativeOpGet, requestID: 7, body: []byte{0}}
	if response, err := assembler.add(frame); err != nil || response != nil {
		t.Fatalf("first chunk = %#v, %v", response, err)
	}
	if response, err := assembler.add(frame); err != nil || response != nil {
		t.Fatalf("second chunk = %#v, %v", response, err)
	}
	if _, err := assembler.add(frame); err == nil {
		t.Fatal("chunk assembler accepted more than its frame bound")
	}
}

func TestNativeResponseAssemblerTransfersOwnedBinaryWithoutExtraCopy(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 64*1024)
	encoded, err := encodeNativeValue(payload)
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 2, 2+len(encoded))
	binary.BigEndian.PutUint16(body, nativeStatusOK)
	body = append(body, encoded...)
	frame := nativeFrame{laneID: 1, opcode: nativeOpGet, requestID: 1, body: body}
	assembler := newNativeResponseAssembler(nativeMaxFrameBytes, nativeMaxResponseChunkFrames)

	allocs := testing.AllocsPerRun(100, func() {
		response, err := assembler.add(frame)
		if err != nil {
			panic(err)
		}
		benchmarkNativeResponseSink = response
	})
	if allocs > 2 {
		t.Fatalf("single-frame binary decode allocated %.1f times, want at most 2", allocs)
	}

	decoded := benchmarkNativeResponseSink.value.([]byte)
	if !bytes.Equal(decoded, payload) {
		t.Fatal("decoded binary changed")
	}
	payloadOffset := len(body) - len(payload)
	if len(decoded) > 0 && &decoded[0] != &body[payloadOffset] {
		t.Fatal("decoded binary was copied instead of taking ownership of the frame buffer")
	}
	if cap(decoded) != len(decoded) {
		t.Fatalf("decoded binary capacity = %d, want %d to protect adjacent fields", cap(decoded), len(decoded))
	}
}

func TestNativeMultiplexingAllowsInterleavedChunkedResponses(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	firstRead := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			errCh <- err
			return
		}
		first, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		close(firstRead)
		second, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		encoded, err := encodeNativeValue([]byte("first-value"))
		if err != nil {
			errCh <- err
			return
		}
		body := append([]byte{0, 0}, encoded...)
		split := len(body) / 2
		if err := writeNativeFrameBody(writer, first, nativeFlagMoreChunks, body[:split]); err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, second, nativeStatusOK, []byte("second-value")); err != nil {
			errCh <- err
			return
		}
		errCh <- writeNativeFrameBody(writer, first, 0, body[split:])
	}()

	exec := NewNativeExecutor(listener.Addr().String(), WithNativeHeartbeat(0, 0), WithNativeTimeout(time.Second))
	defer func() { _ = exec.Close() }()
	type result struct {
		value any
		err   error
	}
	firstResult := make(chan result, 1)
	go func() {
		value, err := exec.Do(context.Background(), "GET", "first")
		firstResult <- result{value: value, err: err}
	}()
	<-firstRead
	secondResult := make(chan result, 1)
	go func() {
		value, err := exec.Do(context.Background(), "GET", "second")
		secondResult <- result{value: value, err: err}
	}()
	first := <-firstResult
	second := <-secondResult
	if first.err != nil || asString(first.value) != "first-value" {
		t.Fatalf("first interleaved response = %#v, %v", first.value, first.err)
	}
	if second.err != nil || asString(second.value) != "second-value" {
		t.Fatalf("second interleaved response = %#v, %v", second.value, second.err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestNativeDecodeRejectsHugeDeclaredCounts(t *testing.T) {
	huge := uint32(nativeMaxContainerItems + 1)

	array := []byte{5, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(array[1:5], huge)
	if _, _, err := decodeNativeValue(array); err == nil {
		t.Fatal("expected huge native array count to fail")
	}

	mapping := []byte{6, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(mapping[1:5], huge)
	if _, _, err := decodeNativeValue(mapping); err == nil {
		t.Fatal("expected huge native map count to fail")
	}

	mget := []byte{nativeCompactKVMGet, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(mget[1:5], huge)
	if _, err := decodeNativeCompactKVMGet(mget); err == nil {
		t.Fatal("expected huge compact MGET count to fail")
	}

	pipeline := []byte{nativeCompactPipelineResponse, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(pipeline[1:5], huge)
	if _, err := decodeNativeCompactPipelineResponse(pipeline); err == nil {
		t.Fatal("expected huge compact pipeline count to fail")
	}
}

func TestNativeDeliverEventDropsWhenBufferFull(t *testing.T) {
	exec := &NativeExecutor{events: make(chan nativeQueuedEvent, 1)}

	exec.deliverEvent("first")
	exec.deliverEvent("second")

	if dropped := exec.DroppedEvents(); dropped != 1 {
		t.Fatalf("expected one dropped event, got %d", dropped)
	}
	if got := exec.consumeEvent(<-exec.events); got != "first" {
		t.Fatalf("unexpected delivered event: %#v", got)
	}
}

func TestNewClientFromURLUsesNativeScheme(t *testing.T) {
	client, err := NewClientFromURL("ferric://alice:secret@localhost:7000?timeout=5s")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	exec, ok := client.exec.(*NativeExecutor)
	if !ok {
		t.Fatalf("expected native executor, got %T", client.exec)
	}
	if exec.opts.Addr != "localhost:7000" {
		t.Fatalf("unexpected address: %s", exec.opts.Addr)
	}
	if exec.opts.Username != "alice" || exec.opts.Password != "secret" {
		t.Fatalf("unexpected credentials: %#v", exec.opts)
	}
	if exec.opts.Timeout != 5*time.Second {
		t.Fatalf("unexpected timeout: %s", exec.opts.Timeout)
	}
	if exec.opts.HeartbeatInterval != 30*time.Second || exec.opts.HeartbeatTimeout != 30*time.Second {
		t.Fatalf("unexpected heartbeat defaults: interval=%s timeout=%s", exec.opts.HeartbeatInterval, exec.opts.HeartbeatTimeout)
	}
	if exec.opts.ReconnectMaxRetries != 1 {
		t.Fatalf("unexpected reconnect default: %d", exec.opts.ReconnectMaxRetries)
	}
}

func TestNativeOptionsCanDisableHeartbeat(t *testing.T) {
	exec := NewNativeExecutor("127.0.0.1:6388", WithNativeHeartbeat(0, 0))
	if exec.opts.HeartbeatInterval != 0 || exec.opts.HeartbeatTimeout != 0 {
		t.Fatalf("unexpected heartbeat override: interval=%s timeout=%s", exec.opts.HeartbeatInterval, exec.opts.HeartbeatTimeout)
	}
}

func TestNativeOptionsCanDisableReconnect(t *testing.T) {
	exec := NewNativeExecutor("127.0.0.1:6388", WithNativeReconnect(0))
	if exec.opts.ReconnectMaxRetries != 0 {
		t.Fatalf("unexpected reconnect override: %d", exec.opts.ReconnectMaxRetries)
	}
}

func TestNativeStaleReaderDoesNotFailNewConnectionPendingRequests(t *testing.T) {
	oldClient, oldServer := net.Pipe()
	defer func() { _ = oldClient.Close() }()
	defer func() { _ = oldServer.Close() }()
	newClient, newServer := net.Pipe()
	defer func() { _ = newServer.Close() }()

	pending := make(chan nativeResponse, 1)
	exec := &NativeExecutor{
		conn: newClient,
		pending: map[uint64]*nativePendingRequest{
			1: {responseCh: pending},
		},
	}

	if exec.closeConnAndFailPendingIfCurrent(oldClient, io.ErrClosedPipe) {
		t.Fatal("stale reader failed current connection")
	}
	select {
	case response := <-pending:
		t.Fatalf("stale reader failed pending request: %#v", response)
	default:
	}
	if exec.conn != newClient {
		t.Fatal("stale reader changed current connection")
	}

	if !exec.closeConnAndFailPendingIfCurrent(newClient, io.ErrClosedPipe) {
		t.Fatal("current reader did not close current connection")
	}
	select {
	case response := <-pending:
		if response.err == nil {
			t.Fatalf("expected pending request error: %#v", response)
		}
	default:
		t.Fatal("current reader did not fail pending request")
	}
	if exec.conn != nil {
		t.Fatal("current reader did not clear connection")
	}
}

func TestNativeExecutorCommandExecWire(t *testing.T) {
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
		errc <- serveNativeWireTest(conn)
	}()

	client := NewClient(listener.Addr().String())
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := client.Ping(ctx, "hello")
	if err != nil {
		t.Fatal(err)
	}
	if got != "PONG" {
		t.Fatalf("expected PONG, got %q", got)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestNativeExecutorReconnectsAfterFailedStartup(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	errc := make(chan error, 1)
	go func() {
		first, err := listener.Accept()
		if err != nil {
			errc <- err
			return
		}
		_, _ = readNativeRequestFrame(bufio.NewReader(first))
		_ = first.Close()

		second, err := listener.Accept()
		if err != nil {
			errc <- err
			return
		}
		defer func() { _ = second.Close() }()
		errc <- serveNativeWireTest(second)
	}()

	client := NewClient(listener.Addr().String())
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := client.Ping(ctx, "hello")
	if err != nil {
		t.Fatal(err)
	}
	if got != "PONG" {
		t.Fatalf("expected PONG, got %q", got)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestNativeGoAwayDrainsPendingRequestAndReconnects(t *testing.T) {
	const goAwayOpcode uint16 = 0x000A

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
		reader := bufio.NewReader(first)
		writer := bufio.NewWriter(first)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			_ = first.Close()
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			_ = first.Close()
			errCh <- err
			return
		}
		request, err := readNativeRequestFrame(reader)
		if err != nil {
			_ = first.Close()
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, nativeFrame{opcode: goAwayOpcode}, nativeStatusOK, map[string]any{"reason": "maintenance"}); err != nil {
			_ = first.Close()
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, request, nativeStatusOK, []byte("PONG")); err != nil {
			_ = first.Close()
			errCh <- err
			return
		}

		_ = first.SetReadDeadline(time.Now().Add(2 * time.Second))
		if unexpected, err := readNativeRequestFrame(reader); err == nil {
			_ = first.Close()
			errCh <- fmt.Errorf("received request on GOAWAY connection: opcode=%#x", unexpected.opcode)
			return
		}
		_ = first.Close()

		second, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = second.Close() }()
		secondReader := bufio.NewReader(second)
		secondWriter := bufio.NewWriter(second)
		startup, err = readNativeRequestFrame(secondReader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(secondWriter, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			errCh <- err
			return
		}
		request, err = readNativeRequestFrame(secondReader)
		if err != nil {
			errCh <- err
			return
		}
		errCh <- writeNativeTestResponse(secondWriter, request, nativeStatusOK, []byte("PONG"))
	}()

	exec := NewNativeExecutor(
		listener.Addr().String(),
		WithNativeHeartbeat(0, 0),
		WithNativeTimeout(time.Second),
	)
	defer func() { _ = exec.Close() }()
	client := NewClientWithExecutor(exec)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if got, err := client.Ping(ctx, "before-goaway"); err != nil || got != "PONG" {
		t.Fatalf("in-flight request during GOAWAY = %q, %v; want PONG", got, err)
	}
	event, err := newPubSub(exec, false).NextEvent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if event.Opcode != goAwayOpcode || event.Name != "GOAWAY" || asString(event.Payload["reason"]) != "maintenance" {
		t.Fatalf("GOAWAY event = %#v", event)
	}

	deadline := time.Now().Add(time.Second)
	for {
		exec.mu.Lock()
		drained := exec.conn == nil
		exec.mu.Unlock()
		if drained {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("GOAWAY connection did not drain")
		}
		time.Sleep(time.Millisecond)
	}

	if got, err := client.Ping(ctx, "after-goaway"); err != nil || got != "PONG" {
		t.Fatalf("request after GOAWAY = %q, %v; want reconnect and PONG", got, err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestNativeExecutorDoesNotReconnectAfterRequestWriteStarted(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	accepted := make(chan struct{}, 2)
	errc := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errc <- err
			return
		}
		accepted <- struct{}{}
		reader := bufio.NewReader(conn)
		writer := bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			_ = conn.Close()
			errc <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			_ = conn.Close()
			errc <- err
			return
		}
		if _, err := readNativeRequestFrame(reader); err != nil {
			_ = conn.Close()
			errc <- err
			return
		}
		_ = conn.Close()
		errc <- nil
	}()

	client := NewClient(listener.Addr().String())
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Ping(ctx, "hello")
	if err == nil {
		t.Fatal("expected closed connection error")
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if got := len(accepted); got != 1 {
		t.Fatalf("expected one accepted connection, got %d", got)
	}
}

func TestNativeCommandExecEncodesComplexRawArgs(t *testing.T) {
	command, err := buildNativeCommand([]any{"CUSTOM.MAP", "TARGET", map[string]any{"type": "email"}})
	if err != nil {
		t.Fatal(err)
	}
	body, err := encodeNativeValue(command.payload)
	if err != nil {
		t.Fatal(err)
	}
	value, rest, err := decodeNativeValue(body)
	if err != nil || len(rest) != 0 {
		t.Fatalf("decode encoded command: rest %x, err %v", rest, err)
	}
	payload := value.(map[string]any)
	args := payload["args"].([]any)
	if got := asString(args[1]); got != `{"type":"email"}` {
		t.Fatalf("expected JSON encoded complex arg, got %#v", args[1])
	}
}

func TestNativeCommandExecPreservesRequestContextNamedArguments(t *testing.T) {
	tests := []struct {
		name string
		args []any
	}{
		{
			name: "implicit command exec",
			args: []any{"HSET", "hash", "REQUEST_CONTEXT", "value"},
		},
		{
			name: "explicit command exec",
			args: []any{"COMMAND_EXEC", "HSET", "hash", "REQUEST_CONTEXT", map[string]any{"field": "value"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command, err := buildNativeCommand(test.args)
			if err != nil {
				t.Fatal(err)
			}
			payload := command.payload.(map[string]any)
			want := test.args[1:]
			if asString(test.args[0]) == "COMMAND_EXEC" {
				want = test.args[2:]
			}
			encodedWant, err := nativeCommandArgs(want)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(payload["args"], encodedWant) {
				t.Fatalf("command args = %#v; want %#v", payload["args"], encodedWant)
			}
			if _, exists := payload["request_context"]; exists {
				t.Fatalf("raw command data was interpreted as request context: %#v", payload)
			}
		})
	}
}

func TestNativeCommandExecCarriesRequestContext(t *testing.T) {
	command, err := buildNativeCommand(commandWithRequestContext(
		"INVOCATION.CREATE",
		[]any{"send-email", "{}"},
		&RequestContext{
			Subject: "proxy",
			Tenant:  "acme",
			Scopes:  []string{"invocation:create:*", "tenant:acme"},
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	payload := command.payload.(map[string]any)
	if payload["command"] != "INVOCATION.CREATE" {
		t.Fatalf("unexpected command-exec command: %#v", payload)
	}
	if !reflect.DeepEqual(payload["args"], []any{"send-email", "{}"}) {
		t.Fatalf("unexpected command-exec args: %#v", payload["args"])
	}
	requestContext := payload["request_context"].(map[string]any)
	if requestContext["subject"] != "proxy" || requestContext["tenant"] != "acme" {
		t.Fatalf("unexpected request context: %#v", requestContext)
	}
	if !reflect.DeepEqual(requestContext["scopes"], []string{"invocation:create:*", "tenant:acme"}) {
		t.Fatalf("unexpected request context scopes: %#v", requestContext["scopes"])
	}
}

func TestNativeExplicitCommandExecCarriesRequestContext(t *testing.T) {
	args := append([]any{"COMMAND_EXEC"}, commandWithRequestContext(
		"INVOCATION.CREATE",
		[]any{"send-email", "{}"},
		&RequestContext{Subject: "proxy", Scopes: []string{"invocation:create:*", "invocation:create:*"}},
	)...)
	command, err := buildNativeCommand(args)
	if err != nil {
		t.Fatal(err)
	}
	payload := command.payload.(map[string]any)
	if payload["command"] != "INVOCATION.CREATE" {
		t.Fatalf("unexpected explicit command-exec command: %#v", payload)
	}
	if !reflect.DeepEqual(payload["args"], []any{"send-email", "{}"}) {
		t.Fatalf("unexpected explicit command-exec args: %#v", payload["args"])
	}
	requestContext := payload["request_context"].(map[string]any)
	if !reflect.DeepEqual(requestContext, map[string]any{"subject": "proxy", "scopes": []string{"invocation:create:*"}}) {
		t.Fatalf("unexpected explicit request context: %#v", requestContext)
	}
}

func TestNativeScheduleCreateBuildsDedicatedPayload(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.SCHEDULE.CREATE", "sched-1",
		"KIND", "one_shot",
		"AT_MS", int64(123),
		"TARGET", map[string]any{"type": "email"},
		"OVERWRITE", "true",
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpFlowScheduleCreate || command.laneID != 1 {
		t.Fatalf("unexpected schedule command routing: %#v", command)
	}
	payload := command.payload.(map[string]any)
	if payload["id"] != "sched-1" || payload["kind"] != "one_shot" || payload["at_ms"] != int64(123) || payload["overwrite"] != true {
		t.Fatalf("unexpected schedule payload: %#v", payload)
	}
	target := payload["target"].(map[string]any)
	if target["type"] != "email" {
		t.Fatalf("unexpected schedule target: %#v", target)
	}
}

func TestNativeGovernanceBuildsDedicatedPayload(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.EFFECT.RESERVE", "flow-1",
		"EFFECT_KEY", "email",
		"EFFECT_TYPE", "email.send",
		"PARTITION", "tenant:1",
		"FENCING", int64(7),
		"NOW", int64(123),
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpFlowEffectReserve || command.laneID != 1 {
		t.Fatalf("unexpected governance command routing: %#v", command)
	}
	payload := command.payload.(map[string]any)
	if payload["id"] != "flow-1" || payload["effect_key"] != "email" || payload["effect_type"] != "email.send" ||
		payload["partition_key"] != "tenant:1" || payload["fencing_token"] != int64(7) || payload["now_ms"] != int64(123) {
		t.Fatalf("unexpected governance payload: %#v", payload)
	}
}

func serveNativeWireTest(conn net.Conn) error {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	startup, err := readNativeRequestFrame(reader)
	if err != nil {
		return err
	}
	if startup.opcode != nativeOpStartup || startup.laneID != 0 {
		return errUnexpectedFrame(startup)
	}
	payload, _, err := decodeNativeValue(startup.body)
	if err != nil {
		return err
	}
	startupMap := payload.(map[string]any)
	if asString(startupMap["driver_name"]) != "ferricstore-go" {
		return errUnexpectedValue("driver_name", startupMap["driver_name"])
	}
	if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
		return err
	}

	command, err := readNativeRequestFrame(reader)
	if err != nil {
		return err
	}
	if command.opcode != nativeOpPing || command.laneID != 0 {
		return errUnexpectedFrame(command)
	}
	payload, _, err = decodeNativeValue(command.body)
	if err != nil {
		return err
	}
	commandMap := payload.(map[string]any)
	if asString(commandMap["message"]) != "hello" {
		return errUnexpectedValue("message", commandMap["message"])
	}
	return writeNativeTestResponse(writer, command, nativeStatusOK, []byte("PONG"))
}
