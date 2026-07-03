package ferricstore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestNativeValueCodecRoundTrip(t *testing.T) {
	input := map[string]any{
		"command": []byte("PING"),
		"args": []any{
			[]byte("hello"),
			int64(42),
			true,
			nil,
			[]any{[]byte("nested")},
			map[string]any{"field": []byte("value")},
		},
	}
	encoded, err := encodeNativeValue(input)
	if err != nil {
		t.Fatal(err)
	}
	decoded, rest, err := decodeNativeValue(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 0 {
		t.Fatalf("unexpected trailing bytes: %d", len(rest))
	}
	if !reflect.DeepEqual(decoded, input) {
		t.Fatalf("decoded mismatch:\nwant %#v\ngot  %#v", input, decoded)
	}
}

func TestNativeCompactResponseDecoders(t *testing.T) {
	okValue, err := decodeNativeCompactOKList([]byte{nativeCompactOKList, 0, 0, 0, 1})
	if err != nil {
		t.Fatal(err)
	}
	if asString(okValue) != "OK" {
		t.Fatalf("unexpected OK value: %#v", okValue)
	}
	if !isOK([]any{[]byte("ok")}) {
		t.Fatalf("expected lowercase compact OK list to be accepted")
	}
	manyOKValue, err := decodeNativeCompactOKList([]byte{nativeCompactOKList, 0, 0, 1, 244})
	if err != nil {
		t.Fatal(err)
	}
	if asString(manyOKValue) != "OK" {
		t.Fatalf("unexpected many OK value: %#v", manyOKValue)
	}

	getBody := append([]byte{nativeCompactKVGet, 1, 0, 0, 0, 5}, []byte("value")...)
	getValue, err := decodeNativeCompactKVGet(getBody)
	if err != nil {
		t.Fatal(err)
	}
	if asString(getValue) != "value" {
		t.Fatalf("unexpected GET value: %#v", getValue)
	}

	var mget bytes.Buffer
	mget.WriteByte(nativeCompactKVMGet)
	_ = binary.Write(&mget, binary.BigEndian, uint32(2))
	mget.WriteByte(1)
	_ = binary.Write(&mget, binary.BigEndian, uint32(1))
	mget.WriteByte('a')
	mget.WriteByte(0)
	mgetValue, err := decodeNativeCompactKVMGet(mget.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(mgetValue) != 2 || asString(mgetValue[0]) != "a" || mgetValue[1] != nil {
		t.Fatalf("unexpected MGET value: %#v", mgetValue)
	}

	var claim bytes.Buffer
	claim.WriteByte(nativeCompactFlowClaimJobs)
	_ = binary.Write(&claim, binary.BigEndian, uint32(1))
	writeCompactBinary(&claim, []byte("flow-1"))
	writeCompactOptionalBinary(&claim, []byte("partition-1"))
	writeCompactBinary(&claim, []byte("lease-1"))
	_ = binary.Write(&claim, binary.BigEndian, uint64(7))
	attrs, err := encodeNativeValue(map[string]any{"tenant": []byte("acme")})
	if err != nil {
		t.Fatal(err)
	}
	claim.Write(attrs)
	claimValue, err := decodeNativeCompactClaimJobs(claim.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := claimedItemsFromNative(claimValue)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("unexpected claim count: %#v", claimed)
	}
	if claimed[0].ID != "flow-1" || claimed[0].PartitionKey != "partition-1" || claimed[0].LeaseToken != "lease-1" || claimed[0].FencingToken != 7 {
		t.Fatalf("unexpected compact claim item: %#v", claimed[0])
	}
	if asString(claimed[0].Attributes["tenant"]) != "acme" {
		t.Fatalf("unexpected compact claim attrs: %#v", claimed[0].Attributes)
	}
}

func TestNativeFlowCompactCommandBuilders(t *testing.T) {
	now := nowMS()

	claim, err := buildNativeCommand([]any{
		"FLOW.CLAIM_DUE",
		"email",
		"STATE", "queued",
		"WORKER", "worker-1",
		"LEASE_MS", int64(30_000),
		"LIMIT", int64(500),
		"NOW", now,
		"PARTITIONS", int64(2), "p1", "p2",
		"RETURN", "JOBS_COMPACT_ATTRS",
	})
	if err != nil {
		t.Fatal(err)
	}
	if claim.opcode != nativeOpFlowClaimDue || claim.flags != nativeFlagCustomPayload {
		t.Fatalf("unexpected compact claim command: %#v", claim)
	}
	if body, ok := claim.payload.([]byte); !ok || len(body) == 0 || body[0] != nativeCompactFlowClaimDueRequest {
		t.Fatalf("unexpected compact claim payload: %#v", claim.payload)
	}

	create, err := buildNativeCommand([]any{
		"FLOW.CREATE_MANY",
		"MIXED",
		"TYPE", "email",
		"STATE", "queued",
		"NOW", int64(1),
		"RUN_AT", int64(1),
		"INDEPENDENT", true,
		"ITEMS",
		"flow-1", "p1", []byte("payload"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if create.opcode != nativeOpFlowCreateMany || create.flags != nativeFlagCustomPayload {
		t.Fatalf("unexpected compact create command: %#v", create)
	}
	if body, ok := create.payload.([]byte); !ok || len(body) == 0 || body[0] != nativeCompactFlowCreateManyMixedRequest {
		t.Fatalf("unexpected compact create payload: %#v", create.payload)
	}

	complete, err := buildNativeCommand([]any{
		"FLOW.COMPLETE_MANY",
		"MIXED",
		"NOW", int64(1),
		"INDEPENDENT", true,
		"ITEMS",
		"flow-1", "p1", "lease-1", int64(7),
	})
	if err != nil {
		t.Fatal(err)
	}
	if complete.opcode != nativeOpFlowCompleteMany || complete.flags != nativeFlagCustomPayload {
		t.Fatalf("unexpected compact complete command: %#v", complete)
	}
	if body, ok := complete.payload.([]byte); !ok || len(body) == 0 || body[0] != nativeCompactFlowCompleteManyOKRequest {
		t.Fatalf("unexpected compact complete payload: %#v", complete.payload)
	}
}

func TestNativeFlowCompleteManyFallsBackWhenResultIsPresent(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.COMPLETE_MANY",
		"MIXED",
		"NOW", int64(1),
		"INDEPENDENT", true,
		"RESULT", []byte("ok"),
		"ITEMS",
		"flow-1", "p1", "lease-1", int64(7),
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpCommandExec || command.flags != 0 {
		t.Fatalf("expected generic fallback for result-bearing complete_many, got %#v", command)
	}
}

func TestNativeExecutorPipelineRejectsEmptyCommandWithoutPanic(t *testing.T) {
	exec := NewNativeExecutor("127.0.0.1:1")

	_, err := exec.Pipeline(context.Background(), [][]any{{}})

	if err == nil {
		t.Fatal("expected empty pipeline command to fail")
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
	exec := &NativeExecutor{events: make(chan any, 1)}

	exec.deliverEvent("first")
	exec.deliverEvent("second")

	if dropped := exec.DroppedEvents(); dropped != 1 {
		t.Fatalf("expected one dropped event, got %d", dropped)
	}
	if got := <-exec.events; got != "first" {
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
}

func TestNativeOptionsCanDisableHeartbeat(t *testing.T) {
	exec := NewNativeExecutor("127.0.0.1:6388", WithNativeHeartbeat(0, 0))
	if exec.opts.HeartbeatInterval != 0 || exec.opts.HeartbeatTimeout != 0 {
		t.Fatalf("unexpected heartbeat override: interval=%s timeout=%s", exec.opts.HeartbeatInterval, exec.opts.HeartbeatTimeout)
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
		pending: map[uint64]chan nativeResponse{
			1: pending,
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

func TestNativeCommandExecEncodesComplexRawArgs(t *testing.T) {
	command, err := buildNativeCommand([]any{"CUSTOM.MAP", "TARGET", map[string]any{"type": "email"}})
	if err != nil {
		t.Fatal(err)
	}
	payload := command.payload.(map[string]any)
	args := payload["args"].([]any)
	if got := asString(args[1]); got != `{"type":"email"}` {
		t.Fatalf("expected JSON encoded complex arg, got %#v", args[1])
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

func TestNativeExecutorPipelineWire(t *testing.T) {
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
		if frame.opcode != nativeOpPipeline || frame.laneID != 1 {
			errc <- errUnexpectedFrame(frame)
			return
		}
		payload, _, err := decodeNativeValue(frame.body)
		if err != nil {
			errc <- err
			return
		}
		m := payload.(map[string]any)
		commands := m["commands"].([]any)
		if len(commands) != 2 {
			errc <- errUnexpectedValue("commands", commands)
			return
		}
		first := commands[0].(map[string]any)
		if asInt64(first["opcode"]) != int64(nativeOpSet) {
			errc <- errUnexpectedValue("first opcode", first["opcode"])
			return
		}
		second := commands[1].(map[string]any)
		if asInt64(second["opcode"]) != int64(nativeOpGet) {
			errc <- errUnexpectedValue("second opcode", second["opcode"])
			return
		}
		errc <- writeNativeTestResponse(writer, frame, nativeStatusOK, []any{
			[]any{"ok", []byte("OK")},
			[]any{"ok", []byte("value")},
		})
	}()

	client := NewClient(listener.Addr().String())
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := client.Pipeline(ctx, [][]any{
		{"SET", "k", []byte("value")},
		{"GET", "k"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || asString(got[1]) != "value" {
		t.Fatalf("unexpected pipeline result: %#v", got)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestNativeExecutorPipelineCompactSETWire(t *testing.T) {
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
		if frame.opcode != nativeOpPipeline || frame.laneID != 1 || frame.flags&nativeFlagCustomPayload == 0 {
			errc <- errUnexpectedFrame(frame)
			return
		}
		want := []byte{
			nativeCompactPipelineRequest, 0x81, 0, 0, 0, 2,
			0, 0, 0, 2, 'k', '1', 0, 0, 0, 2, 'v', '1',
			0, 0, 0, 2, 'k', '2', 0, 0, 0, 2, 'v', '2',
		}
		if !bytes.Equal(frame.body, want) {
			errc <- errUnexpectedValue("compact SET pipeline body", frame.body)
			return
		}
		errc <- writeNativeRawTestResponse(writer, frame, nativeStatusOK, []byte{nativeCompactOKList, 0, 0, 0, 2})
	}()

	client := NewClient(listener.Addr().String())
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := client.Pipeline(ctx, [][]any{
		{"SET", "k1", []byte("v1")},
		{"SET", "k2", []byte("v2")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !isOK(got[0]) || !isOK(got[1]) {
		t.Fatalf("unexpected compact SET pipeline result: %#v", got)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestNativeExecutorPipelineCompactGETWire(t *testing.T) {
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
		if frame.opcode != nativeOpPipeline || frame.laneID != 1 || frame.flags&nativeFlagCustomPayload == 0 {
			errc <- errUnexpectedFrame(frame)
			return
		}
		want := []byte{
			nativeCompactPipelineRequest, 0x82, 0, 0, 0, 2,
			0, 0, 0, 2, 'k', '1',
			0, 0, 0, 2, 'k', '2',
		}
		if !bytes.Equal(frame.body, want) {
			errc <- errUnexpectedValue("compact GET pipeline body", frame.body)
			return
		}
		var mget bytes.Buffer
		mget.WriteByte(nativeCompactKVMGet)
		_ = binary.Write(&mget, binary.BigEndian, uint32(2))
		mget.WriteByte(1)
		_ = binary.Write(&mget, binary.BigEndian, uint32(2))
		mget.WriteString("v1")
		mget.WriteByte(1)
		_ = binary.Write(&mget, binary.BigEndian, uint32(2))
		mget.WriteString("v2")
		errc <- writeNativeRawTestResponse(writer, frame, nativeStatusOK, mget.Bytes())
	}()

	client := NewClient(listener.Addr().String())
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := client.Pipeline(ctx, [][]any{
		{"GET", "k1"},
		{"GET", "k2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || asString(got[0]) != "v1" || asString(got[1]) != "v2" {
		t.Fatalf("unexpected compact GET pipeline result: %#v", got)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestNativeExecutorPipelineCompactResponseWire(t *testing.T) {
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
		if frame.opcode != nativeOpPipeline || frame.laneID != 1 {
			errc <- errUnexpectedFrame(frame)
			return
		}
		var compact bytes.Buffer
		compact.WriteByte(nativeCompactPipelineResponse)
		_ = binary.Write(&compact, binary.BigEndian, uint32(2))
		compact.Write([]byte{0, 1})
		_ = binary.Write(&compact, binary.BigEndian, uint32(2))
		compact.WriteString("OK")
		compact.Write([]byte{0, 1})
		_ = binary.Write(&compact, binary.BigEndian, uint32(5))
		compact.WriteString("value")
		errc <- writeNativeRawTestResponse(writer, frame, nativeStatusOK, compact.Bytes())
	}()

	client := NewClient(listener.Addr().String())
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := client.Pipeline(ctx, [][]any{
		{"SET", "k", []byte("value")},
		{"GET", "k"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || asString(got[0]) != "OK" || asString(got[1]) != "value" {
		t.Fatalf("unexpected compact pipeline result: %#v", got)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func readNativeRequestFrame(reader *bufio.Reader) (nativeFrame, error) {
	header := make([]byte, nativeHeaderLen)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nativeFrame{}, err
	}
	if string(header[0:4]) != nativeMagic || header[4] != nativeRequestVersion {
		return nativeFrame{}, errUnexpectedValue("request header", append([]byte(nil), header[:5]...))
	}
	bodyLen := binary.BigEndian.Uint32(header[20:24])
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(reader, body); err != nil {
		return nativeFrame{}, err
	}
	return nativeFrame{
		flags:     header[5],
		laneID:    binary.BigEndian.Uint32(header[6:10]),
		opcode:    binary.BigEndian.Uint16(header[10:12]),
		requestID: binary.BigEndian.Uint64(header[12:20]),
		body:      body,
	}, nil
}

func writeNativeTestResponse(writer *bufio.Writer, request nativeFrame, status uint16, value any) error {
	valueBody, err := encodeNativeValue(value)
	if err != nil {
		return err
	}
	return writeNativeRawTestResponse(writer, request, status, valueBody)
}

func writeNativeRawTestResponse(writer *bufio.Writer, request nativeFrame, status uint16, valueBody []byte) error {
	body := bytes.NewBuffer(make([]byte, 0, 2+len(valueBody)))
	var statusBytes [2]byte
	binary.BigEndian.PutUint16(statusBytes[:], status)
	body.Write(statusBytes[:])
	body.Write(valueBody)

	header := make([]byte, nativeHeaderLen)
	copy(header[0:4], nativeMagic)
	header[4] = nativeResponseVersion
	binary.BigEndian.PutUint32(header[6:10], request.laneID)
	binary.BigEndian.PutUint16(header[10:12], request.opcode)
	binary.BigEndian.PutUint64(header[12:20], request.requestID)
	binary.BigEndian.PutUint32(header[20:24], uint32(body.Len()))

	if _, err := writer.Write(header); err != nil {
		return err
	}
	if _, err := writer.Write(body.Bytes()); err != nil {
		return err
	}
	return writer.Flush()
}

func errUnexpectedFrame(frame nativeFrame) error {
	return errUnexpectedValue("frame", map[string]any{
		"lane_id": frame.laneID,
		"opcode":  frame.opcode,
	})
}

func errUnexpectedValue(name string, value any) error {
	return NativeError{Status: 1, Value: map[string]any{"message": name + " unexpected: " + asString(value)}}
}
