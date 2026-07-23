package ferricstore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func nativePipelineHelloForTest() map[string]any {
	return nativeHelloForTest()
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
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, nativePipelineHelloForTest()); err != nil {
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

func TestNativePipelineFallsBackCustomFlowPayloadsToCommandExec(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
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
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, nativePipelineHelloForTest()); err != nil {
			errCh <- err
			return
		}
		frame, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if frame.opcode != nativeOpPipeline || frame.flags&nativeFlagCustomPayload != 0 {
			errCh <- errUnexpectedFrame(frame)
			return
		}
		value, rest, err := decodeNativeValue(frame.body)
		if err != nil || len(rest) != 0 {
			errCh <- fmt.Errorf("decode typed pipeline: rest=%d err=%w", len(rest), err)
			return
		}
		payload, err := nativeMap(value)
		if err != nil {
			errCh <- err
			return
		}
		items, ok := payload["commands"].([]any)
		if !ok || len(items) != 2 {
			errCh <- errUnexpectedValue("pipeline commands", payload["commands"])
			return
		}
		for _, raw := range items {
			item, err := nativeMap(raw)
			if err != nil || asInt64(item["opcode"]) != int64(nativeOpCommandExec) {
				errCh <- errUnexpectedValue("pipeline command opcode", item["opcode"])
				return
			}
			body, err := nativeMap(item["body"])
			if err != nil || asString(body["command"]) != "FLOW.CLAIM_DUE" {
				errCh <- errUnexpectedValue("pipeline command body", item["body"])
				return
			}
		}
		errCh <- writeNativeTestResponse(writer, frame, nativeStatusOK, []any{
			[]any{"ok", []any{}},
			[]any{"ok", []any{}},
		})
	}()

	client := NewClient(listener.Addr().String(), WithNativeOptions(WithNativeHeartbeat(0, 0), WithNativeTimeout(time.Second)))
	defer func() { _ = client.Close() }()
	commands := [][]any{
		{"FLOW.CLAIM_DUE", "orders", "WORKER", "worker-1", "LEASE_MS", 1_000, "LIMIT", 1},
		{"FLOW.CLAIM_DUE", "orders", "WORKER", "worker-2", "LEASE_MS", 1_000, "LIMIT", 1},
	}
	got, err := client.Pipeline(context.Background(), commands)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("custom Flow pipeline results = %#v", got)
	}
	if err := <-errCh; err != nil {
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
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, nativePipelineHelloForTest()); err != nil {
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
		errc <- writeNativeCompactTestResponse(writer, frame, nativeStatusOK, []byte{nativeCompactOKList, 0, 0, 0, 2})
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

type countingNativeStringer struct{ calls *atomic.Int64 }

func (s countingNativeStringer) String() string {
	s.calls.Add(1)
	return "value"
}

func TestNativePipelineEncodesTypedPayloadOnce(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	errCh := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader := bufio.NewReader(conn)
		writer := bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			errCh <- err
			return
		}
		request, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		errCh <- writeNativeTestResponse(writer, request, nativeStatusOK, []any{[]any{"ok", "OK"}})
	}()

	var calls atomic.Int64
	client := NewClient(listener.Addr().String())
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := client.Pipeline(ctx, [][]any{{"SET", "key", countingNativeStringer{calls: &calls}}}); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("typed pipeline payload encoded %d times, want once", got)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestCompactEncodersFallBackForTypedValues(t *testing.T) {
	for _, command := range [][]any{
		{"SET", "key", nil},
		{"SET", "key", int64(42)},
		{"SET", "key", map[string]any{"nested": true}},
		{"SET", int64(42), "value"},
	} {
		if _, ok, err := compactSetPipelinePayload([][]any{command}); err != nil {
			t.Fatalf("compactSetPipelinePayload(%#v) returned error: %v", command, err)
		} else if ok {
			t.Fatalf("compactSetPipelinePayload(%#v) changed a typed value instead of falling back", command)
		}
	}
	if _, ok := compactBytes(map[string]any{"nested": true}); ok {
		t.Fatal("compact Flow encoder accepted a structured RawCodec payload")
	}
}

func TestCompactPipelineEncodingHonorsLimitBeforeAllocation(t *testing.T) {
	_, ok, err := compactPipelinePayloadWithLimit([][]any{
		{"SET", "key", bytes.Repeat([]byte{'x'}, 64)},
	}, 32)
	var limitErr nativeEncodeLimitError
	if !ok || !errors.As(err, &limitErr) {
		t.Fatalf("bounded compact encoding = ok %t, error %v; want encoding limit", ok, err)
	}
}

func TestNativeEncoderRejectsExcessiveNesting(t *testing.T) {
	value := any("leaf")
	for range nativeMaxDecodeDepth + 2 {
		value = []any{value}
	}
	if _, err := encodeNativeValue(value); err == nil {
		t.Fatal("native request encoder accepted excessive nesting")
	}
}

func TestNativeEncoderRejectsCyclesAndOversizedBodies(t *testing.T) {
	cycle := map[string]any{}
	cycle["self"] = cycle
	if _, err := encodeNativeValue(cycle); err == nil {
		t.Fatal("native request encoder accepted a reference cycle")
	}
	if _, err := encodeNativeValueWithLimit([]byte("payload"), 8); err == nil {
		t.Fatal("native request encoder exceeded its byte budget")
	}
}

func TestNativeEncoderRejectsOverBudgetContainerBeforeWritingItems(t *testing.T) {
	buf := &nativeEncodeBuffer{limit: 1024}
	state := &nativeEncodeState{remaining: 2, visiting: make(map[nativeEncodeVisit]struct{})}
	err := writeNativeValue(buf, []any{nil, nil}, state, 0)
	if err == nil {
		t.Fatal("expected aggregate item budget error")
	}
	if got := buf.Len(); got != 0 {
		t.Fatalf("encoder wrote %d bytes before rejecting an over-budget container", got)
	}
}

func TestIndefiniteBlockingRequestRetainsWriteDeadline(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = serverConn.Close() }()

	exec := NewNativeExecutor("unused", WithNativeTimeout(25*time.Millisecond))
	exec.mu.Lock()
	exec.conn = clientConn
	exec.writer = bufio.NewWriter(clientConn)
	exec.maxRequestFrameBytes = nativeDefaultRequestFrameBytes
	exec.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		_, err := exec.writeRequest(
			context.Background(), nativeOpCommandExec, 1, 1,
			map[string]any{"command": "BLPOP", "args": []any{"queue", int64(0)}},
			0, clientConn, false,
		)
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected stalled write to time out")
		}
		if !errors.Is(err, os.ErrDeadlineExceeded) && !errors.Is(err, context.DeadlineExceeded) {
			var netErr net.Error
			if !errors.As(err, &netErr) || !netErr.Timeout() {
				t.Fatalf("stalled write error = %v, want timeout", err)
			}
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("indefinite blocking command disabled the transport write deadline")
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
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, nativePipelineHelloForTest()); err != nil {
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
		errc <- writeNativeCompactTestResponse(writer, frame, nativeStatusOK, mget.Bytes())
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

func TestNativeReconnectReplaysSuccessfulConnectionState(t *testing.T) {
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
		setName, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, setName, nativeStatusOK, []byte("OK")); err != nil {
			errCh <- err
			return
		}
		window, err := readNativeRequestFrame(reader)
		if err != nil || window.opcode != nativeOpWindowUpdate {
			errCh <- errUnexpectedFrame(window)
			return
		}
		limits := map[string]any{"limits": map[string]any{
			"max_inflight_per_connection": int64(3), "max_inflight_per_lane": int64(2),
		}}
		if err := writeNativeTestResponse(writer, window, nativeStatusOK, limits); err != nil {
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
		value, _, err := decodeNativeValue(startup.body)
		if err != nil {
			errCh <- err
			return
		}
		startupPayload, err := nativeMap(value)
		if err != nil || asString(startupPayload["client_name"]) != "worker-client" {
			errCh <- errUnexpectedValue("reconnected client_name", startupPayload["client_name"])
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			errCh <- err
			return
		}
		window, err = readNativeRequestFrame(reader)
		if err != nil || window.opcode != nativeOpWindowUpdate {
			errCh <- errUnexpectedFrame(window)
			return
		}
		value, _, err = decodeNativeValue(window.body)
		if err != nil {
			errCh <- err
			return
		}
		windowPayload, err := nativeMap(value)
		if err != nil || asInt64(windowPayload["max_inflight_per_connection"]) != 3 || asInt64(windowPayload["max_inflight_per_lane"]) != 2 {
			errCh <- errUnexpectedValue("replayed WINDOW_UPDATE", windowPayload)
			return
		}
		if err := writeNativeTestResponse(writer, window, nativeStatusOK, limits); err != nil {
			errCh <- err
			return
		}
		get, err := readNativeRequestFrame(reader)
		if err != nil || get.opcode != nativeOpGet {
			errCh <- errUnexpectedFrame(get)
			return
		}
		errCh <- writeNativeTestResponse(writer, get, nativeStatusOK, []byte("value"))
	}()

	exec := NewNativeExecutor(listener.Addr().String(), WithNativeHeartbeat(0, 0), WithNativeTimeout(time.Second))
	defer func() { _ = exec.Close() }()
	client := NewClientWithExecutor(exec)
	if err := client.ClientSetName(context.Background(), "worker-client"); err != nil {
		t.Fatal(err)
	}
	if _, err := exec.Do(context.Background(), "WINDOW_UPDATE",
		"MAX_INFLIGHT_PER_CONNECTION", 3,
		"MAX_INFLIGHT_PER_LANE", 2,
	); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		exec.mu.Lock()
		disconnected := exec.conn == nil
		exec.mu.Unlock()
		if disconnected {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first connection did not close")
		}
		time.Sleep(time.Millisecond)
	}
	if value, err := client.KV().Get(context.Background(), "key"); err != nil || asString(value) != "value" {
		t.Fatalf("GET after connection-state replay = %#v, %v", value, err)
	}
	if err := <-errCh; err != nil {
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
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, nativePipelineHelloForTest()); err != nil {
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
		errc <- writeNativeCompactTestResponse(writer, frame, nativeStatusOK, compact.Bytes())
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
