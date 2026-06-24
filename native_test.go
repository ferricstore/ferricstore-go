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

func TestNewClientFromURLUsesNativeScheme(t *testing.T) {
	client, err := NewClientFromURL("ferric://alice:secret@localhost:7000?timeout=5s")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

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
}

func TestNativeExecutorCommandExecWire(t *testing.T) {
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
		errc <- serveNativeWireTest(conn)
	}()

	client := NewClient(listener.Addr().String())
	defer client.Close()

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
	if command.opcode != nativeOpCommandExec || command.laneID != 1 {
		return errUnexpectedFrame(command)
	}
	payload, _, err = decodeNativeValue(command.body)
	if err != nil {
		return err
	}
	commandMap := payload.(map[string]any)
	if asString(commandMap["command"]) != "PING" {
		return errUnexpectedValue("command", commandMap["command"])
	}
	args := commandMap["args"].([]any)
	if len(args) != 1 || asString(args[0]) != "hello" {
		return errUnexpectedValue("args", args)
	}
	return writeNativeTestResponse(writer, command, nativeStatusOK, []byte("PONG"))
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
