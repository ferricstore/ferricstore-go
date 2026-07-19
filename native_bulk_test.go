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

func TestNativeBulkPayloadsPreserveWireShape(t *testing.T) {
	typedMSet, err := newNativeMSetCommand(
		[]string{"{bulk}:a", "{bulk}:b"},
		[]any{[]byte("one"), int64(2)},
	)
	if err != nil {
		t.Fatal(err)
	}
	typedMSetNX, err := newNativeMSetNXCommand(
		[]string{"{bulk}:a", "{bulk}:b"},
		[]any{[]byte("one"), int64(2)},
	)
	if err != nil {
		t.Fatal(err)
	}
	genericMGet, err := buildNativeCommand([]any{"MGET", "a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	genericMSet, err := buildNativeCommand([]any{
		"MSET", "{bulk}:a", []byte("one"), "{bulk}:b", int64(2),
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		command nativeCommand
		want    any
	}{
		{
			name:    "typed mget",
			command: newNativeMGetCommand([]string{"a", "b"}),
			want:    map[string]any{"keys": []any{[]byte("a"), []byte("b")}},
		},
		{
			name:    "generic mget",
			command: genericMGet,
			want:    map[string]any{"keys": []any{[]byte("a"), []byte("b")}},
		},
		{
			name:    "typed mset",
			command: typedMSet,
			want: map[string]any{"pairs": []any{
				[]any{[]byte("{bulk}:a"), []byte("one")},
				[]any{[]byte("{bulk}:b"), int64(2)},
			}},
		},
		{
			name:    "generic mset",
			command: genericMSet,
			want: map[string]any{"pairs": []any{
				[]any{[]byte("{bulk}:a"), []byte("one")},
				[]any{[]byte("{bulk}:b"), int64(2)},
			}},
		},
		{
			name:    "typed msetnx",
			command: typedMSetNX,
			want: map[string]any{
				"command": []byte("MSETNX"),
				"args": []any{
					[]byte("{bulk}:a"), []byte("one"),
					[]byte("{bulk}:b"), int64(2),
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := encodeNativeValue(tc.command.payload)
			if err != nil {
				t.Fatal(err)
			}
			decoded, rest, err := decodeNativeValue(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if len(rest) != 0 || !reflect.DeepEqual(decoded, tc.want) {
				t.Fatalf("decoded payload = %#v (rest %d); want %#v", decoded, len(rest), tc.want)
			}
		})
	}
}

func TestNativeBulkCommandsRejectEmptyArity(t *testing.T) {
	for _, command := range []string{"DEL", "MGET", "MSET"} {
		t.Run(command, func(t *testing.T) {
			if _, err := buildNativeCommand([]any{command}); err == nil {
				t.Fatalf("empty %s built a native command", command)
			}
		})
	}
}

func TestNativeMSetCommandConstructionHasBoundedAllocations(t *testing.T) {
	args := make([]any, 1, 201)
	args[0] = "MSET"
	for range 100 {
		args = append(args, "key", []byte("value"))
	}
	var command nativeCommand
	allocs := testing.AllocsPerRun(100, func() {
		var err error
		command, err = buildNativeCommand(args)
		if err != nil {
			panic(err)
		}
	})
	if command.opcode != nativeOpMSet {
		t.Fatalf("MSET opcode = %d", command.opcode)
	}
	if allocs > 3 {
		t.Fatalf("MSET(100) command allocations = %.0f; want <= 3", allocs)
	}
}

func TestNativeKeyValueBulkMethodsUseTypedWireCommands(t *testing.T) {
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
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			errCh <- err
			return
		}

		mget, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if mget.opcode != nativeOpMGet {
			errCh <- &NativeError{Value: "MGET did not use typed opcode"}
			return
		}
		if err := writeNativeTestResponse(writer, mget, nativeStatusOK, []any{[]byte("one"), []byte("two")}); err != nil {
			errCh <- err
			return
		}

		mset, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if mset.opcode != nativeOpMSet {
			errCh <- &NativeError{Value: "MSET did not use typed opcode"}
			return
		}
		if err := writeNativeTestResponse(writer, mset, nativeStatusOK, []byte("OK")); err != nil {
			errCh <- err
			return
		}

		msetnx, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if msetnx.opcode != nativeOpCommandExec {
			errCh <- &NativeError{Value: "MSETNX did not use typed command-exec payload"}
			return
		}
		payload, rest, err := decodeNativeValue(msetnx.body)
		if err != nil || len(rest) != 0 {
			errCh <- fmt.Errorf("decode MSETNX payload: %w", err)
			return
		}
		mapping, err := nativeMap(payload)
		if err != nil || asString(mapping["command"]) != "MSETNX" {
			errCh <- fmt.Errorf("unexpected MSETNX payload: %#v (%v)", payload, err)
			return
		}
		errCh <- writeNativeTestResponse(writer, msetnx, nativeStatusOK, int64(1))
	}()

	exec := NewNativeExecutor(listener.Addr().String(), WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0))
	defer func() { _ = exec.Close() }()
	store := NewClientWithExecutor(exec).KV()
	values, err := store.MGet(context.Background(), "one", "two")
	if err != nil || !reflect.DeepEqual(values, []any{[]byte("one"), []byte("two")}) {
		t.Fatalf("typed MGET = %#v, %v", values, err)
	}
	if err := store.MSet(context.Background(), map[string]any{"one": []byte("value")}); err != nil {
		t.Fatal(err)
	}
	stored, err := store.MSetNX(context.Background(), map[string]any{"one": []byte("value")})
	if err != nil || !stored {
		t.Fatalf("typed MSETNX = %t, %v", stored, err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}
