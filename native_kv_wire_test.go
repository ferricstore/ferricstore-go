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

var (
	_ keyValueDelExecutor    = (*NativeExecutor)(nil)
	_ keyValueExistsExecutor = (*NativeExecutor)(nil)
	_ keyValueDelExecutor    = (*TopologyNativeExecutor)(nil)
	_ keyValueExistsExecutor = (*TopologyNativeExecutor)(nil)
)

func TestNativeTypedKeyCountPayloadsPreserveWireShape(t *testing.T) {
	tests := []struct {
		name    string
		command nativeCommand
		want    any
	}{
		{
			name:    "del",
			command: newNativeDelCommand([]string{"a", "b"}),
			want:    map[string]any{"keys": []any{[]byte("a"), []byte("b")}},
		},
		{
			name:    "exists",
			command: newNativeExistsCommand([]string{"a", "b"}),
			want: map[string]any{
				"command": []byte("EXISTS"),
				"args":    []any{[]byte("a"), []byte("b")},
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

func TestNativeKeyValueDelAndExistsUseTypedWireCommands(t *testing.T) {
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

		del, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if del.opcode != nativeOpDel {
			errCh <- fmt.Errorf("DEL opcode = %d; want %d", del.opcode, nativeOpDel)
			return
		}
		if err := writeNativeTestResponse(writer, del, nativeStatusOK, int64(2)); err != nil {
			errCh <- err
			return
		}

		exists, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if exists.opcode != nativeOpCommandExec {
			errCh <- fmt.Errorf("EXISTS opcode = %d; want %d", exists.opcode, nativeOpCommandExec)
			return
		}
		payload, rest, err := decodeNativeValue(exists.body)
		if err != nil || len(rest) != 0 {
			errCh <- fmt.Errorf("decode EXISTS payload: %w", err)
			return
		}
		mapping, err := nativeMap(payload)
		if err != nil || asString(mapping["command"]) != "EXISTS" {
			errCh <- fmt.Errorf("unexpected EXISTS payload: %#v (%v)", payload, err)
			return
		}
		errCh <- writeNativeTestResponse(writer, exists, nativeStatusOK, int64(1))
	}()

	exec := NewNativeExecutor(listener.Addr().String(), WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0))
	defer func() { _ = exec.Close() }()
	store := NewClientWithExecutor(exec).KV()
	if count, err := store.Del(context.Background(), "one", "two"); err != nil || count != 2 {
		t.Fatalf("typed DEL = %d, %v", count, err)
	}
	if count, err := store.Exists(context.Background(), "one", "two"); err != nil || count != 1 {
		t.Fatalf("typed EXISTS = %d, %v", count, err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}
