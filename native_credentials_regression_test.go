package ferricstore

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestNativeExplicitEmptyPasswordStillAuthenticates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		new  func(string) (*NativeExecutor, error)
	}{
		{
			name: "option",
			new: func(addr string) (*NativeExecutor, error) {
				return NewNativeExecutor(addr,
					WithNativeCredentials("alice", ""),
					WithNativeTimeout(time.Second),
					WithNativeHeartbeat(0, 0),
					WithNativeReconnect(0),
				), nil
			},
		},
		{
			name: "URL",
			new: func(addr string) (*NativeExecutor, error) {
				return NewNativeExecutorFromURL("ferric://alice:@"+addr,
					WithNativeTimeout(time.Second),
					WithNativeHeartbeat(0, 0),
					WithNativeReconnect(0),
				)
			},
		},
		{
			name: "URL username only",
			new: func(addr string) (*NativeExecutor, error) {
				return NewNativeExecutorFromURL("ferric://alice@"+addr,
					WithNativeTimeout(time.Second),
					WithNativeHeartbeat(0, 0),
					WithNativeReconnect(0),
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })

			serverErr := make(chan error, 1)
			go serveNativeEmptyPasswordAuth(listener, serverErr)

			exec, err := tt.new(listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = exec.Close() })
			value, err := exec.Do(context.Background(), "PING")
			if err != nil {
				t.Fatal(err)
			}
			if got := asString(value); got != "PONG" {
				t.Fatalf("PING response = %q, want PONG", got)
			}
			if err := <-serverErr; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func serveNativeEmptyPasswordAuth(listener net.Listener, result chan<- error) {
	conn, err := listener.Accept()
	if err != nil {
		result <- err
		return
	}
	defer func() { _ = conn.Close() }()
	reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)

	startup, err := readNativeRequestFrame(reader)
	if err != nil {
		result <- err
		return
	}
	if startup.opcode != nativeOpHello {
		result <- fmt.Errorf("first opcode = %#x, want HELLO", startup.opcode)
		return
	}
	if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
		result <- err
		return
	}

	auth, err := readNativeRequestFrame(reader)
	if err != nil {
		result <- err
		return
	}
	if auth.opcode != nativeOpAuth {
		_ = writeNativeTestResponse(writer, auth, nativeStatusOK, []byte("PONG"))
		result <- fmt.Errorf("second opcode = %#x, want AUTH", auth.opcode)
		return
	}
	value, rest, err := decodeNativeValue(auth.body)
	if err != nil || len(rest) != 0 {
		result <- fmt.Errorf("decode AUTH payload: value=%#v rest=%d err=%w", value, len(rest), err)
		return
	}
	payload, err := nativeMap(value)
	if err != nil {
		result <- fmt.Errorf("AUTH payload: %w", err)
		return
	}
	if username, password := asString(payload["username"]), asString(payload["password"]); username != "alice" || password != "" {
		result <- fmt.Errorf("AUTH credentials = %q/%q, want alice/empty", username, password)
		return
	}
	if err := writeNativeTestResponse(writer, auth, nativeStatusOK, map[string]any{"authenticated": true}); err != nil {
		result <- err
		return
	}

	request, err := readNativeRequestFrame(reader)
	if err != nil {
		result <- err
		return
	}
	result <- writeNativeTestResponse(writer, request, nativeStatusOK, []byte("PONG"))
}
