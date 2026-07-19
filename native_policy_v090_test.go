package ferricstore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestV090HelloRequiresPolicyReplaceAndGenerationCapabilities(t *testing.T) {
	for _, missing := range []string{"replace", "expected_generation"} {
		t.Run(missing, func(t *testing.T) {
			hello := nativeHelloForTest()
			fields := []any{"type", "replace", "expected_generation", "states"}
			filtered := make([]any, 0, len(fields)-1)
			for _, field := range fields {
				if asString(field) != missing {
					filtered = append(filtered, field)
				}
			}
			hello["capabilities"].(map[string]any)["schemas"] = map[string]any{
				"FLOW.POLICY.SET": map[string]any{
					"required": []any{"type"},
					"fields":   filtered,
				},
			}
			if _, err := parseNativeHelloContract(hello, nativeDefaultResponseBytes); err == nil {
				t.Fatalf("HELLO without %s was accepted", missing)
			}
		})
	}
}

func TestV090NativePolicyCASPayloadDisablesAutomaticReplay(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.POLICY.SET", "order",
		"REPLACE", true,
		"EXPECTED_GENERATION", int64(7),
		"STATE", "queued", "MODE", "FIFO",
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := command.payload.(map[string]any)
	if payload["replace"] != true || asInt64(payload["expected_generation"]) != 7 {
		t.Fatalf("policy payload = %#v", payload)
	}
	if command.replayPolicy != nativeReplayNever {
		t.Fatalf("CAS replay policy = %d, want never", command.replayPolicy)
	}

	patch, err := buildNativeCommand([]any{"FLOW.POLICY.SET", "order", "REPLACE", false})
	if err != nil {
		t.Fatal(err)
	}
	if patch.replayPolicy != nativeReplayDefault {
		t.Fatalf("non-CAS replay policy = %d, want default", patch.replayPolicy)
	}
}

func TestV090NativePolicyCASDoesNotReplaySafeBusy(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		if err := serveNativeStartup(reader, writer); err != nil {
			serverErr <- err
			return
		}
		first, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if first.opcode != nativeOpFlowPolicySet {
			serverErr <- errUnexpectedFrame(first)
			return
		}
		if err := writeNativeTestResponse(writer, first, nativeStatusBusy, map[string]any{
			"code": "busy", "retryable": true, "safe_to_retry": true,
		}); err != nil {
			serverErr <- err
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		unexpected, err := readNativeRequestFrame(reader)
		if err == nil {
			_ = writeNativeTestResponse(writer, unexpected, nativeStatusOK, policySnapshotResponse("order", 8, nil))
			serverErr <- fmt.Errorf("CAS was replayed with request %d", unexpected.requestID)
			return
		}
		if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	exec := NewNativeExecutor(listener.Addr().String(),
		WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(2),
	)
	t.Cleanup(func() { _ = exec.Close() })
	_, err = exec.Do(context.Background(),
		"FLOW.POLICY.SET", "order", "EXPECTED_GENERATION", int64(7),
	)
	var nativeErr NativeError
	if !errors.As(err, &nativeErr) || nativeErr.Status != nativeStatusBusy {
		t.Fatalf("CAS error = %T %v; want original busy error", err, err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestV090NativePipelineContainingPolicyCASDoesNotReplayBusyItems(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		if err := serveNativeStartup(reader, writer); err != nil {
			serverErr <- err
			return
		}
		first, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		busy := map[string]any{"code": "busy", "retryable": true, "safe_to_retry": true}
		if err := writeNativeTestResponse(writer, first, nativeStatusOK, []any{[]any{"error", busy}}); err != nil {
			serverErr <- err
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		unexpected, err := readNativeRequestFrame(reader)
		if err == nil {
			_ = writeNativeTestResponse(writer, unexpected, nativeStatusOK, []any{[]any{"ok", policySnapshotResponse("order", 8, nil)}})
			serverErr <- errors.New("pipeline containing CAS was replayed")
			return
		}
		if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	exec := NewNativeExecutor(listener.Addr().String(),
		WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(2),
	)
	t.Cleanup(func() { _ = exec.Close() })
	_, err = exec.Pipeline(context.Background(), [][]any{
		{"FLOW.POLICY.SET", "order", "EXPECTED_GENERATION", int64(7)},
	})
	var pipelineErr *PipelineError
	if !errors.As(err, &pipelineErr) {
		t.Fatalf("pipeline CAS error = %T %v; want item failure", err, err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestV090PostSendMutationOutcomeIsNotReplayed(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	tcpListener := listener.(*net.TCPListener)
	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		if err := serveNativeStartup(reader, writer); err != nil {
			_ = conn.Close()
			serverErr <- err
			return
		}
		mutation, err := readNativeRequestFrame(reader)
		if err != nil {
			_ = conn.Close()
			serverErr <- err
			return
		}
		if mutation.opcode != nativeOpSet {
			_ = conn.Close()
			serverErr <- errUnexpectedFrame(mutation)
			return
		}
		// The request has been received and may have committed. Closing without
		// a response creates an unknown post-send outcome.
		_ = conn.Close()
		_ = tcpListener.SetDeadline(time.Now().Add(150 * time.Millisecond))
		unexpected, err := listener.Accept()
		if err == nil {
			_ = unexpected.Close()
			serverErr <- errors.New("post-send mutation was replayed on a new connection")
			return
		}
		if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	exec := NewNativeExecutor(listener.Addr().String(),
		WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(3),
	)
	t.Cleanup(func() { _ = exec.Close() })
	if _, err := exec.Do(context.Background(), "SET", "key", "value"); err == nil {
		t.Fatal("post-send disconnect returned success")
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}
