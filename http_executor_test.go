package ferricstore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPExecutorBinaryCommandAndURLSelection(t *testing.T) {
	var received map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/commands" {
			t.Fatalf("unexpected request %s %s", request.Method, request.URL.Path)
		}
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Fatal(err)
		}
		writeHTTPJSON(t, writer, http.StatusOK, map[string]any{
			"encoding": httpBinaryEncoding,
			"results": []any{map[string]any{
				"status": "ok",
				"value":  map[string]any{httpBytesTag: base64.StdEncoding.EncodeToString([]byte{0, 1, 255})},
			}},
		})
	}))
	defer server.Close()

	client, err := NewClientFromURL(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if _, ok := client.exec.(*HTTPExecutor); !ok {
		t.Fatalf("NewClientFromURL selected %T, want *HTTPExecutor", client.exec)
	}

	value, err := client.Command(context.Background(), "SET", []byte("key"), []byte{0, 1, 255})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(value, []byte{0, 1, 255}) {
		t.Fatalf("value = %#v", value)
	}
	commands := received["commands"].([]any)
	command := commands[0].([]any)
	if command[0] != "SET" {
		t.Fatalf("command name = %#v", command[0])
	}
	if !reflect.DeepEqual(command[1], map[string]any{httpBytesTag: base64.StdEncoding.EncodeToString([]byte("key"))}) {
		t.Fatalf("binary key = %#v", command[1])
	}
}

func TestHTTPPipelineUsesOneRequestAndPreservesItemErrors(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		writeHTTPJSON(t, writer, http.StatusOK, map[string]any{
			"encoding": httpBinaryEncoding,
			"results": []any{
				map[string]any{"status": "ok", "value": "OK"},
				map[string]any{"status": "error", "error": map[string]any{
					"code": "noperm", "message": "NOPERM denied", "retryable": false,
				}},
				map[string]any{"status": "ok", "value": int64(3)},
			},
		})
	}))
	defer server.Close()

	exec, err := NewHTTPExecutorFromURL(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	client := NewClientWithExecutor(exec)
	values, err := client.Pipeline(context.Background(), [][]any{
		{"SET", "a", "1"}, {"GET", "secret"}, {"INCR", "counter"},
	})
	var pipelineErr *PipelineError
	if !errors.As(err, &pipelineErr) || len(pipelineErr.Failures) != 1 || pipelineErr.Failures[0].Index != 1 {
		t.Fatalf("pipeline error = %#v", err)
	}
	if requests.Load() != 1 || len(values) != 3 || !isOK(values[0]) || asInt64(values[2]) != 3 {
		t.Fatalf("requests=%d values=%#v", requests.Load(), values)
	}
	var httpErr *HTTPError
	if !errors.As(values[1].(error), &httpErr) || httpErr.Code != "noperm" || httpErr.SafeToRetry {
		t.Fatalf("item error = %#v", values[1])
	}
}

func TestHTTPAuthenticationAndRedirectsPreserveHeaders(t *testing.T) {
	var targetAuth string
	var targetCustom string
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		targetAuth = request.Header.Get("Authorization")
		targetCustom = request.Header.Get("X-Trace")
		writeHTTPJSON(t, writer, http.StatusOK, map[string]any{
			"encoding": httpBinaryEncoding,
			"results":  []any{map[string]any{"status": "ok", "value": "PONG"}},
		})
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", target.URL+"/v1/commands")
		writer.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	exec, err := NewHTTPExecutorFromURL(
		redirect.URL,
		WithHTTPBearerToken("secret"),
		WithHTTPHeaders(http.Header{"X-Trace": []string{"trace-1"}}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	if _, err := exec.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	if targetAuth != "Bearer secret" || targetCustom != "trace-1" {
		t.Fatalf("redirected headers auth=%q trace=%q", targetAuth, targetCustom)
	}
}

func TestHTTPBasicAuthRequiresHTTPSAndRejectsCredentialConflicts(t *testing.T) {
	if _, err := NewHTTPExecutorFromURL(
		"http://example.com", WithHTTPBasicAuth("worker", "secret"),
	); err == nil {
		t.Fatal("expected plaintext Basic auth rejection")
	}
	if _, err := NewHTTPExecutorFromURL(
		"https://example.com",
		WithHTTPBearerToken("token"),
		WithHTTPBasicAuth("worker", "secret"),
	); err == nil {
		t.Fatal("expected credential conflict")
	}
	if _, err := NewHTTPExecutorFromURL("https://worker:secret@example.com"); err == nil {
		t.Fatal("expected URL user-info rejection")
	}
}

func TestHTTPExecutorRejectsSessionCommandsAndBoundsResponses(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"results":[{"status":"ok","value":"too large"}]}`))
	}))
	defer server.Close()

	exec, err := NewHTTPExecutorFromURL(server.URL, WithHTTPMaxResponseBytes(8))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	if _, err := exec.Do(context.Background(), "MULTI"); !errors.Is(err, ErrHTTPConnectionAffineCommand) {
		t.Fatalf("session command error = %v", err)
	}
	if requests.Load() != 0 {
		t.Fatal("session command reached HTTP server")
	}
	var httpErr *HTTPError
	_, err = exec.Do(context.Background(), "PING")
	if !errors.As(err, &httpErr) || httpErr.Code != "response_too_large" {
		t.Fatalf("bounded response error = %#v", err)
	}
}

func TestHTTPExecutorReusesKeepAliveConnections(t *testing.T) {
	var connections atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeHTTPJSON(t, writer, http.StatusOK, map[string]any{
			"encoding": httpBinaryEncoding,
			"results":  []any{map[string]any{"status": "ok", "value": "PONG"}},
		})
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	defer server.Close()

	exec, err := NewHTTPExecutorFromURL(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	for range 3 {
		if _, err := exec.Do(context.Background(), "PING"); err != nil {
			t.Fatal(err)
		}
	}
	if connections.Load() != 1 {
		t.Fatalf("connections = %d, want 1", connections.Load())
	}
}

func TestHTTPExecutorUsesStructuredFlowDescriptors(t *testing.T) {
	var received []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var envelope map[string]any
		if err := json.NewDecoder(request.Body).Decode(&envelope); err != nil {
			t.Fatal(err)
		}
		received = append(received, envelope)
		writeHTTPJSON(t, writer, http.StatusOK, map[string]any{
			"encoding": httpBinaryEncoding,
			"results":  []any{map[string]any{"status": "ok", "value": map[string]any{}}},
		})
	}))
	defer server.Close()

	exec, err := NewHTTPExecutorFromURL(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	_, err = exec.Do(
		context.Background(),
		"FLOW.START_AND_CLAIM", "flow-1", "TYPE", "checkout", "INITIAL_STATE", "queued",
		"WORKER", "worker-1", "LEASE_MS", int64(30_000), "NOW", int64(100),
	)
	if err != nil {
		t.Fatal(err)
	}
	encoded := received[0]["commands"].([]any)[0].(map[string]any)
	if encoded["command"] != "FLOW.START_AND_CLAIM" || encoded["opcode"] != float64(0x0223) {
		t.Fatalf("descriptor = %#v", encoded)
	}
	payload := encoded["payload"].(map[string]any)[httpMapTag].([]any)
	if len(payload) == 0 {
		t.Fatal("structured payload was empty")
	}

	if _, err := exec.Do(
		context.Background(),
		"FLOW.VALUE.PUT", []byte(`{"named":true}`), "NOW", int64(100),
		"OWNER_FLOW_ID", "flow-1", "NAME", "result",
	); err != nil {
		t.Fatal(err)
	}
	encoded = received[1]["commands"].([]any)[0].(map[string]any)
	if encoded["command"] != "FLOW.VALUE.PUT" || encoded["opcode"] != float64(nativeOpFlowValuePut) {
		t.Fatalf("FLOW.VALUE.PUT descriptor = %#v", encoded)
	}
}

func TestHTTPTopLevelTimeoutErrorKeepsRetrySafetyExplicit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeHTTPJSON(t, writer, http.StatusServiceUnavailable, map[string]any{
			"error": map[string]any{"code": "server_overloaded", "retry_after_ms": int64(25)},
		})
	}))
	defer server.Close()
	exec, err := NewHTTPExecutorFromURL(server.URL, WithHTTPTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	_, err = exec.Do(context.Background(), "PING")
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != 503 || httpErr.Code != "server_overloaded" || httpErr.SafeToRetry {
		t.Fatalf("top-level error = %#v", err)
	}
}

func writeHTTPJSON(t *testing.T, writer http.ResponseWriter, status int, value any) {
	t.Helper()
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPExecutorConcurrentUseIsSafe(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeHTTPJSON(t, writer, http.StatusOK, map[string]any{
			"encoding": httpBinaryEncoding,
			"results":  []any{map[string]any{"status": "ok", "value": "PONG"}},
		})
	}))
	defer server.Close()
	exec, err := NewHTTPExecutorFromURL(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	var wait sync.WaitGroup
	for range 16 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, err := exec.Do(context.Background(), "PING"); err != nil {
				t.Errorf("PING: %v", err)
			}
		}()
	}
	wait.Wait()
}
