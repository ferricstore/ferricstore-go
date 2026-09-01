package ferricstore

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPRejectsNativeControlsAndWrappedSessionCommandsBeforeNetwork(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()
	executor, err := NewHTTPExecutorFromURL(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()

	tests := [][]any{
		{"BACKPRESSURE"},
		{"ASKING"},
		{"EVENT"},
		{"FETCH_OR_COMPUTE", "cache", 1000},
		{"FETCH_OR_COMPUTE_ERROR", "cache", "token", "failed"},
		{"FETCH_OR_COMPUTE_RESULT", "cache", "token", "value", 1000},
		{"GOAWAY"},
		{"OPTIONS"},
		{"MONITOR"},
		{"PIPELINE"},
		{"PSYNC"},
		{"READONLY"},
		{"READWRITE"},
		{"REPLCONF"},
		{"RESET"},
		{"ROUTE"},
		{"ROUTE_BATCH"},
		{"SANDBOX"},
		{"SHARDS"},
		{"SSUBSCRIBE", "shard-events"},
		{"STARTUP"},
		{"SUBSCRIBE_EVENTS"},
		{"UNSUBSCRIBE_EVENTS"},
		{"SUNSUBSCRIBE", "shard-events"},
		{"SYNC"},
		{"WINDOW_UPDATE"},
		{"COMMAND_EXEC", "MULTI"},
		{"COMMAND_EXEC", "FETCH_OR_COMPUTE", "cache", 1000},
		{"COMMAND_EXEC", "FETCH_OR_COMPUTE_ERROR", "cache", "token", "failed"},
		{"COMMAND_EXEC", "FETCH_OR_COMPUTE_RESULT", "cache", "token", "value", 1000},
		{"COMMAND_EXEC", "SUBSCRIBE", "events"},
		{"COMMAND_EXEC", "ASKING"},
		{"COMMAND_EXEC", "MONITOR"},
		{"COMMAND_EXEC", "READONLY"},
		{"COMMAND_EXEC", "READWRITE"},
		{"COMMAND_EXEC", "REPLCONF", "listening-port", 6388},
		{"COMMAND_EXEC", "SYNC"},
		{"COMMAND_EXEC", "PSYNC", "?", -1},
		{"COMMAND_EXEC", "SSUBSCRIBE", "shard-events"},
		{"COMMAND_EXEC", "SUNSUBSCRIBE", "shard-events"},
	}
	for _, args := range tests {
		name := commandPart(args[0])
		t.Run(name+"_wrapped_"+commandPart(args[min(1, len(args)-1)]), func(t *testing.T) {
			if _, err := executor.Do(context.Background(), args...); !errors.Is(err, ErrHTTPConnectionAffineCommand) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	if requests.Load() != 0 {
		t.Fatalf("requests = %d", requests.Load())
	}
}

func TestHTTPSupportsLongLivedBlockingCommands(t *testing.T) {
	var commands []string
	client := &http.Client{Transport: httpRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		var envelope map[string]any
		if err := decodeTestJSON(request.Body, &envelope); err != nil {
			t.Fatal(err)
		}
		command := envelope["commands"].([]any)[0].([]any)
		commands = append(commands, commandPart(command[0]))
		return testHTTPResponse(http.StatusOK, httpSuccessEnvelope(nil)), nil
	})}
	executor, err := NewHTTPExecutorFromURL("http://example.com", WithHTTPClient(client))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()

	for _, args := range [][]any{
		{"BLPOP", "queue", 1},
		{"BRPOP", "queue", 1},
		{"BLMOVE", "source", "destination", "LEFT", "RIGHT", 1},
		{"BLMPOP", 1, 1, "queue", "LEFT"},
		{"XREAD", "BLOCK", 1, "STREAMS", "events", "$"},
		{"XREADGROUP", "GROUP", "workers", "one", "BLOCK", 1, "STREAMS", "events", ">"},
	} {
		if _, err := executor.Do(context.Background(), args...); err != nil {
			t.Fatalf("%s: %v", args[0], err)
		}
	}
	if len(commands) != 6 {
		t.Fatalf("commands = %#v", commands)
	}
}

func TestHTTPDispositionKeepsEveryLongLivedSingleRequestCommandSupported(t *testing.T) {
	for _, command := range []string{
		"BLMOVE", "BLMPOP", "BLPOP", "BRPOP", "BRPOPLPUSH", "BZMPOP", "BZPOPMAX", "BZPOPMIN",
		"XREAD", "XREADGROUP",
	} {
		if got := HTTPCommandDisposition(command); got != "supported" {
			t.Errorf("%s disposition = %q, want supported", command, got)
		}
	}
}

func TestHTTPBlockingCommandsExtendOrDisableDefaultTimeout(t *testing.T) {
	var remaining []time.Duration
	client := &http.Client{Transport: httpRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if deadline, ok := request.Context().Deadline(); ok {
			remaining = append(remaining, time.Until(deadline))
		} else {
			remaining = append(remaining, 0)
		}
		return testHTTPResponse(http.StatusOK, httpSuccessEnvelope(nil)), nil
	})}
	executor, err := NewHTTPExecutorFromURL(
		"http://example.com", WithHTTPClient(client), WithHTTPTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Do(context.Background(), "BLPOP", "queue", 5); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Do(context.Background(), "XREAD", "BLOCK", 0, "STREAMS", "events", "$"); err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 3 || remaining[0] <= time.Second || remaining[0] > 2*time.Second ||
		remaining[1] <= 6*time.Second || remaining[1] > 7*time.Second || remaining[2] != 0 {
		t.Fatalf("request timeouts = %v", remaining)
	}
}

func TestHTTPCommandExecUsesStructuredDescriptorAndPreservesRequestContext(t *testing.T) {
	var received map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if err := decodeTestJSON(request.Body, &received); err != nil {
			t.Fatal(err)
		}
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("value"))
	}))
	defer server.Close()
	client, err := NewClientFromURL(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	value, err := client.CommandExecWithContext(
		context.Background(), "GET", &RequestContext{Subject: "worker", Tenant: "tenant-a"}, "key",
	)
	if err != nil {
		t.Fatal(err)
	}
	if asString(value) != "value" {
		t.Fatalf("value = %#v", value)
	}
	command, ok := received["commands"].([]any)[0].(map[string]any)
	if !ok {
		t.Fatalf("command = %#v, want structured descriptor", received["commands"])
	}
	if command["command"] != "COMMAND_EXEC" || command["opcode"] != float64(nativeOpCommandExec) {
		t.Fatalf("descriptor = %#v", command)
	}
	payload, err := decodeHTTPValue(command["payload"])
	if err != nil {
		t.Fatal(err)
	}
	mapping, ok := payload.(map[string]any)
	if !ok || asString(mapping["command"]) != "GET" {
		t.Fatalf("payload = %#v", payload)
	}
	args, ok := mapping["args"].([]any)
	if !ok || len(args) != 1 || asString(args[0]) != "key" {
		t.Fatalf("args = %#v", mapping["args"])
	}
	requestContext, ok := mapping["request_context"].(map[string]any)
	if !ok || asString(requestContext["subject"]) != "worker" || asString(requestContext["tenant"]) != "tenant-a" {
		t.Fatalf("request_context = %#v", mapping["request_context"])
	}
}

func TestHTTPEncodingBoundsPointerIndirection(t *testing.T) {
	value := any("leaf")
	for range nativeMaxEncodeDepth + 2 {
		next := value
		value = &next
	}
	if _, err := encodeHTTPValue(value); err == nil || !strings.Contains(err.Error(), "nesting depth") {
		t.Fatalf("error = %v", err)
	}
}

func TestHTTPCanceledContextIsNotReportedAsRetryable(t *testing.T) {
	executor, err := NewHTTPExecutorFromURL("http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = executor.Do(ctx, "PING")
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Code != "transport_canceled" || httpErr.Retryable {
		t.Fatalf("error = %#v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error does not preserve context cancellation: %v", err)
	}
}

func TestHTTPLocalFailuresAreMarkedNotSentAndNeverReachRoundTripper(t *testing.T) {
	var roundTrips atomic.Int32
	client := &http.Client{Transport: httpRoundTripFunc(func(*http.Request) (*http.Response, error) {
		roundTrips.Add(1)
		return testHTTPResponse(http.StatusOK, httpSuccessEnvelope("PONG")), nil
	})}
	executor, err := NewHTTPExecutorFromURL(
		"http://example.com",
		WithHTTPClient(client),
		WithHTTPMaxRequestBytes(128),
		WithHTTPMaxBatchCommands(1),
	)
	if err != nil {
		t.Fatal(err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name string
		call func() error
	}{
		{name: "already canceled", call: func() error { _, err := executor.Do(canceled, "PING"); return err }},
		{name: "encoding", call: func() error { _, err := executor.Do(context.Background(), "SET", "key", make(chan int)); return err }},
		{name: "request size", call: func() error {
			_, err := executor.Do(context.Background(), "SET", "key", strings.Repeat("x", 256))
			return err
		}},
		{name: "batch size", call: func() error {
			_, err := executor.Pipeline(context.Background(), [][]any{{"PING"}, {"PING"}})
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.call()
			var notSent *commandNotSentError
			if !errors.As(err, &notSent) {
				t.Fatalf("error = %#v; want commandNotSentError", err)
			}
		})
	}
	if got := roundTrips.Load(); got != 0 {
		t.Fatalf("local failures performed %d HTTP exchange(s)", got)
	}

	if err := executor.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = executor.Do(context.Background(), "PING")
	var notSent *commandNotSentError
	if !errors.As(err, &notSent) {
		t.Fatalf("closed executor error = %#v; want commandNotSentError", err)
	}
	if got := roundTrips.Load(); got != 0 {
		t.Fatalf("closed executor performed %d HTTP exchange(s)", got)
	}
}

func TestHTTPRetryAfterSupportsHTTPDate(t *testing.T) {
	now := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	if got := parseHTTPRetryAfter(now.Add(3*time.Second).Format(http.TimeFormat), now); got != 3_000 {
		t.Fatalf("Retry-After date = %dms, want 3000ms", got)
	}
	if got := parseHTTPRetryAfter("7", now); got != 7_000 {
		t.Fatalf("Retry-After seconds = %dms, want 7000ms", got)
	}
}

func TestHTTP2MultiplexesConcurrentCommandsOnOneConnection(t *testing.T) {
	var connections atomic.Int32
	var active atomic.Int32
	var maximum atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.ProtoMajor != 2 {
			t.Errorf("protocol = %s, want HTTP/2", request.Proto)
		}
		current := active.Add(1)
		for current > maximum.Load() && !maximum.CompareAndSwap(maximum.Load(), current) {
		}
		time.Sleep(10 * time.Millisecond)
		active.Add(-1)
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	server.EnableHTTP2 = true
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.StartTLS()
	defer server.Close()
	tlsConfig := server.Client().Transport.(*http.Transport).TLSClientConfig
	executor, err := NewHTTPExecutorFromURL(server.URL, WithHTTPTLSConfig(tlsConfig))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	maximum.Store(0)
	var wait sync.WaitGroup
	for range 20 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, err := executor.Do(context.Background(), "PING"); err != nil {
				t.Errorf("PING: %v", err)
			}
		}()
	}
	wait.Wait()
	if connections.Load() != 1 || maximum.Load() <= 1 {
		t.Fatalf("connections=%d max concurrent streams=%d", connections.Load(), maximum.Load())
	}
}

func TestHTTP2CanBeDisabledForTLSConnections(t *testing.T) {
	var protocol string
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		protocol = request.Proto
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	tlsConfig := server.Client().Transport.(*http.Transport).TLSClientConfig
	executor, err := NewHTTPExecutorFromURL(
		server.URL, WithHTTPTLSConfig(tlsConfig), WithHTTP2(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	if protocol != "HTTP/1.1" {
		t.Fatalf("protocol = %q, want HTTP/1.1", protocol)
	}
}

func TestHTTPMaxConnectionsBoundsHTTP1Concurrency(t *testing.T) {
	var active atomic.Int32
	var maximum atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		current := active.Add(1)
		for current > maximum.Load() && !maximum.CompareAndSwap(maximum.Load(), current) {
		}
		time.Sleep(20 * time.Millisecond)
		active.Add(-1)
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	defer server.Close()
	executor, err := NewHTTPExecutorFromURL(server.URL, WithHTTPMaxConnections(2))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	var wait sync.WaitGroup
	for range 12 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, err := executor.Do(context.Background(), "PING"); err != nil {
				t.Errorf("PING: %v", err)
			}
		}()
	}
	wait.Wait()
	if maximum.Load() != 2 {
		t.Fatalf("maximum concurrent requests = %d, want 2", maximum.Load())
	}
}

func TestHTTPBasicAuthIsSentOverTLS(t *testing.T) {
	var authorization string
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		authorization = request.Header.Get("Authorization")
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	defer server.Close()

	executor, err := NewHTTPExecutorFromURL(
		server.URL,
		WithHTTPClient(server.Client()),
		WithHTTPBasicAuth("worker", "secret"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("worker:secret"))
	if authorization != want {
		t.Fatalf("Authorization = %q, want %q", authorization, want)
	}
}

func TestHTTPCustomClientStillPreservesRedirectCredentials(t *testing.T) {
	var authorization string
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		authorization = request.Header.Get("Authorization")
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", target.URL+"/v1/commands")
		writer.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	executor, err := NewHTTPExecutorFromURL(
		redirect.URL,
		WithHTTPClient(&http.Client{}),
		WithHTTPBearerToken("secret"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	if authorization != "Bearer secret" {
		t.Fatalf("redirected Authorization = %q", authorization)
	}
}

func TestHTTPFlowValueMGetConvertsCompactNativePayload(t *testing.T) {
	var command map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var envelope map[string]any
		if err := decodeTestJSON(request.Body, &envelope); err != nil {
			t.Fatal(err)
		}
		command = envelope["commands"].([]any)[0].(map[string]any)
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope([]any{}))
	}))
	defer server.Close()
	executor, err := NewHTTPExecutorFromURL(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "FLOW.VALUE.MGET", "ref-1", "MAX_BYTES", 128); err != nil {
		t.Fatal(err)
	}
	if command["opcode"] != float64(nativeOpFlowValueMGet) {
		t.Fatalf("opcode = %#v", command["opcode"])
	}
	payload := command["payload"].(map[string]any)[httpMapTag].([]any)
	if len(payload) != 2 {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestHTTPRejectsEveryConnectionAffineCommandBeforeNetwork(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()
	executor, err := NewHTTPExecutorFromURL(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	for command := range httpConnectionAffineCommands {
		t.Run(command, func(t *testing.T) {
			if _, err := executor.Do(context.Background(), command); !errors.Is(err, ErrHTTPConnectionAffineCommand) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	if requests.Load() != 0 {
		t.Fatalf("requests = %d", requests.Load())
	}
}

func TestHTTPCommandDispositionCoversNativeCommandFamilies(t *testing.T) {
	for _, command := range []string{
		"HELLO", "AUTH", "PING", "CLIENT", "QUIT", "COMMAND_EXEC", "GET", "SET",
		"FLOW.CREATE", "FLOW.QUERY", "CLUSTER.HEALTH", "FERRICSTORE.METRICS",
	} {
		disposition := HTTPCommandDisposition(command)
		if disposition != "supported" && disposition != "native_only" {
			t.Fatalf("%s disposition = %q", command, disposition)
		}
	}
	if HTTPCommandDisposition("SET") != "supported" || HTTPCommandDisposition("AUTH") != "native_only" {
		t.Fatal("unexpected HTTP command disposition")
	}
}

func TestHTTPEnforcesRequestBatchAndConnectionLimits(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	defer server.Close()

	executor, err := NewHTTPExecutorFromURL(server.URL, WithHTTPMaxRequestBytes(32))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Do(context.Background(), "SET", "key", string(make([]byte, 64))); err == nil {
		t.Fatal("expected request limit error")
	}
	if requests.Load() != 0 {
		t.Fatal("oversized request reached the server")
	}
	var httpErr *HTTPError
	if _, err := executor.Do(context.Background(), "CUSTOM", make([]any, 33)); !errors.As(err, &httpErr) || httpErr.Code != "request_too_large" {
		t.Fatalf("oversized container error = %#v", err)
	}
	if requests.Load() != 0 {
		t.Fatal("oversized container reached the server")
	}
	_ = executor.Close()

	if _, err := NewHTTPExecutorFromURL(server.URL, WithHTTPMaxBatchCommands(0)); err == nil {
		t.Fatal("expected invalid batch limit")
	}
	if _, err := NewHTTPExecutorFromURL(server.URL, WithHTTPMaxConnections(0)); err == nil {
		t.Fatal("expected invalid connection limit")
	}
}

func TestHTTPTimeoutIncludesResponseBodyAndIsNotSafeToReplay(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"encoding":"ferricstore-json-v1","results":[`)
		if flusher, ok := writer.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(150 * time.Millisecond)
		_, _ = io.WriteString(writer, `]}`)
	}))
	defer server.Close()
	executor, err := NewHTTPExecutorFromURL(server.URL, WithHTTPTimeout(30*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	_, err = executor.Do(context.Background(), "PING")
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Code != "transport_timeout" || !httpErr.Retryable || httpErr.SafeToRetry {
		t.Fatalf("timeout error = %#v", err)
	}
}

func TestHTTPRejectsMalformedSuccessEnvelopes(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "invalid json", body: `{`},
		{name: "trailing json", body: `{}` + `{}`},
		{name: "unknown encoding", body: `{"encoding":"other","results":[{"status":"ok","value":null}]}`},
		{name: "wrong count", body: `{"results":[]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(writer, test.body)
			}))
			defer server.Close()
			executor, err := NewHTTPExecutorFromURL(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = executor.Close() }()
			_, err = executor.Do(context.Background(), "PING")
			var httpErr *HTTPError
			if !errors.As(err, &httpErr) || httpErr.Code != "invalid_response" {
				t.Fatalf("error = %#v", err)
			}
		})
	}
}

func TestHTTPBinaryMapMatchesNativeGoMapShape(t *testing.T) {
	value, err := decodeHTTPValue(map[string]any{httpMapTag: []any{
		[]any{map[string]any{httpBytesTag: base64.StdEncoding.EncodeToString([]byte("key"))}, "value"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"key": []byte("value")}
	if !reflect.DeepEqual(value, want) {
		t.Fatalf("value = %#v, want %#v", value, want)
	}
}

func httpSuccessEnvelope(value any) map[string]any {
	return map[string]any{
		"encoding": httpBinaryEncoding,
		"results":  []any{map[string]any{"status": "ok", "value": value}},
	}
}

func decodeTestJSON(reader io.Reader, value any) error {
	decoder := json.NewDecoder(reader)
	return decoder.Decode(value)
}

type httpRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn httpRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func testHTTPResponse(status int, value any) *http.Response {
	body, _ := json.Marshal(value)
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}
