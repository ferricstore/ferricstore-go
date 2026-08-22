package ferricstore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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
