package ferricstore

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestAdvanceHTTPResponseLossAfterDispatchIsOutcomeUnknown(t *testing.T) {
	for _, http2 := range []bool{false, true} {
		name := "http1"
		if http2 {
			name = "http2"
		}
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			var committed atomic.Bool
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				requests.Add(1)
				_, _ = io.Copy(io.Discard, request.Body)
				committed.Store(true)
				writer.Header().Set("Content-Type", "application/json")
				writer.Header().Set("Content-Length", strconv.Itoa(1<<20))
				writer.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(writer, `{"encoding":"ferricstore-json-v1","results":[`)
				if flusher, ok := writer.(http.Flusher); ok {
					flusher.Flush()
				}
			}))
			server.EnableHTTP2 = http2
			var options []HTTPOption
			if http2 {
				server.StartTLS()
				options = append(options, WithHTTPTLSConfig(
					server.Client().Transport.(*http.Transport).TLSClientConfig,
				))
			} else {
				server.Start()
			}
			defer server.Close()

			executor, err := NewHTTPExecutorFromURL(server.URL, options...)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = executor.Close() }()
			_, err = NewClientWithExecutor(executor).Advance(
				context.Background(), durableStepClaim(), "schedule_warning",
			)
			if !committed.Load() {
				t.Fatal("server did not dispatch the durable mutation before losing its response")
			}
			if !errorsIsDurableMutationUnknown(err) {
				t.Fatalf("error = %v; want durable mutation outcome unknown", err)
			}
			if got := requests.Load(); got != 1 {
				t.Fatalf("HTTP durable mutation attempts = %d; want 1", got)
			}
		})
	}
}

func TestAdvanceNativeEOFOnceServerHasAcceptedMutationIsOutcomeUnknown(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	var requests atomic.Int32
	var committed atomic.Bool
	serverDone := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		startup, readErr := readNativeRequestFrame(reader)
		if readErr != nil {
			_ = conn.Close()
			serverDone <- readErr
			return
		}
		if writeErr := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); writeErr != nil {
			_ = conn.Close()
			serverDone <- writeErr
			return
		}
		request, readErr := readNativeRequestFrame(reader)
		if readErr != nil {
			_ = conn.Close()
			serverDone <- readErr
			return
		}
		requests.Add(1)
		if request.opcode != nativeOpFlowStepContinue {
			_ = conn.Close()
			serverDone <- errUnexpectedFrame(request)
			return
		}
		committed.Store(true)
		serverDone <- conn.Close()
	}()

	client := NewClient(listener.Addr().String(), WithNativeOptions(
		WithNativeTimeout(time.Second),
		WithNativeHeartbeat(0, 0),
		WithNativeReconnect(0),
	))
	defer func() { _ = client.Close() }()
	_, err = client.Advance(context.Background(), durableStepClaim(), "schedule_warning")
	if serverErr := <-serverDone; serverErr != nil {
		t.Fatal(serverErr)
	}
	if !committed.Load() {
		t.Fatal("server did not accept the durable mutation before closing the connection")
	}
	if !errorsIsDurableMutationUnknown(err) {
		t.Fatalf("error = %v; want durable mutation outcome unknown", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("native durable mutation attempts = %d; want 1", got)
	}
}

func errorsIsDurableMutationUnknown(err error) bool {
	return err != nil && errors.Is(err, ErrDurableMutationUncertain)
}
