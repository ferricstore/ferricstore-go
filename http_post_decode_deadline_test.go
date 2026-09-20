package ferricstore

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestHTTPDecodedResponseAfterDeadlineIsRejected(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	requestContext := make(chan context.Context, 1)
	body := &deadlineHeldHTTPBody{started: started, release: release, data: []byte(`{"encoding":"ferricstore-json-v1","results":[{"status":"ok","value":"late"}]}`)}
	client := &http.Client{Transport: httpRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		requestContext <- request.Context()
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: body, Request: request}, nil
	})}
	executor, err := NewHTTPExecutorFromURL("http://example.com", WithHTTPClient(client), WithHTTPTimeout(30*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()

	result := make(chan error, 1)
	go func() { _, err := executor.Do(context.Background(), "PING"); result <- err }()
	<-started
	ctx := <-requestContext
	<-ctx.Done()
	close(release)
	err = <-result
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Code != "transport_timeout" || !httpErr.Retryable || httpErr.SafeToRetry || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late HTTP response error = %#v, want controlled timeout", err)
	}
}

func TestHTTPLateNon2xxResponseUsesTimeoutClassification(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	requestContext := make(chan context.Context, 1)
	body := &deadlineHeldHTTPBody{started: started, release: release, data: []byte(`{"error":{"code":"server_overloaded","retry_after_ms":60000}}`)}
	client := &http.Client{Transport: httpRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		requestContext <- request.Context()
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Status: "503 Service Unavailable", Header: make(http.Header), Body: body, Request: request}, nil
	})}
	executor, err := NewHTTPExecutorFromURL("http://example.com", WithHTTPClient(client), WithHTTPTimeout(30*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()

	result := make(chan error, 1)
	go func() { _, err := executor.Do(context.Background(), "PING"); result <- err }()
	<-started
	ctx := <-requestContext
	<-ctx.Done()
	close(release)
	err = <-result
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Code != "transport_timeout" || !httpErr.Retryable || httpErr.SafeToRetry || httpErr.RetryAfterMS != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late non-2xx error = %#v, want controlled timeout without retry metadata", err)
	}
}

func TestHTTPBodyReadTimeoutPrecedesNon2xxWrapper(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	requestContext := make(chan context.Context, 1)
	body := &deadlineHeldHTTPBody{started: started, release: release, err: context.DeadlineExceeded}
	client := &http.Client{Transport: httpRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		requestContext <- request.Context()
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Status: "503 Service Unavailable", Header: make(http.Header), Body: body, Request: request}, nil
	})}
	executor, err := NewHTTPExecutorFromURL("http://example.com", WithHTTPClient(client), WithHTTPTimeout(30*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()

	result := make(chan error, 1)
	go func() { _, err := executor.Do(context.Background(), "PING"); result <- err }()
	<-started
	ctx := <-requestContext
	<-ctx.Done()
	close(release)
	err = <-result
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusServiceUnavailable || httpErr.Code != "transport_timeout" || !httpErr.Retryable || httpErr.SafeToRetry || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("body-read timeout error = %#v, want direct controlled timeout", err)
	}
}

type deadlineHeldHTTPBody struct {
	started chan<- struct{}
	release <-chan struct{}
	data    []byte
	err     error
	once    sync.Once
}

func (body *deadlineHeldHTTPBody) Read(destination []byte) (int, error) {
	body.once.Do(func() { close(body.started) })
	<-body.release
	if body.err != nil {
		return 0, body.err
	}
	if len(body.data) == 0 {
		return 0, io.EOF
	}
	count := copy(destination, body.data)
	body.data = body.data[count:]
	return count, nil
}

func (*deadlineHeldHTTPBody) Close() error { return nil }
