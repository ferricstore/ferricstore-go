package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPRedirectTreatsDifferentPortAsDifferentOrigin(t *testing.T) {
	var received http.Header
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		received = request.Header.Clone()
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", target.URL+"/v1/commands")
		writer.WriteHeader(http.StatusFound)
	}))
	defer redirect.Close()

	executor, err := NewHTTPExecutorFromURL(
		redirect.URL,
		WithHTTPBearerToken("authorization-secret"),
		WithHTTPHeaders(http.Header{
			"Referer":   []string{"https://source.example/secret?token=referer-secret"},
			"X-API-Key": []string{"api-secret"},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Authorization", "Referer", "X-API-Key"} {
		if value := received.Get(name); value != "" {
			t.Errorf("different-port redirected %s = %q, want empty", name, value)
		}
	}
}

func TestHTTPRedirectDoesNotReplayBodyAcrossDifferentPort(t *testing.T) {
	var targetRequests atomic.Int32
	var targetBody []byte
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		targetRequests.Add(1)
		targetBody, _ = io.ReadAll(request.Body)
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", target.URL+"/v1/commands")
		writer.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	executor, err := NewHTTPExecutorFromURL(redirect.URL, WithHTTPBearerToken("authorization-secret"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "SET", "secret-key", "secret-value"); err == nil {
		t.Fatal("different-port body-preserving redirect succeeded")
	}
	if targetRequests.Load() != 0 || len(targetBody) != 0 {
		t.Fatalf("different-port redirect replayed body: requests=%d body=%q", targetRequests.Load(), targetBody)
	}
}

func TestHTTPValidRedirectLocationRedactsQuerySecretFromErrors(t *testing.T) {
	const secret = "valid-location-query-secret"
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	targetURL := target.URL
	target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", targetURL+"/v1/commands?token="+url.QueryEscape(secret))
		writer.WriteHeader(http.StatusFound)
	}))
	defer redirect.Close()

	executor, err := NewHTTPExecutorFromURL(redirect.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	_, err = executor.Do(context.Background(), "PING")
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Cause == nil {
		t.Fatalf("valid redirect transport error = %v, want HTTPError with cause", err)
	}
	if !strings.Contains(httpErr.Cause.Error(), "ferricstore HTTP redirect failed") {
		t.Fatalf("valid redirect cause = %q, want safe redirect diagnostic", httpErr.Cause)
	}
	for name, text := range map[string]string{
		"cause":   httpErr.Cause.Error(),
		"error":   err.Error(),
		"format":  fmt.Sprintf("%+v", err),
		"wrapped": fmt.Sprintf("%v", httpErr.Cause),
	} {
		if strings.Contains(text, secret) {
			t.Errorf("valid redirect %s exposed query secret: %q", name, text)
		}
	}
}

func TestHTTPRedirectRedactsPathRawPathAndOpaqueFromErrors(t *testing.T) {
	tests := []struct {
		name     string
		secret   string
		location func(string) string
	}{
		{
			name:     "path",
			secret:   "redirect-path-secret",
			location: func(targetURL string) string { return targetURL + "/" + "redirect-path-secret" },
		},
		{
			name:     "raw path",
			secret:   "redirect-raw-path-secret",
			location: func(targetURL string) string { return targetURL + "/safe%2Fredirect-raw-path-secret" },
		},
		{
			name:     "opaque",
			secret:   "redirect-opaque-secret",
			location: func(string) string { return "http:redirect-opaque-secret" },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
			}))
			targetURL := target.URL
			target.Close()
			redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Location", test.location(targetURL))
				writer.WriteHeader(http.StatusFound)
			}))
			defer redirect.Close()

			executor, err := NewHTTPExecutorFromURL(redirect.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = executor.Close() }()
			_, err = executor.Do(context.Background(), "PING")
			var httpErr *HTTPError
			if !errors.As(err, &httpErr) || httpErr.Cause == nil {
				t.Fatalf("redirect transport error = %v, want HTTPError with cause", err)
			}
			if !strings.Contains(httpErr.Cause.Error(), "ferricstore HTTP redirect failed") {
				t.Fatalf("redirect cause = %q, want safe redirect diagnostic", httpErr.Cause)
			}
			for name, text := range map[string]string{
				"cause":  httpErr.Cause.Error(),
				"error":  err.Error(),
				"format": fmt.Sprintf("%+v", err),
				"sharp":  fmt.Sprintf("%#v", err),
			} {
				if strings.Contains(text, test.secret) {
					t.Errorf("redirect %s exposed %s secret: %q", name, test.name, text)
				}
			}
		})
	}
}

func TestHTTPNonRedirectTransportCausePreservesURL(t *testing.T) {
	const secret = "initial-path-transport-secret"
	executor, err := NewHTTPExecutorFromURL("http://127.0.0.1:1/" + secret)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	_, err = executor.Do(context.Background(), "PING")
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Cause == nil {
		t.Fatalf("non-redirect transport error = %v, want HTTPError with cause", err)
	}
	if !strings.Contains(httpErr.Cause.Error(), secret) {
		t.Fatalf("non-redirect cause = %q, want original URL path", httpErr.Cause)
	}
}

func TestHTTPCustomRedirectCallbackErrorRedactsURLText(t *testing.T) {
	const secret = "caller-redirect-secret"
	var targetRequests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		targetRequests.Add(1)
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	defer target.Close()
	targetURL, err := url.Parse(target.URL + "/caller-path-secret?token=" + secret)
	if err != nil {
		t.Fatal(err)
	}
	targetURL.User = url.UserPassword("caller-user", secret)
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", targetURL.String())
		writer.WriteHeader(http.StatusFound)
	}))
	defer redirect.Close()

	var callbackCalls atomic.Int32
	client := &http.Client{CheckRedirect: func(request *http.Request, _ []*http.Request) error {
		callbackCalls.Add(1)
		return fmt.Errorf("caller rejected redirect URL %s: safe-callback-diagnostic", request.URL)
	}}
	executor, err := NewHTTPExecutorFromURL(redirect.URL, WithHTTPClient(client))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	_, err = executor.Do(context.Background(), "PING")
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Cause == nil {
		t.Fatalf("caller redirect error = %v, want HTTPError with cause", err)
	}
	if !strings.Contains(httpErr.Cause.Error(), "ferricstore HTTP redirect failed") {
		t.Fatalf("caller redirect cause = %q, want safe redirect diagnostic", httpErr.Cause)
	}
	for name, text := range map[string]string{
		"cause":  httpErr.Cause.Error(),
		"error":  err.Error(),
		"format": fmt.Sprintf("%+v", err),
		"sharp":  fmt.Sprintf("%#v", err),
	} {
		if strings.Contains(text, secret) {
			t.Errorf("caller redirect %s exposed URL secret: %q", name, text)
		}
	}
	if callbackCalls.Load() != 1 || targetRequests.Load() != 0 {
		t.Fatalf("callback calls=%d target requests=%d; want one callback and no target request", callbackCalls.Load(), targetRequests.Load())
	}
}

func TestHTTPCustomRedirectErrorsExposeOnlySafeDiagnostic(t *testing.T) {
	tests := []struct {
		name     string
		secret   string
		location func(string, string) string
		callback func(*http.Request, string) error
	}{
		{
			name:   "relative location resolves secret path and query",
			secret: "relative-redirect-secret",
			location: func(_ string, secret string) string {
				return "/relative-path-" + secret + "?token=" + secret
			},
			callback: func(request *http.Request, _ string) error {
				return fmt.Errorf("callback target %s", request.URL)
			},
		},
		{
			name:   "same URL absolute location has secret path",
			secret: "absolute-redirect-secret",
			location: func(sourceURL, secret string) string {
				return sourceURL + "/absolute-path-" + secret + "?token=" + secret
			},
			callback: func(request *http.Request, _ string) error {
				return fmt.Errorf("callback target %s", request.URL)
			},
		},
		{
			name:   "callback formats URL fields",
			secret: "formatted-redirect-secret",
			location: func(sourceURL, secret string) string {
				return sourceURL + "/formatted-path-" + secret + "?token=" + secret
			},
			callback: func(request *http.Request, _ string) error {
				return fmt.Errorf("callback path=%s raw_query=%s url=%#v", request.URL.Path, request.URL.RawQuery, request.URL)
			},
		},
		{
			name:   "callback returns nested url error with nil cause",
			secret: "nested-redirect-secret",
			location: func(sourceURL, secret string) string {
				return sourceURL + "/nested-path-" + secret + "?token=" + secret
			},
			callback: func(request *http.Request, _ string) error {
				return fmt.Errorf("wrapped callback: %w", &url.Error{Op: "callback", URL: request.URL.String()})
			},
		},
		{
			name:   "callback URL includes userinfo",
			secret: "userinfo-redirect-secret",
			location: func(sourceURL, secret string) string {
				target, err := url.Parse(sourceURL + "/userinfo-path?token=" + secret)
				if err != nil {
					return "http://invalid"
				}
				target.User = url.UserPassword("redirect-user", secret)
				return target.String()
			},
			callback: func(request *http.Request, _ string) error {
				return fmt.Errorf("callback target %s", request.URL)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var source *httptest.Server
			source = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Location", test.location(source.URL, test.secret))
				writer.WriteHeader(http.StatusFound)
			}))
			defer source.Close()

			var callbackCalls atomic.Int32
			client := &http.Client{CheckRedirect: func(request *http.Request, _ []*http.Request) error {
				callbackCalls.Add(1)
				return test.callback(request, test.secret)
			}}
			executor, err := NewHTTPExecutorFromURL(source.URL, WithHTTPClient(client))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = executor.Close() }()
			_, err = executor.Do(context.Background(), "PING")
			var httpErr *HTTPError
			if !errors.As(err, &httpErr) || httpErr.Cause == nil {
				t.Fatalf("redirect callback error = %v, want HTTPError with cause", err)
			}
			if !strings.Contains(httpErr.Cause.Error(), "ferricstore HTTP redirect failed") {
				t.Fatalf("redirect callback cause = %q, want safe redirect diagnostic", httpErr.Cause)
			}
			for name, text := range map[string]string{
				"cause":  httpErr.Cause.Error(),
				"error":  err.Error(),
				"format": fmt.Sprintf("%+v", err),
				"sharp":  fmt.Sprintf("%#v", err),
			} {
				if strings.Contains(text, test.secret) {
					t.Errorf("redirect callback %s exposed secret: %q", name, text)
				}
			}
			if errors.Unwrap(httpErr.Cause) != nil {
				t.Fatalf("redirect callback cause unwrap = %v, want no raw cause", errors.Unwrap(httpErr.Cause))
			}
			var rawURL *url.Error
			if errors.As(httpErr.Cause, &rawURL) {
				t.Fatalf("redirect callback cause retained raw url.Error: %#v", rawURL)
			}
			if callbackCalls.Load() != 1 {
				t.Fatalf("callback calls = %d, want 1", callbackCalls.Load())
			}
		})
	}
}

func TestHTTPCustomTransportIdentityAndRedirectSanitization(t *testing.T) {
	const secret = "transport-identity-redirect-secret"
	transport := &identityRedirectTransport{
		location: "http://example.com/" + secret + "?token=" + secret,
	}
	caller := &http.Client{
		Transport: transport,
		Timeout:   time.Second,
		CheckRedirect: func(request *http.Request, _ []*http.Request) error {
			return fmt.Errorf("caller rejected %s", request.URL)
		},
	}
	executor, err := NewHTTPExecutorFromURL("http://example.com", WithHTTPClient(caller))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if executor.client.Transport != transport {
		t.Fatalf("custom transport identity changed: got %T, want %T", executor.client.Transport, transport)
	}
	if _, ok := executor.client.Transport.(interface{ CloseIdleConnections() }); !ok {
		t.Fatal("custom transport hid CloseIdleConnections")
	}
	if _, ok := executor.client.Transport.(interface{ CancelRequest(*http.Request) }); !ok {
		t.Fatal("custom transport hid legacy CancelRequest")
	}
	if executor.client.Timeout != caller.Timeout {
		t.Fatalf("custom client timeout = %s, want %s", executor.client.Timeout, caller.Timeout)
	}
	_, err = executor.Do(context.Background(), "PING")
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Cause == nil {
		t.Fatalf("custom redirect error = %v, want HTTPError with cause", err)
	}
	for name, text := range map[string]string{
		"cause":  httpErr.Cause.Error(),
		"error":  err.Error(),
		"format": fmt.Sprintf("%+v", err),
		"sharp":  fmt.Sprintf("%#v", err),
	} {
		if strings.Contains(text, secret) {
			t.Errorf("custom transport redirect %s exposed secret: %q", name, text)
		}
	}
}

func TestHTTPNonRedirectTypedNilNestedURLErrorDoesNotPanic(t *testing.T) {
	var typedNil *url.Error
	ordinaryCause := fmt.Errorf("ordinary transport wrapper: %w", typedNil)
	executor, err := NewHTTPExecutorFromURL(
		"http://example.com",
		WithHTTPClient(&http.Client{Transport: httpRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, ordinaryCause
		})}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	_, err = executor.Do(context.Background(), "PING")
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Cause == nil {
		t.Fatalf("typed-nil transport error = %v, want HTTPError with cause", err)
	}
	if !strings.Contains(httpErr.Cause.Error(), "ordinary transport wrapper") {
		t.Fatalf("typed-nil transport cause = %q, want original diagnostic", httpErr.Cause)
	}
	if !errors.Is(httpErr.Cause, ordinaryCause) {
		t.Fatalf("typed-nil transport cause no longer preserves original error: %v", httpErr.Cause)
	}
	_ = fmt.Sprintf("%v", err)
	_ = fmt.Sprintf("%#v", err)
}

func TestHTTPNonRedirectDirectTypedNilURLErrorDoesNotPanic(t *testing.T) {
	var typedNil *url.Error
	executor, err := NewHTTPExecutorFromURL(
		"http://example.com",
		WithHTTPClient(&http.Client{Transport: httpRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, typedNil
		})}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	_, err = executor.Do(context.Background(), "PING")
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Cause == nil {
		t.Fatalf("direct typed-nil transport error = %v, want HTTPError with cause", err)
	}
	if !strings.Contains(httpErr.Cause.Error(), "non-redirect transport error") {
		t.Fatalf("direct typed-nil cause = %q, want safe diagnostic", httpErr.Cause)
	}
	if errors.Unwrap(httpErr.Cause) != nil {
		t.Fatal("direct typed-nil cause exposed an unwrap-able raw error")
	}
	_ = err.Error()
	_ = fmt.Sprintf("%+v", err)
	_ = fmt.Sprintf("%#v", err)
}

type identityRedirectTransport struct {
	location string
}

func (t *identityRedirectTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response := testHTTPResponse(http.StatusFound, nil)
	response.Header.Set("Location", t.location)
	response.Request = request
	return response, nil
}

func (*identityRedirectTransport) CloseIdleConnections() {}

func (*identityRedirectTransport) CancelRequest(*http.Request) {}
