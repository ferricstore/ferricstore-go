package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

var redirectSensitiveHeaderNames = []string{
	"Authorization",
	"Www-Authenticate",
	"Cookie",
	"Cookie2",
	"Proxy-Authorization",
	"Proxy-Authenticate",
}

func TestHTTPRedirectScrubsCredentialsOnSameHostHTTPSDowngrade(t *testing.T) {
	var received http.Header
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		received = request.Header.Clone()
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	defer target.Close()
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", target.URL+"/v1/commands")
		writer.WriteHeader(http.StatusFound)
	}))
	defer redirect.Close()

	options := redirectSensitiveHTTPOptions()
	tlsConfig := redirect.Client().Transport.(*http.Transport).TLSClientConfig
	options = append(options, WithHTTPTLSConfig(tlsConfig))
	executor, err := NewHTTPExecutorFromURL(redirect.URL, options...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	assertRedirectSensitiveHeadersEmpty(t, received)
}

func TestHTTPRedirectPreservesCredentialsOnSameHostHTTPSUpgrade(t *testing.T) {
	var received http.Header
	target := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		received = request.Header.Clone()
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", target.URL+"/v1/commands")
		writer.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	options := redirectSensitiveHTTPOptions()
	tlsConfig := target.Client().Transport.(*http.Transport).TLSClientConfig
	options = append(options, WithHTTPTLSConfig(tlsConfig))
	executor, err := NewHTTPExecutorFromURL(redirect.URL, options...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	for _, name := range redirectSensitiveHeaderNames {
		if value := received.Get(name); value == "" {
			t.Errorf("same-host upgrade removed %s", name)
		}
	}
}

func TestHTTPRedirectScrubsCredentialsOnParentToSubdomain(t *testing.T) {
	const (
		parent = "parent.example.test"
		sub    = "sub.parent.example.test"
	)
	var received http.Header
	_, client := newRedirectHostTestServer(t, func(writer http.ResponseWriter, request *http.Request) {
		switch request.Host {
		case parent:
			writer.Header().Set("Location", "http://"+sub+"/v1/commands")
			writer.WriteHeader(http.StatusFound)
		case sub:
			received = request.Header.Clone()
			writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
		default:
			http.Error(writer, "unexpected host", http.StatusBadRequest)
		}
	})

	options := redirectSensitiveHTTPOptions()
	options = append(options, WithHTTPClient(client))
	executor, err := NewHTTPExecutorFromURL("http://"+parent, options...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	assertRedirectSensitiveHeadersEmpty(t, received)
}

func TestHTTPRedirectDoesNotRestoreCredentialsAfterUnsafeHop(t *testing.T) {
	const (
		original = "original.example.test"
		other    = "other.example.test"
	)
	var received http.Header
	_, client := newRedirectHostTestServer(t, func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Host == original && request.URL.Path == "/v1/commands":
			writer.Header().Set("Location", "http://"+other+"/v1/commands")
			writer.WriteHeader(http.StatusFound)
		case request.Host == other:
			writer.Header().Set("Location", "http://"+original+"/returned")
			writer.WriteHeader(http.StatusFound)
		case request.Host == original && request.URL.Path == "/returned":
			received = request.Header.Clone()
			writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
		default:
			http.Error(writer, "unexpected redirect hop", http.StatusBadRequest)
		}
	})

	options := redirectSensitiveHTTPOptions()
	options = append(options, WithHTTPClient(client))
	executor, err := NewHTTPExecutorFromURL("http://"+original, options...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	assertRedirectSensitiveHeadersEmpty(t, received)
}

func TestHTTPRedirectScrubsMixedCaseCredentialsAtEveryUnsafeHop(t *testing.T) {
	const (
		original     = "mixed.example.test"
		intermediate = "sub.mixed.example.test"
	)
	var intermediateReceived http.Header
	var finalReceived http.Header
	_, client := newRedirectHostTestServer(t, func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Host == original && request.URL.Path == "/v1/commands":
			writer.Header().Set("Location", "http://"+intermediate+"/v1/commands")
			writer.WriteHeader(http.StatusFound)
		case request.Host == intermediate:
			intermediateReceived = request.Header.Clone()
			writer.Header().Set("Location", "http://"+original+"/returned")
			writer.WriteHeader(http.StatusFound)
		case request.Host == original && request.URL.Path == "/returned":
			finalReceived = request.Header.Clone()
			writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
		default:
			http.Error(writer, "unexpected redirect hop", http.StatusBadRequest)
		}
	})

	options := redirectSensitiveHTTPOptionsMixedCase()
	options = append(options, WithHTTPClient(client))
	executor, err := NewHTTPExecutorFromURL("http://"+original, options...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	assertRedirectSensitiveHeadersEmpty(t, intermediateReceived)
	assertRedirectSensitiveHeadersEmpty(t, finalReceived)
}

func TestHTTPPostToGetRedirectDoesNotRestoreBodyHeaders(t *testing.T) {
	var received http.Header
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost {
			writer.Header().Set("Location", serverURLForRedirectTest(request)+"/returned")
			writer.WriteHeader(http.StatusFound)
			return
		}
		if request.Method != http.MethodGet {
			t.Errorf("redirected method = %s, want GET", request.Method)
		}
		received = request.Header.Clone()
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	defer server.Close()

	options := []HTTPOption{
		WithHTTPHeaders(http.Header{
			"Content-Encoding": {"encoding-secret"},
			"Content-Language": {"language-secret"},
			"Content-Location": {"location-secret"},
			"Content-Type":     {"type-secret"},
		}),
	}
	executor, err := NewHTTPExecutorFromURL(server.URL, options...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"Content-Encoding", "Content-Language", "Content-Location", "Content-Type", "Content-Length", "Transfer-Encoding",
	} {
		if value := received.Get(name); value != "" {
			t.Errorf("redirected %s = %q, want empty", name, value)
		}
	}
}

func TestHTTPPostToGetRedirectScrubsMixedCaseBodyHeaders(t *testing.T) {
	sourceURL, err := url.Parse("http://example.test/v1/commands")
	if err != nil {
		t.Fatal(err)
	}
	targetURL, err := url.Parse("http://example.test/returned")
	if err != nil {
		t.Fatal(err)
	}
	mixedHeaders := http.Header{
		"cOnTeNt-EnCoDiNg":  {"encoding-secret"},
		"cOnTeNt-LaNgUaGe":  {"language-secret"},
		"cOnTeNt-LoCaTiOn":  {"location-secret"},
		"cOnTeNt-TyPe":      {"type-secret"},
		"cOnTeNt-LeNgTh":    {"length-secret"},
		"tRaNsFeR-EnCoDiNg": {"transfer-secret"},
	}
	initial := &http.Request{Method: http.MethodPost, URL: sourceURL, Header: mixedHeaders}
	redirected := &http.Request{Method: http.MethodGet, URL: targetURL, Header: mixedHeaders.Clone()}
	if err := preserveHTTPRedirectHeaders(redirected, []*http.Request{initial}); err != nil {
		t.Fatal(err)
	}
	for name := range redirected.Header {
		if isHTTPRedirectHeader(name, httpRedirectBodyHeaders[:]) {
			t.Errorf("redirected mixed-case %s header survived, want scrubbed", name)
		}
	}
}

func TestHTTPRedirectRejectsURLUserinfo(t *testing.T) {
	var targetRequests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		targetRequests.Add(1)
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	defer target.Close()
	targetURL, err := url.Parse(target.URL + "/v1/commands")
	if err != nil {
		t.Fatal(err)
	}
	targetURL.User = url.UserPassword("redirect-user", "redirect-secret")

	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", targetURL.String())
		writer.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	executor, err := NewHTTPExecutorFromURL(redirect.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	_, err = executor.Do(context.Background(), "PING")
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Cause == nil || !strings.Contains(httpErr.Cause.Error(), "userinfo") {
		t.Fatalf("redirect with URL userinfo error = %v, want userinfo rejection", err)
	}
	if strings.Contains(httpErr.Cause.Error(), "redirect-secret") {
		t.Fatalf("redirect error exposed URL userinfo secret: %v", httpErr.Cause)
	}
	if targetRequests.Load() != 0 {
		t.Fatalf("URL userinfo redirect reached target %d time(s)", targetRequests.Load())
	}
}

func TestHTTPMalformedRedirectLocationRedactsUserinfo(t *testing.T) {
	const secret = "malformed-location-secret"
	var redirectCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", "http://redirect-user:"+secret+"@%zz")
		writer.WriteHeader(http.StatusFound)
	}))
	defer server.Close()

	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		redirectCalls.Add(1)
		return nil
	}}
	executor, err := NewHTTPExecutorFromURL(server.URL, WithHTTPClient(client))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	_, err = executor.Do(context.Background(), "PING")
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Cause == nil {
		t.Fatalf("malformed redirect error = %v, want HTTPError with cause", err)
	}
	if !strings.Contains(httpErr.Cause.Error(), "failed to parse redirect Location header") {
		t.Fatalf("malformed redirect cause = %q, want safe parse diagnostic", httpErr.Cause)
	}
	for name, text := range map[string]string{
		"error":   err.Error(),
		"cause":   httpErr.Cause.Error(),
		"wrapped": fmt.Sprintf("%+v", err),
	} {
		if strings.Contains(text, secret) {
			t.Errorf("malformed redirect %s exposed secret: %q", name, text)
		}
	}
	if redirectCalls.Load() != 0 {
		t.Fatalf("CheckRedirect called %d time(s) for malformed Location", redirectCalls.Load())
	}
}

func TestHTTPRedirectDoesNotReplayBodyAcrossUnsafeHop(t *testing.T) {
	var targetRequests atomic.Int32
	var targetBody []byte
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		targetRequests.Add(1)
		targetBody, _ = io.ReadAll(request.Body)
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	defer target.Close()
	differentHostURL := strings.Replace(target.URL, "127.0.0.1", "localhost", 1)

	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", differentHostURL+"/v1/commands")
		writer.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	executor, err := NewHTTPExecutorFromURL(redirect.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "SET", "secret-key", "secret-value"); err == nil {
		t.Fatal("unsafe body-preserving redirect succeeded")
	}
	if targetRequests.Load() != 0 || len(targetBody) != 0 {
		t.Fatalf("unsafe redirect replayed body: requests=%d body=%q", targetRequests.Load(), targetBody)
	}
}

func TestHTTPRedirectReplaysBodyAcrossSameHostPortChange(t *testing.T) {
	var targetBody []byte
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		targetBody, _ = io.ReadAll(request.Body)
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", target.URL+"/v1/commands")
		writer.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	executor, err := NewHTTPExecutorFromURL(redirect.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "SET", "key", "value"); err != nil {
		t.Fatal(err)
	}
	if len(targetBody) == 0 {
		t.Fatal("same-host port-change redirect did not replay request body")
	}
}

func TestHTTPRedirectCredentialHopCanonicalizesIPv6Host(t *testing.T) {
	source, err := url.Parse("http://[2001:db8::1]:8080")
	if err != nil {
		t.Fatal(err)
	}
	target, err := url.Parse("https://[2001:0db8:0:0:0:0:0:1]:8443")
	if err != nil {
		t.Fatal(err)
	}
	if !safeHTTPRedirectCredentialHop(source, target) {
		t.Fatal("equivalent IPv6 redirect host was treated as unsafe")
	}

	different, err := url.Parse("https://[2001:db8::2]:8443")
	if err != nil {
		t.Fatal(err)
	}
	if safeHTTPRedirectCredentialHop(source, different) {
		t.Fatal("different IPv6 redirect host was treated as safe")
	}
}

func TestHTTPRedirectCredentialHopRequiresExactIPv6Zone(t *testing.T) {
	tests := []struct {
		name   string
		source string
		target string
		want   bool
	}{
		{
			name:   "identical zone with canonicalized address",
			source: "http://[fe80::1%25en0]:8080",
			target: "https://[FE80:0:0:0:0:0:0:1%25en0]:8443",
			want:   true,
		},
		{
			name:   "zone differs only by case",
			source: "http://[fe80::1%25en0]:8080",
			target: "https://[fe80::1%25EN0]:8443",
		},
		{
			name:   "unscoped canonicalized address",
			source: "http://[2001:db8::1]:8080",
			target: "https://[2001:0db8:0:0:0:0:0:1]:8443",
			want:   true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source, err := url.Parse(test.source)
			if err != nil {
				t.Fatal(err)
			}
			target, err := url.Parse(test.target)
			if err != nil {
				t.Fatal(err)
			}
			if got := safeHTTPRedirectCredentialHop(source, target); got != test.want {
				t.Fatalf("safe redirect hop = %t, want %t", got, test.want)
			}
		})
	}

	source, err := url.Parse("http://[fe80::1%25en0]:8080")
	if err != nil {
		t.Fatal(err)
	}
	target, err := url.Parse("http://[fe80::1%25EN0]:8081")
	if err != nil {
		t.Fatal(err)
	}
	initial := &http.Request{
		Method: http.MethodPost,
		URL:    source,
		Header: http.Header{"Authorization": {"secret"}, "Cookie": {"session=secret"}},
	}
	redirected := &http.Request{
		Method: http.MethodGet,
		URL:    target,
		Header: initial.Header.Clone(),
	}
	if err := preserveHTTPRedirectHeaders(redirected, []*http.Request{initial}); err != nil {
		t.Fatal(err)
	}
	if redirected.Header.Get("Authorization") != "" || redirected.Header.Get("Cookie") != "" {
		t.Fatalf("differently cased IPv6 zone retained credentials: %#v", redirected.Header)
	}

	bodyRedirect := &http.Request{
		Method: http.MethodPost,
		URL:    target,
		Header: initial.Header.Clone(),
		Body:   io.NopCloser(strings.NewReader("secret body")),
	}
	if err := preserveHTTPRedirectHeaders(bodyRedirect, []*http.Request{initial}); err == nil {
		t.Fatal("differently cased IPv6 zone allowed body replay")
	}
}

func TestHTTPRedirectCredentialHopFailsClosedForIDNAAliases(t *testing.T) {
	tests := []struct {
		name   string
		source string
		target string
		want   bool
	}{
		{
			name:   "unicode source to punycode target",
			source: "http://b\u00fccher.example",
			target: "https://xn--bcher-kva.example",
		},
		{
			name:   "exact unicode host",
			source: "http://b\u00fccher.example",
			target: "https://b\u00fccher.example",
			want:   true,
		},
		{
			name:   "punycode source to unicode target",
			source: "http://xn--bcher-kva.example",
			target: "https://b\u00fccher.example",
		},
		{
			name:   "unicode case variant",
			source: "http://B\u00dcCHER.example",
			target: "https://b\u00fccher.example",
		},
		{
			name:   "punycode case variant",
			source: "http://XN--BCHER-KVA.EXAMPLE",
			target: "https://xn--bcher-kva.example",
			want:   true,
		},
		{
			name:   "trailing dot",
			source: "http://xn--bcher-kva.example.",
			target: "https://xn--bcher-kva.example",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source, err := url.Parse(test.source)
			if err != nil {
				t.Fatal(err)
			}
			target, err := url.Parse(test.target)
			if err != nil {
				t.Fatal(err)
			}
			if got := safeHTTPRedirectCredentialHop(source, target); got != test.want {
				t.Fatalf("safe redirect hop = %t, want %t", got, test.want)
			}
		})
	}
}

func TestHTTPRedirectScrubsRefererAfterUnsafeHop(t *testing.T) {
	var receivedReferer string
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedReferer = request.Header.Get("Referer")
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	defer target.Close()
	differentHostURL := strings.Replace(target.URL, "127.0.0.1", "localhost", 1)
	intermediate := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", differentHostURL+"/v1/commands")
		writer.WriteHeader(http.StatusFound)
	}))
	defer intermediate.Close()
	source := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", intermediate.URL+"/secret?token=secret")
		writer.WriteHeader(http.StatusFound)
	}))
	defer source.Close()

	executor, err := NewHTTPExecutorFromURL(source.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	if receivedReferer != "" {
		t.Fatalf("unsafe redirected Referer = %q, want empty", receivedReferer)
	}
}

func TestHTTPRejectsMixedCaseAuthorizationCredentialConflict(t *testing.T) {
	if _, err := NewHTTPExecutorFromURL(
		"https://example.com",
		WithHTTPBearerToken("bearer-secret"),
		WithHTTPHeaders(http.Header{"aUtHoRiZaTiOn": {"caller-secret"}}),
	); err == nil {
		t.Fatal("mixed-case Authorization credential conflict was accepted")
	}
}

func TestHTTPCustomClientRetainsCallerRedirectPolicy(t *testing.T) {
	var callbackAuthorization string
	var targetAuthorization string
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		targetAuthorization = request.Header.Get("Authorization")
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	defer target.Close()
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", target.URL+"/v1/commands")
		writer.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	client := redirect.Client()
	client.CheckRedirect = func(request *http.Request, _ []*http.Request) error {
		callbackAuthorization = request.Header.Get("Authorization")
		return nil
	}
	executor, err := NewHTTPExecutorFromURL(
		redirect.URL,
		WithHTTPClient(client),
		WithHTTPBearerToken("secret"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	if callbackAuthorization != "Bearer secret" || targetAuthorization != "Bearer secret" {
		t.Fatalf("custom redirect policy authorization = callback %q, target %q; want both retained", callbackAuthorization, targetAuthorization)
	}
}

func TestHTTPCustomClientRetainsCallerCookieJar(t *testing.T) {
	var targetCookie string
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		targetCookie = request.Header.Get("Cookie")
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	defer target.Close()
	differentHostURL := strings.Replace(target.URL, "127.0.0.1", "localhost", 1)
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", differentHostURL+"/v1/commands")
		writer.WriteHeader(http.StatusFound)
	}))
	defer redirect.Close()

	executor, err := NewHTTPExecutorFromURL(
		redirect.URL,
		WithHTTPClient(&http.Client{Jar: permissiveHTTPJar{}}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	if targetCookie != "caller=controlled" {
		t.Fatalf("custom cookie jar sent %q, want caller=controlled", targetCookie)
	}
}

func TestHTTPDefaultRedirectPolicyStopsAfterTenRequests(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		hop := requests.Add(1) - 1
		if hop < 11 {
			writer.Header().Set("Location", fmt.Sprintf("%s/v1/commands?hop=%d", serverURLForRedirectTest(request), hop+1))
			writer.WriteHeader(http.StatusFound)
			return
		}
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	defer server.Close()

	executor, err := NewHTTPExecutorFromURL(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "PING"); err == nil {
		t.Fatal("default redirect policy followed more than ten requests")
	}
	if got := requests.Load(); got != 10 {
		t.Fatalf("requests before default redirect stop = %d, want 10", got)
	}
}

func TestHTTPCustomClientCanChooseMoreThanTenRedirects(t *testing.T) {
	var requests atomic.Int32
	var redirectCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		hop := requests.Add(1) - 1
		if hop < 11 {
			writer.Header().Set("Location", fmt.Sprintf("%s/v1/commands?hop=%d", serverURLForRedirectTest(request), hop+1))
			writer.WriteHeader(http.StatusFound)
			return
		}
		writeHTTPJSON(t, writer, http.StatusOK, httpSuccessEnvelope("PONG"))
	}))
	defer server.Close()

	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		redirectCalls.Add(1)
		return nil
	}}
	executor, err := NewHTTPExecutorFromURL(server.URL, WithHTTPClient(client))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executor.Close() }()
	if _, err := executor.Do(context.Background(), "PING"); err != nil {
		t.Fatal(err)
	}
	if got := redirectCalls.Load(); got != 11 {
		t.Fatalf("custom redirect callbacks = %d, want 11", got)
	}
}

func redirectSensitiveHTTPOptions() []HTTPOption {
	return []HTTPOption{
		WithHTTPBearerToken("authorization-secret"),
		WithHTTPHeaders(http.Header{
			"Www-Authenticate":    {"www-authenticate-secret"},
			"Cookie":              {"cookie-secret"},
			"Cookie2":             {"cookie2-secret"},
			"Proxy-Authorization": {"proxy-authorization-secret"},
			"Proxy-Authenticate":  {"proxy-authenticate-secret"},
		}),
	}
}

type permissiveHTTPJar struct{}

func (permissiveHTTPJar) Cookies(*url.URL) []*http.Cookie {
	return []*http.Cookie{{Name: "caller", Value: "controlled"}}
}

func (permissiveHTTPJar) SetCookies(*url.URL, []*http.Cookie) {}

func redirectSensitiveHTTPOptionsMixedCase() []HTTPOption {
	return []HTTPOption{
		WithHTTPHeaders(http.Header{
			"aUtHoRiZaTiOn":       {"authorization-secret"},
			"wWw-AuThEnTiCaTe":    {"www-authenticate-secret"},
			"cOoKiE":              {"cookie-secret"},
			"cOoKiE2":             {"cookie2-secret"},
			"pRoXy-AuThOrIzAtIoN": {"proxy-authorization-secret"},
			"pRoXy-AuThEnTiCaTe":  {"proxy-authenticate-secret"},
		}),
	}
}

func assertRedirectSensitiveHeadersEmpty(t *testing.T, received http.Header) {
	t.Helper()
	for _, name := range redirectSensitiveHeaderNames {
		if value := received.Get(name); value != "" {
			t.Errorf("redirected %s = %q, want empty", name, value)
		}
	}
}

func newRedirectHostTestServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *http.Client) {
	t.Helper()
	server := httptest.NewServer(handler)
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, network, server.Listener.Addr().String())
		},
	}
	t.Cleanup(func() { transport.CloseIdleConnections() })
	t.Cleanup(server.Close)
	return server, &http.Client{Transport: transport, CheckRedirect: preserveHTTPRedirectHeaders}
}

func serverURLForRedirectTest(request *http.Request) string {
	return "http://" + request.Host
}
