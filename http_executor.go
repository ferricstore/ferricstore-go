package ferricstore

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrHTTPConnectionAffineCommand = errors.New("command requires the native TCP transport")

// HTTPError is a stable HTTP transport, gateway, or command failure.
type HTTPError struct {
	StatusCode   int
	Code         string
	Message      string
	Retryable    bool
	SafeToRetry  bool
	RetryAfterMS int64
	Details      map[string]any
	Cause        error
}

func (e *HTTPError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	return "ferricstore HTTP " + e.Code
}

func (e *HTTPError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// HTTPExecutor executes the SDK's unchanged command surface through the
// FerricStore stateless HTTP/HTTPS batch endpoint.
type HTTPExecutor struct {
	baseURL string
	secure  bool
	opts    httpOptions

	mu        sync.RWMutex
	client    *http.Client
	transport *http.Transport
	configErr error
	closed    bool
}

func NewHTTPExecutorFromURL(rawURL string, opts ...HTTPOption) (*HTTPExecutor, error) {
	baseURL, secure, err := parseHTTPBaseURL(rawURL)
	if err != nil {
		return nil, err
	}
	executor := &HTTPExecutor{baseURL: baseURL, secure: secure, opts: defaultHTTPOptions()}
	applyHTTPOptions(&executor.opts, opts...)
	executor.configure()
	if executor.configErr != nil {
		return nil, executor.configErr
	}
	return executor, nil
}

func (e *HTTPExecutor) configure() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.configErr = validateHTTPOptions(e.opts, e.secure)
	if e.configErr != nil {
		return
	}
	if e.opts.Client != nil {
		if e.transport != nil {
			e.transport.CloseIdleConnections()
		}
		client := *e.opts.Client
		callerRedirect := client.CheckRedirect
		client.CheckRedirect = func(request *http.Request, via []*http.Request) error {
			if err := preserveHTTPRedirectHeaders(request, via); err != nil {
				return err
			}
			if callerRedirect != nil {
				return callerRedirect(request, via)
			}
			return nil
		}
		e.client = &client
		e.transport = nil
		return
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = e.opts.MaxConnections
	transport.MaxIdleConnsPerHost = e.opts.MaxConnections
	transport.MaxConnsPerHost = e.opts.MaxConnections
	transport.IdleConnTimeout = 90 * time.Second
	transport.ForceAttemptHTTP2 = e.opts.HTTP2
	if !e.opts.HTTP2 {
		transport.TLSNextProto = make(map[string]func(string, *tls.Conn) http.RoundTripper)
	}
	if e.opts.TLSConfig != nil {
		transport.TLSClientConfig = e.opts.TLSConfig.Clone()
	}
	if e.transport != nil {
		e.transport.CloseIdleConnections()
	}
	e.transport = transport
	e.client = &http.Client{Transport: transport, CheckRedirect: preserveHTTPRedirectHeaders}
}

func (e *HTTPExecutor) Do(ctx context.Context, args ...any) (any, error) {
	results, err := e.executeBatch(ctx, [][]any{args})
	if err != nil {
		return nil, err
	}
	if results[0].err != nil {
		return nil, results[0].err
	}
	return results[0].value, nil
}

func (e *HTTPExecutor) Pipeline(ctx context.Context, commands [][]any) ([]any, error) {
	results, err := e.pipelineDetailed(ctx, commands)
	if err != nil {
		return nil, err
	}
	return pipelineResultValues(results)
}

func (*HTTPExecutor) supportsNativeRequestContextArguments() {}

func (e *HTTPExecutor) pipelineDetailed(
	ctx context.Context,
	commands [][]any,
) ([]pipelineItemResult, error) {
	return e.executeBatch(ctx, commands)
}

func (e *HTTPExecutor) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	transport := e.transport
	e.mu.Unlock()
	if transport != nil {
		transport.CloseIdleConnections()
	}
	return nil
}

func (e *HTTPExecutor) executeBatch(
	ctx context.Context,
	commands [][]any,
) ([]pipelineItemResult, error) {
	if len(commands) == 0 {
		return nil, nil
	}
	client, options, err := e.snapshot()
	if err != nil {
		return nil, err
	}
	if len(commands) > options.MaxBatchCommands {
		return nil, &HTTPError{Code: "too_many_commands", Message: fmt.Sprintf(
			"ferricstore HTTP batch has %d commands; limit is %d", len(commands), options.MaxBatchCommands,
		)}
	}
	encoded := make([]any, len(commands))
	encodeState := newHTTPEncodeState(options.MaxRequestBytes)
	for index, command := range commands {
		encoded[index], err = encodeHTTPCommandWithState(command, encodeState)
		if err != nil {
			if errors.Is(err, errHTTPRequestEncodingBudget) {
				return nil, &HTTPError{Code: "request_too_large", Message: "FerricStore HTTP request exceeds max request bytes"}
			}
			return nil, err
		}
	}
	body, err := json.Marshal(map[string]any{"encoding": httpBinaryEncoding, "commands": encoded})
	if err != nil {
		return nil, fmt.Errorf("encode FerricStore HTTP request: %w", err)
	}
	if int64(len(body)) > options.MaxRequestBytes {
		return nil, &HTTPError{Code: "request_too_large", Message: "FerricStore HTTP request exceeds max request bytes"}
	}
	effectiveTimeout := nativeEffectiveTimeout(options.Timeout, pipelineBlockingBudget(commands))
	requestContext, cancel := context.WithCancel(ctx)
	if effectiveTimeout > 0 {
		requestContext, cancel = context.WithTimeout(ctx, effectiveTimeout)
	}
	defer cancel()
	request, err := http.NewRequestWithContext(
		requestContext, http.MethodPost, e.baseURL+"/v1/commands", bytes.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf("create FerricStore HTTP request: %w", err)
	}
	request.Header = requestHeaders(options)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := client.Do(request)
	if err != nil {
		code, retryable := classifyHTTPTransportError(err, requestContext.Err())
		return nil, &HTTPError{
			Code: code, Message: "FerricStore HTTP request failed", Retryable: retryable, Cause: err,
		}
	}
	defer func() { _ = response.Body.Close() }()
	decoded, err := readHTTPResponse(response, options.MaxResponseBytes, requestContext)
	if err != nil {
		if response.StatusCode != http.StatusOK {
			var httpErr *HTTPError
			if errors.As(err, &httpErr) && httpErr.Code == "response_too_large" {
				return nil, err
			}
			return nil, &HTTPError{
				StatusCode: response.StatusCode,
				Code:       "http_error",
				Message:    "FerricStore HTTP endpoint returned " + response.Status,
				Cause:      err,
			}
		}
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, topLevelHTTPError(response, decoded)
	}
	return decodeHTTPResults(decoded, len(commands))
}

func (e *HTTPExecutor) snapshot() (*http.Client, httpOptions, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return nil, httpOptions{}, &HTTPError{Code: "closed", Message: "FerricStore HTTP executor is closed"}
	}
	if e.configErr != nil {
		return nil, httpOptions{}, e.configErr
	}
	if e.client == nil {
		return nil, httpOptions{}, errors.New("ferricstore HTTP client is not configured")
	}
	options := e.opts
	options.Headers = cloneHTTPHeader(e.opts.Headers)
	return e.client, options, nil
}

func readHTTPResponse(response *http.Response, limit int64, ctx context.Context) (map[string]any, error) {
	if response.ContentLength > limit {
		return nil, &HTTPError{StatusCode: response.StatusCode, Code: "response_too_large", Message: "FerricStore HTTP response exceeds max response bytes"}
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		code, retryable := classifyHTTPTransportError(err, ctx.Err())
		return nil, &HTTPError{
			StatusCode: response.StatusCode, Code: code,
			Message: "FerricStore HTTP response read failed", Retryable: retryable, Cause: err,
		}
	}
	if int64(len(body)) > limit {
		return nil, &HTTPError{StatusCode: response.StatusCode, Code: "response_too_large", Message: "FerricStore HTTP response exceeds max response bytes"}
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var envelope map[string]any
	if err := decoder.Decode(&envelope); err != nil {
		return nil, &HTTPError{StatusCode: response.StatusCode, Code: "invalid_response", Message: "FerricStore HTTP response is not valid JSON", Cause: err}
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return nil, &HTTPError{StatusCode: response.StatusCode, Code: "invalid_response", Message: "FerricStore HTTP response contains trailing JSON"}
	}
	return envelope, nil
}

func decodeHTTPResults(envelope map[string]any, expected int) ([]pipelineItemResult, error) {
	if encoding, exists := envelope["encoding"]; exists && encoding != httpBinaryEncoding {
		return nil, &HTTPError{StatusCode: 200, Code: "invalid_response", Message: "FerricStore HTTP response uses an unknown encoding"}
	}
	values, ok := envelope["results"].([]any)
	if !ok || len(values) != expected {
		return nil, &HTTPError{StatusCode: 200, Code: "invalid_response", Message: "FerricStore HTTP response result count is invalid"}
	}
	results := make([]pipelineItemResult, len(values))
	for index, raw := range values {
		item, ok := raw.(map[string]any)
		if !ok {
			return nil, &HTTPError{StatusCode: 200, Code: "invalid_response", Message: "FerricStore HTTP command result is not an object"}
		}
		switch item["status"] {
		case "ok":
			decoded, err := decodeHTTPValue(item["value"])
			if err != nil {
				return nil, &HTTPError{StatusCode: 200, Code: "invalid_response", Message: err.Error(), Cause: err}
			}
			results[index].value = decoded
		case "error":
			if _, valid := item["error"].(map[string]any); !valid {
				return nil, &HTTPError{StatusCode: 200, Code: "invalid_response", Message: "FerricStore HTTP command error is not an object"}
			}
			results[index].err = commandHTTPError(item)
		default:
			return nil, &HTTPError{StatusCode: 200, Code: "invalid_response", Message: "FerricStore HTTP command result has an invalid status"}
		}
	}
	return results, nil
}

func commandHTTPError(item map[string]any) error {
	details, _ := item["error"].(map[string]any)
	return newHTTPError(200, details, "upstream_error")
}

func topLevelHTTPError(response *http.Response, envelope map[string]any) error {
	details, _ := envelope["error"].(map[string]any)
	errorValue := newHTTPError(response.StatusCode, details, "http_"+strconv.Itoa(response.StatusCode))
	if errorValue.RetryAfterMS == 0 {
		errorValue.RetryAfterMS = parseHTTPRetryAfter(response.Header.Get("Retry-After"), time.Now())
	}
	return errorValue
}

func classifyHTTPTransportError(err, contextErr error) (string, bool) {
	switch {
	case errors.Is(err, context.Canceled) || errors.Is(contextErr, context.Canceled):
		return "transport_canceled", false
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(contextErr, context.DeadlineExceeded):
		return "transport_timeout", true
	default:
		return "transport_error", true
	}
}

func parseHTTPRetryAfter(value string, now time.Time) int64 {
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds > 0 {
		if seconds > (1<<63-1)/1_000 {
			return 1<<63 - 1
		}
		return seconds * 1_000
	}
	date, err := http.ParseTime(value)
	if err != nil || !date.After(now) {
		return 0
	}
	return date.Sub(now).Milliseconds()
}

func newHTTPError(status int, details map[string]any, fallbackCode string) *HTTPError {
	code, _ := details["code"].(string)
	if code == "" {
		code = fallbackCode
	}
	message, _ := details["message"].(string)
	if message == "" {
		message = strings.ReplaceAll(code, "_", " ")
	}
	retryable, _ := details["retryable"].(bool)
	safe, _ := details["safe_to_retry"].(bool)
	retryAfter := httpNonNegativeInt64(details["retry_after_ms"])
	return &HTTPError{
		StatusCode: status, Code: code, Message: message, Retryable: retryable,
		SafeToRetry: safe, RetryAfterMS: retryAfter, Details: details,
	}
}

func httpNonNegativeInt64(value any) int64 {
	switch typed := value.(type) {
	case json.Number:
		parsed, _ := strconv.ParseInt(typed.String(), 10, 64)
		if parsed >= 0 {
			return parsed
		}
	case int64:
		if typed >= 0 {
			return typed
		}
	}
	return 0
}

func requestHeaders(options httpOptions) http.Header {
	headers := cloneHTTPHeader(options.Headers)
	if options.bearerToken != "" {
		headers.Set("Authorization", "Bearer "+options.bearerToken)
	} else if options.basicSet {
		username := options.username
		if username == "" {
			username = "default"
		}
		credentials := base64.StdEncoding.EncodeToString([]byte(username + ":" + options.password))
		headers.Set("Authorization", "Basic "+credentials)
	}
	return headers
}

func preserveHTTPRedirectHeaders(request *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	if len(via) == 0 {
		return nil
	}
	for name, values := range via[0].Header {
		if request.Method == http.MethodGet && (strings.EqualFold(name, "Content-Type") ||
			strings.EqualFold(name, "Content-Length") || strings.EqualFold(name, "Transfer-Encoding")) {
			continue
		}
		if request.Header.Values(name) == nil {
			request.Header[name] = append([]string(nil), values...)
		}
	}
	return nil
}

func parseHTTPBaseURL(rawURL string) (string, bool, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		// net/url includes the complete input in parse errors. HTTP URLs may
		// contain mistakenly embedded credentials, so do not disclose it.
		return "", false, errors.New("invalid FerricStore HTTP URL")
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", false, errors.New("FerricStore HTTP URLs must use http:// or https://")
	}
	if parsed.Hostname() == "" {
		return "", false, errors.New("FerricStore HTTP URL must include a host")
	}
	rawPort := parsed.Port()
	if rawPort != "" {
		port, portErr := strconv.Atoi(rawPort)
		if portErr != nil || port < 1 || port > 65_535 {
			return "", false, errors.New("FerricStore HTTP URL port is outside the TCP port range")
		}
	} else if strings.HasSuffix(parsed.Host, ":") {
		return "", false, errors.New("FerricStore HTTP URL has an empty port")
	}
	if parsed.User != nil {
		return "", false, errors.New("FerricStore HTTP credentials must use explicit options, not URL user info")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", false, errors.New("FerricStore HTTP URL cannot contain a query or fragment")
	}
	return strings.TrimRight(parsed.String(), "/"), scheme == "https", nil
}
