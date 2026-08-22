package ferricstore

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/textproto"
	"strings"
	"time"
)

const (
	defaultHTTPTimeout          = 30 * time.Second
	defaultHTTPMaxRequestBytes  = 1024 * 1024
	defaultHTTPMaxResponseBytes = 16 * 1024 * 1024
	defaultHTTPMaxBatchCommands = 1000
	defaultHTTPMaxConnections   = 100
)

// HTTPOption configures an HTTPExecutor.
type HTTPOption func(*httpOptions)

type httpOptions struct {
	Client           *http.Client
	Headers          http.Header
	Timeout          time.Duration
	MaxRequestBytes  int64
	MaxResponseBytes int64
	MaxBatchCommands int
	MaxConnections   int
	HTTP2            bool
	TLSConfig        *tls.Config

	bearerToken string
	username    string
	password    string
	basicSet    bool
}

func defaultHTTPOptions() httpOptions {
	return httpOptions{
		Headers:          make(http.Header),
		Timeout:          defaultHTTPTimeout,
		MaxRequestBytes:  defaultHTTPMaxRequestBytes,
		MaxResponseBytes: defaultHTTPMaxResponseBytes,
		MaxBatchCommands: defaultHTTPMaxBatchCommands,
		MaxConnections:   defaultHTTPMaxConnections,
		HTTP2:            true,
	}
}

// WithHTTPOptions applies HTTP-specific options to a Client created from an
// http:// or https:// URL. It has no effect on injected or native executors.
func WithHTTPOptions(opts ...HTTPOption) ClientOption {
	return func(client *Client) {
		if !client.ownsTransportConfiguration {
			return
		}
		executor, ok := client.exec.(*HTTPExecutor)
		if !ok || executor == nil {
			return
		}
		applyHTTPOptions(&executor.opts, opts...)
		executor.configure()
	}
}

func WithHTTPClient(client *http.Client) HTTPOption {
	return func(options *httpOptions) { options.Client = client }
}

func WithHTTPHeaders(headers http.Header) HTTPOption {
	return func(options *httpOptions) { options.Headers = cloneHTTPHeader(headers) }
}

func WithHTTPBearerToken(token string) HTTPOption {
	return func(options *httpOptions) { options.bearerToken = token }
}

func WithHTTPBasicAuth(username, password string) HTTPOption {
	return func(options *httpOptions) {
		options.username = username
		options.password = password
		options.basicSet = true
	}
}

func WithHTTPTimeout(timeout time.Duration) HTTPOption {
	return func(options *httpOptions) { options.Timeout = timeout }
}

func WithHTTPMaxRequestBytes(limit int64) HTTPOption {
	return func(options *httpOptions) { options.MaxRequestBytes = limit }
}

func WithHTTPMaxResponseBytes(limit int64) HTTPOption {
	return func(options *httpOptions) { options.MaxResponseBytes = limit }
}

func WithHTTPMaxBatchCommands(limit int) HTTPOption {
	return func(options *httpOptions) { options.MaxBatchCommands = limit }
}

// WithHTTPMaxConnections bounds active and idle connections to one endpoint.
// Waiting for capacity is included in the command timeout.
func WithHTTPMaxConnections(limit int) HTTPOption {
	return func(options *httpOptions) { options.MaxConnections = limit }
}

// WithHTTP2 controls HTTP/2 negotiation for the SDK-owned transport. HTTP/1.1
// remains available either way and is used when the endpoint does not offer H2.
func WithHTTP2(enabled bool) HTTPOption {
	return func(options *httpOptions) { options.HTTP2 = enabled }
}

func WithHTTPTLSConfig(config *tls.Config) HTTPOption {
	return func(options *httpOptions) { options.TLSConfig = config }
}

func applyHTTPOptions(options *httpOptions, opts ...HTTPOption) {
	for _, option := range opts {
		if option != nil {
			option(options)
		}
	}
}

func validateHTTPOptions(options httpOptions, secure bool) error {
	if options.Timeout <= 0 {
		return errors.New("ferricstore HTTP timeout must be positive")
	}
	if options.MaxRequestBytes <= 0 || options.MaxResponseBytes <= 0 {
		return errors.New("ferricstore HTTP request and response byte limits must be positive")
	}
	if options.MaxBatchCommands <= 0 {
		return errors.New("ferricstore HTTP max batch commands must be positive")
	}
	if options.MaxConnections <= 0 {
		return errors.New("ferricstore HTTP max connections must be positive")
	}
	if err := validateHTTPHeaders(options.Headers); err != nil {
		return err
	}
	hasAuthorization := options.Headers.Get("Authorization") != ""
	if options.bearerToken != "" {
		if options.basicSet || hasAuthorization {
			return errors.New("ferricstore HTTP credentials are mutually exclusive")
		}
		if containsHTTPNewline(options.bearerToken) {
			return errors.New("ferricstore HTTP bearer token cannot contain newlines")
		}
	}
	if options.basicSet {
		if !secure {
			return errors.New("ferricstore HTTP Basic credentials require an https:// URL")
		}
		if hasAuthorization || options.bearerToken != "" {
			return errors.New("ferricstore HTTP credentials are mutually exclusive")
		}
		username := options.username
		if username == "" {
			username = "default"
		}
		if strings.Contains(username, ":") {
			return errors.New("ferricstore HTTP Basic username cannot contain ':'")
		}
		if containsHTTPNewline(username) || containsHTTPNewline(options.password) {
			return errors.New("ferricstore HTTP Basic credentials cannot contain newlines")
		}
	}
	if options.Client != nil && options.TLSConfig != nil {
		return errors.New("ferricstore HTTP custom client and TLS config are mutually exclusive")
	}
	return nil
}

func validateHTTPHeaders(headers http.Header) error {
	for name, values := range headers {
		if name == "" || textproto.CanonicalMIMEHeaderKey(name) == "" || containsHTTPNewline(name) {
			return fmt.Errorf("ferricstore HTTP header name %q is invalid", name)
		}
		for _, value := range values {
			if containsHTTPNewline(value) {
				return fmt.Errorf("ferricstore HTTP header %q contains a newline", name)
			}
		}
	}
	return nil
}

func containsHTTPNewline(value string) bool {
	return strings.ContainsAny(value, "\r\n")
}

func cloneHTTPHeader(header http.Header) http.Header {
	if header == nil {
		return make(http.Header)
	}
	return header.Clone()
}
