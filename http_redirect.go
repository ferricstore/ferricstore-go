package ferricstore

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
)

var httpRedirectBodyHeaders = [...]string{
	"Content-Encoding",
	"Content-Language",
	"Content-Location",
	"Content-Type",
	"Content-Length",
	"Transfer-Encoding",
}

type httpRedirectPolicyError struct {
	message string
}

type httpRedirectState struct {
	redirected  atomic.Bool
	resolvedURL atomic.Pointer[url.URL]
}

type httpRedirectStateContextKey struct{}

// httpRedirectSanitizedError intentionally does not unwrap the original
// redirect error. Its text is the only safe representation of that error.
type httpRedirectSanitizedError struct {
	message string
}

// httpNilSafeError is used only for malformed non-redirect error chains that
// contain a typed-nil error. It keeps the diagnostic and exact cause match
// without exposing a chain that standard errors.Is/errors.As can panic on.
type httpNilSafeError struct {
	cause   error
	message string
}

func (e *httpRedirectSanitizedError) Error() string {
	return e.message
}

func (e *httpRedirectSanitizedError) Format(state fmt.State, verb rune) {
	if verb == 'q' {
		_, _ = fmt.Fprintf(state, "%q", e.message)
		return
	}
	_, _ = io.WriteString(state, e.message)
}

func (e *httpNilSafeError) Error() string {
	return e.message
}

func (e *httpNilSafeError) Is(target error) bool {
	return safeHTTPErrorIs(e.cause, target)
}

func (e *httpNilSafeError) Format(state fmt.State, verb rune) {
	if verb == 'q' {
		_, _ = fmt.Fprintf(state, "%q", e.message)
		return
	}
	_, _ = io.WriteString(state, e.message)
}

func (e *httpRedirectPolicyError) Error() string {
	return e.message
}

func newHTTPRedirectPolicyError(message string) error {
	return &httpRedirectPolicyError{message: message}
}

func sanitizeHTTPRedirectCause(_ error, state *httpRedirectState) error {
	message := "ferricstore HTTP redirect failed"
	if state != nil {
		if resolvedURL := state.resolvedURL.Load(); resolvedURL != nil {
			if safeURL := sanitizeHTTPRedirectURL(resolvedURL.String()); safeURL != httpRedirectRedactedURL {
				message += " for " + safeURL
			}
		}
	}
	return &httpRedirectSanitizedError{message: message}
}

func shouldSanitizeHTTPRedirectCause(err error, state *httpRedirectState) bool {
	if state != nil && state.redirected.Load() {
		return true
	}
	var urlErr *url.Error
	if !errors.As(err, &urlErr) || urlErr == nil {
		return false
	}
	if urlErr.Err != nil && !isHTTPNilError(urlErr.Err) && strings.HasPrefix(urlErr.Err.Error(), "failed to parse Location header") {
		if state != nil {
			state.redirected.Store(true)
		}
		return true
	}
	return false
}

func newHTTPNilSafeError(err error) error {
	return &httpNilSafeError{cause: err, message: safeHTTPErrorText(err)}
}

func safeHTTPErrorText(err error) (message string) {
	if isHTTPNilError(err) || hasHTTPDirectNilCause(err) {
		return "non-redirect transport error"
	}
	defer func() {
		if recover() != nil {
			message = "non-redirect transport error"
		}
	}()
	return err.Error()
}

func hasHTTPDirectNilCause(err error) bool {
	urlErr, ok := err.(*url.Error)
	return ok && urlErr != nil && isHTTPNilError(urlErr.Err)
}

func hasHTTPTypedNilError(err error) bool {
	if err == nil {
		return false
	}
	if isHTTPNilError(err) {
		return true
	}
	switch unwrapped := err.(type) {
	case interface{ Unwrap() error }:
		return hasHTTPTypedNilError(unwrapped.Unwrap())
	case interface{ Unwrap() []error }:
		for _, child := range unwrapped.Unwrap() {
			if hasHTTPTypedNilError(child) {
				return true
			}
		}
	}
	return false
}

func isHTTPNilError(err error) bool {
	if err == nil {
		return false
	}
	value := reflect.ValueOf(err)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func sameHTTPErrorValue(left, right error) bool {
	if left == nil || right == nil {
		return left == right
	}
	leftType := reflect.TypeOf(left)
	return leftType == reflect.TypeOf(right) && leftType.Comparable() && left == right
}

func safeHTTPErrorIs(err, target error) bool {
	if err == nil || target == nil {
		return err == target
	}
	if sameHTTPErrorValue(err, target) {
		return true
	}
	if isHTTPNilError(err) {
		return false
	}
	if matcher, ok := err.(interface{ Is(error) bool }); ok && matcher.Is(target) {
		return true
	}
	switch unwrapped := err.(type) {
	case interface{ Unwrap() error }:
		return safeHTTPErrorIs(unwrapped.Unwrap(), target)
	case interface{ Unwrap() []error }:
		for _, child := range unwrapped.Unwrap() {
			if safeHTTPErrorIs(child, target) {
				return true
			}
		}
	}
	return false
}

func markHTTPRedirectState(request *http.Request) {
	if request == nil {
		return
	}
	state, ok := request.Context().Value(httpRedirectStateContextKey{}).(*httpRedirectState)
	if !ok {
		return
	}
	state.redirected.Store(true)
	if request.URL != nil {
		resolvedURL := *request.URL
		state.resolvedURL.Store(&resolvedURL)
	}
}

func preserveHTTPRedirectHeaders(request *http.Request, via []*http.Request) error {
	if len(via) > 0 {
		markHTTPRedirectState(request)
	}
	if len(via) >= 10 {
		return newHTTPRedirectPolicyError("stopped after 10 redirects")
	}
	if len(via) == 0 {
		return nil
	}
	if request.URL != nil && request.URL.User != nil {
		return newHTTPRedirectPolicyError("redirect target must not contain URL userinfo")
	}
	if request.Header == nil {
		request.Header = make(http.Header)
	}
	crossOriginHop := hasUnsafeHTTPRedirectHop(via, request)
	if crossOriginHop && request.Body != nil && request.Body != http.NoBody {
		return newHTTPRedirectPolicyError("refusing to replay request body across unsafe redirect")
	}
	if crossOriginHop {
		// net/http adds transport-managed essentials after CheckRedirect. Do not
		// carry any caller or SDK header across a different origin.
		request.Header = make(http.Header)
	}
	if request.Method == http.MethodGet {
		scrubHTTPRedirectHeaders(request.Header, httpRedirectBodyHeaders[:])
	}
	for name, values := range via[0].Header {
		if request.Method == http.MethodGet && isHTTPRedirectHeader(name, httpRedirectBodyHeaders[:]) {
			continue
		}
		if crossOriginHop {
			continue
		}
		if request.Header.Values(name) == nil {
			request.Header[name] = append([]string(nil), values...)
		}
	}
	return nil
}

func hasUnsafeHTTPRedirectHop(via []*http.Request, request *http.Request) bool {
	// Check the complete chain because net/http keeps sensitive-header stripping
	// sticky after an unsafe hop, even when a later hop returns to the origin.
	previous := via[0]
	for _, current := range via[1:] {
		if !safeHTTPRedirectCredentialHop(requestURL(previous), requestURL(current)) {
			return true
		}
		previous = current
	}
	return !safeHTTPRedirectCredentialHop(requestURL(previous), requestURL(request))
}

func requestURL(request *http.Request) *url.URL {
	if request == nil {
		return nil
	}
	return request.URL
}

func safeHTTPRedirectCredentialHop(source, target *url.URL) bool {
	sourceOrigin, sourceOK := httpRedirectOriginForURL(source)
	targetOrigin, targetOK := httpRedirectOriginForURL(target)
	if !sourceOK || !targetOK {
		return false
	}
	return sourceOrigin.scheme == targetOrigin.scheme &&
		sourceOrigin.port == targetOrigin.port &&
		sameHTTPRedirectHost(sourceOrigin.host, targetOrigin.host)
}

type httpRedirectOrigin struct {
	scheme string
	host   string
	port   uint16
}

func httpRedirectOriginForURL(rawURL *url.URL) (httpRedirectOrigin, bool) {
	if rawURL == nil || rawURL.User != nil {
		return httpRedirectOrigin{}, false
	}
	scheme := strings.ToLower(rawURL.Scheme)
	if scheme != "http" && scheme != "https" {
		return httpRedirectOrigin{}, false
	}
	host := rawURL.Hostname()
	if host == "" {
		return httpRedirectOrigin{}, false
	}
	port := rawURL.Port()
	if port == "" {
		if scheme == "http" {
			return httpRedirectOrigin{scheme: scheme, host: host, port: 80}, true
		}
		return httpRedirectOrigin{scheme: scheme, host: host, port: 443}, true
	}
	parsedPort, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return httpRedirectOrigin{}, false
	}
	return httpRedirectOrigin{scheme: scheme, host: host, port: uint16(parsedPort)}, true
}

func sameHTTPRedirectHost(source, target string) bool {
	sourceIP, sourceZone, sourceIsIPv6 := parseHTTPRedirectIPv6(source)
	targetIP, targetZone, targetIsIPv6 := parseHTTPRedirectIPv6(target)
	if sourceIsIPv6 || targetIsIPv6 {
		return sourceIsIPv6 && targetIsIPv6 && sourceZone == targetZone && sourceIP == targetIP
	}
	if source == target {
		return true
	}
	// Do not infer IDNA equivalence without an owned, maintained normalizer.
	// ASCII host casing is safe; non-ASCII aliases fail closed.
	if isASCIIHTTPRedirectHost(source) && isASCIIHTTPRedirectHost(target) && strings.EqualFold(source, target) {
		return true
	}
	return false
}

func isASCIIHTTPRedirectHost(host string) bool {
	for index := 0; index < len(host); index++ {
		if host[index] > 0x7f {
			return false
		}
	}
	return true
}

func parseHTTPRedirectIPv6(host string) (netip.Addr, string, bool) {
	address, zone, _ := strings.Cut(host, "%")
	parsed, err := netip.ParseAddr(address)
	if err != nil || !parsed.Is6() {
		return netip.Addr{}, "", false
	}
	return parsed, zone, true
}

func isHTTPRedirectHeader(name string, candidates []string) bool {
	for _, candidate := range candidates {
		if strings.EqualFold(name, candidate) {
			return true
		}
	}
	return false
}

func scrubHTTPRedirectHeaders(headers http.Header, names []string) {
	for name := range headers {
		if isHTTPRedirectHeader(name, names) {
			delete(headers, name)
		}
	}
}

const httpRedirectRedactedURL = "<redacted redirect URL>"

func sanitizeHTTPRedirectURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" || parsed.Opaque != "" {
		return httpRedirectRedactedURL
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return httpRedirectRedactedURL
	}
	return scheme + "://" + parsed.Host
}
