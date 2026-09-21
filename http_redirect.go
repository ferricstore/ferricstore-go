package ferricstore

import (
	"errors"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
)

var httpRedirectUnsafeHeaders = [...]string{
	"Authorization",
	"Www-Authenticate",
	"Cookie",
	"Cookie2",
	"Proxy-Authorization",
	"Proxy-Authenticate",
	"Referer",
}

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

func (e *httpRedirectPolicyError) Error() string {
	return e.message
}

func newHTTPRedirectPolicyError(message string) error {
	return &httpRedirectPolicyError{message: message}
}

func sanitizeHTTPRedirectCause(err error) error {
	var policyErr *httpRedirectPolicyError
	if errors.As(err, &policyErr) {
		return policyErr
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Err != nil &&
		strings.HasPrefix(urlErr.Err.Error(), "failed to parse Location header") {
		return newHTTPRedirectPolicyError("failed to parse redirect Location header")
	}
	return err
}

func preserveHTTPRedirectHeaders(request *http.Request, via []*http.Request) error {
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
	unsafeHop := hasUnsafeHTTPRedirectHop(via, request)
	if unsafeHop && request.Body != nil && request.Body != http.NoBody {
		return newHTTPRedirectPolicyError("refusing to replay request body across unsafe redirect")
	}
	if unsafeHop {
		scrubHTTPRedirectHeaders(request.Header, httpRedirectUnsafeHeaders[:])
	}
	if request.Method == http.MethodGet {
		scrubHTTPRedirectHeaders(request.Header, httpRedirectBodyHeaders[:])
	}
	for name, values := range via[0].Header {
		if request.Method == http.MethodGet && isHTTPRedirectHeader(name, httpRedirectBodyHeaders[:]) {
			continue
		}
		if unsafeHop && isHTTPRedirectHeader(name, httpRedirectUnsafeHeaders[:]) {
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
	// Ports remain compatible with net/http's same-host behavior; host changes
	// and HTTPS downgrades are never trusted for credential forwarding.
	if source == nil || target == nil || source.User != nil || target.User != nil {
		return false
	}
	sourceScheme := strings.ToLower(source.Scheme)
	targetScheme := strings.ToLower(target.Scheme)
	if (sourceScheme != "http" && sourceScheme != "https") ||
		(targetScheme != "http" && targetScheme != "https") {
		return false
	}
	if sourceScheme == "https" && targetScheme == "http" {
		return false
	}
	sourceHost := source.Hostname()
	targetHost := target.Hostname()
	return sourceHost != "" && sameHTTPRedirectHost(sourceHost, targetHost)
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
