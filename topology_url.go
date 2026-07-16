package ferricstore

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type parsedFerricURL struct {
	Host           string
	Port           int
	RawURL         string
	Scheme         string
	TLS            bool
	Username       string
	Password       string
	CredentialsSet bool
	ExplicitPort   bool
	Timeout        time.Duration
	HasTimeout     bool
	query          url.Values
}

func parseFerricURL(raw string) (parsedFerricURL, error) {
	hasScheme := strings.Contains(raw, "://")
	explicitPort := false
	if !hasScheme {
		explicitPort = nativeAddressHasExplicitPort(raw)
		raw = "ferric://" + nativeAddressWithPort(raw, nativeDefaultPort)
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return parsedFerricURL{}, err
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return parsedFerricURL{}, fmt.Errorf("FerricStore URL path %q is unsupported", parsed.Path)
	}
	if parsed.Fragment != "" {
		return parsedFerricURL{}, errors.New("FerricStore URL fragments are unsupported")
	}
	scheme := strings.ToLower(parsed.Scheme)
	tlsEnabled := false
	defaultPort := nativeDefaultPort
	switch scheme {
	case "ferric":
	case "ferrics":
		tlsEnabled = true
		defaultPort = nativeDefaultTLSPort
	default:
		return parsedFerricURL{}, fmt.Errorf("unsupported FerricStore URL scheme %q", parsed.Scheme)
	}
	host := parsed.Hostname()
	if host == "" {
		return parsedFerricURL{}, errors.New("FerricStore URL requires a host")
	}
	ipHost := host
	if address, _, hasZone := strings.Cut(host, "%"); hasZone {
		ipHost = address
	}
	if strings.Contains(ipHost, ":") && net.ParseIP(ipHost) == nil {
		return parsedFerricURL{}, fmt.Errorf("invalid FerricStore URL host %q", host)
	}
	rawPort := parsed.Port()
	if hasScheme && rawPort != "" {
		explicitPort = true
	}
	if rawPort == "" {
		rawPort = defaultPort
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		return parsedFerricURL{}, fmt.Errorf("invalid FerricStore URL port %q", rawPort)
	}
	if port < 1 || port > 65535 {
		return parsedFerricURL{}, fmt.Errorf("FerricStore URL port %d is outside the TCP port range", port)
	}
	var query url.Values
	if parsed.RawQuery != "" {
		query, err = url.ParseQuery(parsed.RawQuery)
		if err != nil {
			return parsedFerricURL{}, fmt.Errorf("invalid FerricStore URL query: %w", err)
		}
	}
	timeout, hasTimeout, err := ferricURLTimeout(query)
	if err != nil {
		return parsedFerricURL{}, err
	}
	password := ""
	if parsed.User != nil {
		password, _ = parsed.User.Password()
	}
	out := parsedFerricURL{
		Host: host, Port: port, Scheme: scheme, TLS: tlsEnabled, Password: password,
		CredentialsSet: parsed.User != nil,
		ExplicitPort:   explicitPort, Timeout: timeout, HasTimeout: hasTimeout, query: query,
	}
	if parsed.User != nil {
		out.Username = parsed.User.Username()
	}
	out.RawURL = out.URL()
	return out, nil
}

func (p parsedFerricURL) Address() string {
	return net.JoinHostPort(p.Host, strconv.Itoa(p.Port))
}

func (p parsedFerricURL) Endpoint() RoutingEndpoint {
	endpoint := RoutingEndpoint{Node: p.Host, Host: p.Host, NativePort: p.Port}
	if p.TLS {
		endpoint.NativeTLSPort = p.Port
	}
	return endpoint
}

func (p parsedFerricURL) URL() string {
	host := p.Host
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		host = "[" + host + "]"
	}
	user := ""
	if p.CredentialsSet {
		user = url.UserPassword(p.Username, p.Password).String() + "@"
	}
	rawURL := fmt.Sprintf("%s://%s%s:%d", p.Scheme, user, host, p.Port)
	if query := p.query.Encode(); query != "" {
		rawURL += "?" + query
	}
	return rawURL
}

func ferricURLTimeout(query url.Values) (time.Duration, bool, error) {
	for key := range query {
		if key != "timeout" {
			return 0, false, fmt.Errorf("unsupported FerricStore URL query option %q", key)
		}
	}
	values, present := query["timeout"]
	if !present {
		return 0, false, nil
	}
	if len(values) != 1 {
		return 0, false, errors.New("invalid ferricstore native timeout: specify it exactly once")
	}
	if values[0] == "" {
		return 0, false, errors.New("invalid ferricstore native timeout: duration is empty")
	}
	timeout, err := time.ParseDuration(values[0])
	if err != nil {
		return 0, false, fmt.Errorf("invalid ferricstore native timeout: %w", err)
	}
	if timeout < 0 {
		return 0, false, errors.New("invalid ferricstore native timeout: duration cannot be negative")
	}
	return timeout, true, nil
}

func endpointKey(endpoint RoutingEndpoint) string {
	if endpoint.NativeTLSPort > 0 {
		return fmt.Sprintf("%s:%d:%d", strings.ToLower(endpoint.Host), endpoint.NativePort, endpoint.NativeTLSPort)
	}
	return fmt.Sprintf("%s:%d", strings.ToLower(endpoint.Host), endpoint.NativePort)
}

func connectionKeyForEndpoint(endpoint RoutingEndpoint, useTLS bool) string {
	port := endpoint.NativePort
	scheme := "ferric"
	if useTLS {
		scheme = "ferrics"
		if endpoint.NativeTLSPort > 0 {
			port = endpoint.NativeTLSPort
		}
	}
	return fmt.Sprintf("%s://%s:%d", scheme, strings.ToLower(endpoint.Host), port)
}

func urlFromEndpoint(endpoint RoutingEndpoint, useTLS bool) string {
	port := endpoint.NativePort
	scheme := "ferric"
	if useTLS {
		scheme = "ferrics"
		if endpoint.NativeTLSPort > 0 {
			port = endpoint.NativeTLSPort
		}
	}
	host := endpoint.Host
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		host = "[" + host + "]"
	}
	return fmt.Sprintf("%s://%s:%d", scheme, host, port)
}

func normalizedStringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func stringSet(values ...string) map[string]struct{} {
	return normalizedStringSet(values)
}
