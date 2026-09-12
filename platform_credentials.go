package ferricstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const platformCredentialResponseLimit = 1 << 20

// PlatformCredentialRequest identifies the organization and cluster for a
// short-lived native credential. ClusterID is the Platform cluster UUID.
type PlatformCredentialRequest struct {
	Organization string
	ClusterID    string
	TTL          time.Duration
}

// PlatformCredential is a short-lived, scoped FerricStore native credential.
// The originating Platform bearer token is never forwarded to the data plane.
type PlatformCredential struct {
	Endpoint              string
	Username              string
	Password              string
	ExpiresAt             time.Time
	Namespace             string
	CredentialFingerprint string
	ClusterID             string
	OrganizationID        string
	Principal             string
	PrincipalType         string
}

// PlatformCredentialBroker exchanges Platform user or service-account bearer
// tokens for short-lived FerricStore native credentials.
type PlatformCredentialBroker struct {
	baseURL *url.URL
	client  *http.Client
}

// NewPlatformCredentialBroker validates a Platform control-plane URL. HTTPS is
// required except for loopback development and integration endpoints.
func NewPlatformCredentialBroker(controlURL string, client *http.Client) (*PlatformCredentialBroker, error) {
	parsed, err := url.Parse(strings.TrimSpace(controlURL))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, errors.New("ferricstore platform control URL is invalid")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("ferricstore platform control URL must not contain credentials, query, or fragment")
	}
	if parsed.Scheme != "https" && (parsed.Scheme != "http" || !loopbackHost(parsed.Hostname())) {
		return nil, errors.New("ferricstore platform control URL must use HTTPS")
	}
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	copyURL := *parsed
	copyURL.Path = strings.TrimRight(copyURL.Path, "/")
	return &PlatformCredentialBroker{baseURL: &copyURL, client: client}, nil
}

// Exchange requests one scoped native credential. bearerToken may be a
// time-limited fsp_user_ token or an fsp_sa_ service-account token.
func (b *PlatformCredentialBroker) Exchange(
	ctx context.Context,
	bearerToken string,
	request PlatformCredentialRequest,
) (PlatformCredential, error) {
	if b == nil || b.baseURL == nil || b.client == nil {
		return PlatformCredential{}, errors.New("ferricstore platform credential broker is not configured")
	}
	if err := ctx.Err(); err != nil {
		return PlatformCredential{}, err
	}
	bearerToken = strings.TrimSpace(bearerToken)
	if bearerToken == "" {
		return PlatformCredential{}, errors.New("ferricstore platform bearer token is required")
	}
	request.ClusterID = strings.TrimSpace(request.ClusterID)
	if request.ClusterID == "" || strings.Contains(request.ClusterID, "/") {
		return PlatformCredential{}, errors.New("ferricstore platform cluster ID is invalid")
	}
	if request.TTL < 0 || request.TTL > time.Hour || (request.TTL > 0 && request.TTL < time.Minute) {
		return PlatformCredential{}, errors.New("ferricstore platform credential TTL must be between one minute and one hour")
	}

	payload := struct {
		Credential struct {
			Organization string `json:"organization,omitempty"`
			TTLSeconds   int64  `json:"ttl_seconds,omitempty"`
		} `json:"credential"`
	}{}
	payload.Credential.Organization = strings.TrimSpace(request.Organization)
	if request.TTL > 0 {
		payload.Credential.TTLSeconds = int64(request.TTL / time.Second)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return PlatformCredential{}, fmt.Errorf("encode Platform credential request: %w", err)
	}

	endpoint := *b.baseURL
	endpoint.Path = b.baseURL.Path + "/api/v1/credential-exchange/clusters/" + url.PathEscape(request.ClusterID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return PlatformCredential{}, fmt.Errorf("create Platform credential request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+bearerToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	client := *b.client
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return errors.New("Platform credential exchange redirects are not allowed")
	}
	response, err := client.Do(req)
	if err != nil {
		return PlatformCredential{}, fmt.Errorf("exchange Platform credential: %w", err)
	}
	defer func() { _ = response.Body.Close() }()

	responseBody, err := io.ReadAll(io.LimitReader(response.Body, platformCredentialResponseLimit+1))
	if err != nil {
		return PlatformCredential{}, fmt.Errorf("read Platform credential response: %w", err)
	}
	if len(responseBody) > platformCredentialResponseLimit {
		return PlatformCredential{}, errors.New("platform credential response exceeds one MiB")
	}
	if response.StatusCode != http.StatusCreated {
		return PlatformCredential{}, platformExchangeError(response.StatusCode, responseBody)
	}

	var decoded struct {
		Data struct {
			Endpoint              string `json:"endpoint"`
			Username              string `json:"username"`
			Password              string `json:"password"`
			ExpiresAt             string `json:"expires_at"`
			Namespace             string `json:"namespace"`
			CredentialFingerprint string `json:"credential_fingerprint"`
			ClusterID             string `json:"cluster_id"`
			OrganizationID        string `json:"organization_id"`
			Principal             string `json:"principal"`
			PrincipalType         string `json:"principal_type"`
		} `json:"data"`
	}
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return PlatformCredential{}, errors.New("platform credential response is invalid JSON")
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, decoded.Data.ExpiresAt)
	if err != nil || !expiresAt.After(time.Now()) {
		return PlatformCredential{}, errors.New("platform credential response has an invalid expiry")
	}
	if decoded.Data.Endpoint == "" || decoded.Data.Username == "" || decoded.Data.Password == "" {
		return PlatformCredential{}, errors.New("Platform credential response is incomplete")
	}
	parsedEndpoint, err := parseFerricURL(decoded.Data.Endpoint)
	if err != nil || parsedEndpoint.CredentialsSet {
		return PlatformCredential{}, errors.New("Platform credential response has an invalid FerricStore endpoint")
	}

	return PlatformCredential{
		Endpoint:              decoded.Data.Endpoint,
		Username:              decoded.Data.Username,
		Password:              decoded.Data.Password,
		ExpiresAt:             expiresAt,
		Namespace:             decoded.Data.Namespace,
		CredentialFingerprint: decoded.Data.CredentialFingerprint,
		ClusterID:             decoded.Data.ClusterID,
		OrganizationID:        decoded.Data.OrganizationID,
		Principal:             decoded.Data.Principal,
		PrincipalType:         decoded.Data.PrincipalType,
	}, nil
}

// NewClient exchanges a control-plane token and constructs a native client
// configured with the resulting temporary credential.
func (b *PlatformCredentialBroker) NewClient(
	ctx context.Context,
	bearerToken string,
	request PlatformCredentialRequest,
	opts ...ClientOption,
) (*Client, PlatformCredential, error) {
	credential, err := b.Exchange(ctx, bearerToken, request)
	if err != nil {
		return nil, PlatformCredential{}, err
	}
	opts = append(opts, WithNativeOptions(WithNativeCredentials(credential.Username, credential.Password)))
	client, err := NewClientFromURL(credential.Endpoint, opts...)
	if err != nil {
		return nil, PlatformCredential{}, err
	}
	return client, credential, nil
}

func platformExchangeError(status int, body []byte) error {
	var decoded struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &decoded)
	code := strings.TrimSpace(decoded.Error.Code)
	if code == "" {
		code = "request_failed"
	}
	return fmt.Errorf("Platform credential exchange failed with status %d (%s)", status, code)
}

func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
