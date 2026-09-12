package ferricstore

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPlatformCredentialBrokerExchange(t *testing.T) {
	t.Parallel()

	const token = "fsp_sa_test-control-token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/credential-exchange/clusters/cluster-123" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Fatal("control token was not sent as the exchange bearer token")
		}
		var request map[string]map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if request["credential"]["organization"] != "acme" || request["credential"]["ttl_seconds"] != float64(300) {
			t.Fatalf("unexpected credential request: %#v", request)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
			"endpoint":               "ferrics://store.example.com:6389",
			"username":               "platform_cli_0123456789abcdef",
			"password":               "temporary-native-secret",
			"expires_at":             time.Now().Add(5 * time.Minute).UTC().Format(time.RFC3339Nano),
			"namespace":              "tenant:acme:primary",
			"credential_fingerprint": "sha256:abc",
			"cluster_id":             "cluster-123",
			"organization_id":        "organization-123",
			"principal":              "deploy-bot",
			"principal_type":         "service_account",
		}})
	}))
	defer server.Close()

	broker, err := NewPlatformCredentialBroker(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	credential, err := broker.Exchange(context.Background(), token, PlatformCredentialRequest{
		Organization: "acme",
		ClusterID:    "cluster-123",
		TTL:          5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if credential.Username != "platform_cli_0123456789abcdef" || credential.Password != "temporary-native-secret" {
		t.Fatalf("unexpected credential: %#v", credential)
	}
	if credential.Principal != "deploy-bot" || credential.PrincipalType != "service_account" {
		t.Fatalf("unexpected principal: %#v", credential)
	}
}

func TestPlatformCredentialBrokerSecurityPolicy(t *testing.T) {
	t.Parallel()

	for _, rawURL := range []string{
		"http://platform.example.com",
		"https://user:secret@platform.example.com",
		"https://platform.example.com?token=secret",
	} {
		if broker, err := NewPlatformCredentialBroker(rawURL, nil); err == nil || broker != nil {
			t.Fatalf("NewPlatformCredentialBroker(%q) = %#v, %v; want rejection", rawURL, broker, err)
		}
	}
}

func TestPlatformCredentialBrokerDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()

	targetCalled := false
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		targetCalled = true
	}))
	defer target.Close()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()

	broker, err := NewPlatformCredentialBroker(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = broker.Exchange(context.Background(), "fsp_user_secret", PlatformCredentialRequest{ClusterID: "cluster-123"})
	if err == nil || !strings.Contains(err.Error(), "redirects are not allowed") {
		t.Fatalf("Exchange() error = %v, want redirect rejection", err)
	}
	if targetCalled {
		t.Fatal("redirect target received the Platform bearer token")
	}
}
