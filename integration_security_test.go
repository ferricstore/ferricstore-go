//go:build integration

package ferricstore

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func TestIntegrationSecurityBootstrap(t *testing.T) {
	if os.Getenv("FERRICSTORE_SECURITY_BOOTSTRAP") != "1" {
		t.Skip("security bootstrap is run by scripts/integration-security-docker.sh")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bootstrap := NewClient(integrationAddress(), WithNativeOptions(WithNativeHeartbeat(0, 0)))
	setAdminErr := bootstrap.ACLSetUser(ctx, "default", "on", ">"+securityAdminPassword(t), "+@all", "~*")
	_ = bootstrap.Close()

	client := NewClient(integrationAddress(), WithNativeOptions(
		WithNativeCredentials("default", securityAdminPassword(t)),
		WithNativeHeartbeat(0, 0),
	))
	defer client.Close()
	if _, err := client.Ping(ctx); err != nil {
		t.Fatalf("authenticate after default-user bootstrap (SETUSER error %v): %v", setAdminErr, err)
	}
	if err := client.ACLSetUser(ctx, securityReaderUser(t),
		"on", ">"+securityReaderPassword(t), "-@all", "+PING", "+GET", "+SET",
		"+SHARDS", "+SUBSCRIBE_EVENTS", "+FLOW.QUERY", "+FLOW.QUERY.EXPLAIN",
		"~secure:allowed:*"); err != nil {
		t.Fatal(err)
	}
	if err := client.ACLSave(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationSecurityReady(t *testing.T) {
	if os.Getenv("FERRICSTORE_SECURITY_READY") != "1" {
		t.Skip("security readiness is run by scripts/integration-security-docker.sh")
	}
	client := securityClient(t, "default", securityAdminPassword(t), securityTLSConfig(t, true, "localhost"))
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if pong, err := client.Ping(ctx); err != nil || pong != "PONG" {
		t.Fatalf("authenticated mTLS PING = %q, %v", pong, err)
	}
}

func TestIntegrationSecurityAuthenticationAndACL(t *testing.T) {
	requireSecurityIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	anonymous := securityClient(t, "", "", securityTLSConfig(t, true, "localhost"))
	_, err := anonymous.Ping(ctx)
	anonymous.Close()
	requireSecurityError(t, err, "auth", "noauth", "authentication", "wrongpass", "invalid username-password")

	wrongPassword := securityClient(t, "default", "definitely-wrong", securityTLSConfig(t, true, "localhost"))
	_, err = wrongPassword.Ping(ctx)
	wrongPassword.Close()
	requireSecurityError(t, err, "auth", "invalid", "password")

	admin := securityClient(t, "default", securityAdminPassword(t), securityTLSConfig(t, true, "localhost"))
	defer admin.Close()
	if pong, err := admin.Ping(ctx); err != nil || pong != "PONG" {
		t.Fatalf("admin authenticated PING = %q, %v", pong, err)
	}
	if err := admin.KV().Set(ctx, "secure:allowed:seed", "allowed"); err != nil {
		t.Fatal(err)
	}
	if err := admin.KV().Set(ctx, "secure:denied:seed", "denied"); err != nil {
		t.Fatal(err)
	}

	reader := securityClient(t, securityReaderUser(t), securityReaderPassword(t), securityTLSConfig(t, true, "localhost"))
	defer reader.Close()
	if value, err := reader.KV().Get(ctx, "secure:allowed:seed"); err != nil || asString(value) != "allowed" {
		t.Fatalf("reader allowed GET = %#v, %v", value, err)
	}
	if err := reader.KV().Set(ctx, "secure:allowed:write", "reader"); err != nil {
		t.Fatalf("reader allowed SET: %v", err)
	}
	if _, err := reader.KV().Get(ctx, "secure:denied:seed"); err == nil {
		t.Fatal("reader accessed a key outside its ACL pattern")
	}
	if _, err := reader.Delete(ctx, "secure:allowed:write"); err == nil {
		t.Fatal("reader executed a command outside its ACL command set")
	}

	queryID := "secure:allowed:query:" + integrationSuffix("acl")
	queryPartition := "secure:allowed:query:partition"
	queryType := "go-sdk-security-query"
	if _, err := admin.Create(ctx, CreateOptions{
		ID: queryID, Type: queryType, State: "ready", PartitionKey: queryPartition,
		NowMS: time.Now().UnixMilli(), Idempotent: Bool(true),
	}); err != nil {
		t.Fatal(err)
	}
	query := "FROM runs WHERE partition_key = @partition AND type = @type AND state = @state ORDER BY updated_at_ms ASC LIMIT 10 RETURN RECORDS"
	params := map[string]any{"partition": queryPartition, "type": queryType, "state": "ready"}
	waitForFlowQueryResult(t, ctx, func() (*FlowQueryResult, error) {
		return reader.FlowQuery(ctx, query, params)
	}, func(result *FlowQueryResult) bool {
		return result != nil && len(result.Records) == 1
	})
	if explain, err := reader.FlowExplain(ctx, query, params); err != nil || explain.Status != "planned" {
		t.Fatalf("reader allowed EXPLAIN = %#v, %v", explain, err)
	}

	deniedParams := map[string]any{"partition": "secure:denied:query:partition", "type": queryType, "state": "ready"}
	if _, err := reader.FlowQuery(ctx, query, deniedParams); err == nil {
		t.Fatal("reader queried a partition outside its ACL key pattern")
	}
	if _, err := reader.FlowExplain(ctx, query, deniedParams); err == nil {
		t.Fatal("reader explained a partition outside its ACL key pattern")
	}
	if _, err := reader.FlowQueryIndexes(ctx); err == nil {
		t.Fatal("reader accessed admin-only FLOW.QUERY.INDEXES")
	}
}

func TestIntegrationSecurityTLSVerificationAndEnforcement(t *testing.T) {
	requireSecurityIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	plain := NewClient(integrationAddress(), WithNativeOptions(
		WithNativeCredentials("default", securityAdminPassword(t)),
		WithNativeHeartbeat(0, 0),
	))
	_, err := plain.Ping(ctx)
	plain.Close()
	requireSecurityError(t, err, "tls", "ssl", "secure")

	noClientCertificate := securityClient(t, "default", securityAdminPassword(t), securityTLSConfig(t, false, "localhost"))
	_, err = noClientCertificate.Ping(ctx)
	noClientCertificate.Close()
	if err == nil {
		t.Fatal("mTLS connection without a client certificate succeeded")
	}

	wrongServerName := securityClient(t, "default", securityAdminPassword(t), securityTLSConfig(t, true, "wrong.invalid"))
	_, err = wrongServerName.Ping(ctx)
	wrongServerName.Close()
	if err == nil {
		t.Fatal("TLS connection with an invalid server name succeeded")
	}
	var hostnameError x509.HostnameError
	if !errors.As(err, &hostnameError) && !strings.Contains(strings.ToLower(err.Error()), "certificate") {
		t.Fatalf("wrong server name returned an unrelated error: %v", err)
	}

	valid := securityClient(t, "default", securityAdminPassword(t), securityTLSConfig(t, true, "localhost"))
	defer valid.Close()
	if pong, err := valid.Ping(ctx); err != nil || pong != "PONG" {
		t.Fatalf("verified mTLS PING = %q, %v", pong, err)
	}
}

func securityClient(t *testing.T, username, password string, config *tls.Config) *Client {
	t.Helper()
	return NewClient(os.Getenv("FERRICSTORE_TLS_ADDR"), WithNativeOptions(
		WithNativeCredentials(username, password),
		WithNativeTLS(config),
		WithNativeHeartbeat(0, 0),
	))
}

func securityTLSConfig(t *testing.T, includeClientCertificate bool, serverName string) *tls.Config {
	t.Helper()
	caPEM, err := os.ReadFile(requiredSecurityEnv(t, "FERRICSTORE_SECURITY_CA_CERT"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("security CA certificate contains no certificates")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: serverName}
	if includeClientCertificate {
		certificate, err := tls.LoadX509KeyPair(
			requiredSecurityEnv(t, "FERRICSTORE_SECURITY_CLIENT_CERT"),
			requiredSecurityEnv(t, "FERRICSTORE_SECURITY_CLIENT_KEY"),
		)
		if err != nil {
			t.Fatal(err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config
}

func requireSecurityIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("FERRICSTORE_SECURITY_TEST") != "1" {
		t.Skip("security integration is run by scripts/integration-security-docker.sh")
	}
}

func requiredSecurityEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required", name)
	}
	return value
}

func securityAdminPassword(t *testing.T) string {
	return requiredSecurityEnv(t, "FERRICSTORE_SECURITY_ADMIN_PASSWORD")
}

func securityReaderUser(t *testing.T) string {
	return requiredSecurityEnv(t, "FERRICSTORE_SECURITY_READER_USER")
}

func securityReaderPassword(t *testing.T) string {
	return requiredSecurityEnv(t, "FERRICSTORE_SECURITY_READER_PASSWORD")
}

func requireSecurityError(t *testing.T, err error, fragments ...string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected security error")
	}
	message := strings.ToLower(err.Error())
	for _, fragment := range fragments {
		if strings.Contains(message, fragment) {
			return
		}
	}
	t.Fatalf("security error %q did not contain one of %q", err, fragments)
}
