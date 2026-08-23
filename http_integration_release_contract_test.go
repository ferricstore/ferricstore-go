package ferricstore

import (
	"os"
	"strings"
	"testing"
)

func TestHTTPIntegrationIsRequiredByCIAndRelease(t *testing.T) {
	runner := readRepositoryContractFile(t, "scripts/integration-http-tls.sh")
	assertImmutableFerricStoreImages(t, "scripts/integration-http-tls.sh", runner)
	for _, required := range []string{
		"FERRICSTORE_HTTP_TLS_ENABLED=true",
		"FERRICSTORE_USERNAME",
		"FERRICSTORE_PASSWORD",
		"FERRICSTORE_CA_FILE",
		"@sha256:",
		"chmod 700",
		"chmod 600",
		`rm -f "$tls_dir/ca.key"`,
		"source=$tls_dir/server.key,target=/tls/server.key,readonly",
		"sdk-http-denied",
		"ACL authorization probe unexpectedly allowed SET",
		"unauthenticated HTTP request returned",
		"go test -tags integration",
	} {
		if !strings.Contains(runner, required) {
			t.Fatalf("HTTP integration runner does not contain %q", required)
		}
	}
	if strings.Contains(runner, "FERRICSTORE_SKIP_COMMAND_COVERAGE") {
		t.Fatal("HTTP integration runner bypasses supported-command coverage")
	}

	for _, workflow := range []string{".github/workflows/test.yml", ".github/workflows/release.yml"} {
		contents := readRepositoryContractFile(t, workflow)
		assertImmutableFerricStoreImages(t, workflow, contents)
		if !strings.Contains(contents, "./scripts/integration-http-tls.sh") {
			t.Fatalf("%s does not gate on TLS HTTP integration", workflow)
		}
		if !strings.Contains(contents, "@sha256:") {
			t.Fatalf("%s does not pin the HTTP integration image by digest", workflow)
		}
	}

	readme := readRepositoryContractFile(t, "README.md")
	for _, required := range []string{"integration-http-tls.sh", "FERRICSTORE_CA_FILE"} {
		if !strings.Contains(readme, required) {
			t.Fatalf("README does not document %q", required)
		}
	}
}

func assertImmutableFerricStoreImages(t *testing.T, path, contents string) {
	t.Helper()
	for _, line := range strings.Split(contents, "\n") {
		if strings.Contains(line, "quay.io/ferricstore/ferricstore:") && !strings.Contains(line, "@sha256:") {
			t.Fatalf("%s contains a mutable FerricStore image reference: %s", path, strings.TrimSpace(line))
		}
	}
}

func readRepositoryContractFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}
