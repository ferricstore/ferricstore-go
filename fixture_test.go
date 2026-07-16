package ferricstore

import (
	"os"
	"strings"
	"testing"
)

func TestIntegrationDockerScriptDefaultsToFerricStore075(t *testing.T) {
	for _, path := range []string{
		"scripts/integration-docker.sh",
		"scripts/integration-security-docker.sh",
		"scripts/integration-cluster-docker.sh",
	} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "ghcr.io/ferricstore/ferricstore:0.7.5") {
			t.Fatalf("%s should default to FerricStore 0.7.5", path)
		}
	}
}

func TestDockerComposeDefaultsToSupportedFerricStoreVersion(t *testing.T) {
	body, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	contents := string(body)
	if !strings.Contains(contents, "ghcr.io/ferricstore/ferricstore:0.7.5") {
		t.Fatal("docker compose should default to the SDK's pinned supported server version")
	}
	if strings.Contains(contents, "ferricstore:latest") {
		t.Fatal("docker compose should not use a mutable latest server image")
	}
}

func TestCIUsesVersionedIntegrationRunner(t *testing.T) {
	body, err := os.ReadFile(".github/workflows/test.yml")
	if err != nil {
		t.Fatal(err)
	}
	contents := string(body)
	if !strings.Contains(contents, "./scripts/integration-docker.sh") {
		t.Fatal("CI should use the version-pinned integration runner")
	}
	if !strings.Contains(contents, "./scripts/integration-security-docker.sh") {
		t.Fatal("CI should enforce protected-mode, ACL, TLS, and mTLS integration")
	}
	if !strings.Contains(contents, "./scripts/integration-cluster-docker.sh") {
		t.Fatal("CI should enforce real multi-node routing and failover integration")
	}
	if !strings.Contains(contents, "./scripts/fuzz-smoke.sh") {
		t.Fatal("CI should enforce the protocol and URL fuzz smoke suite")
	}
	if !strings.Contains(contents, "./scripts/stress.sh") {
		t.Fatal("CI should enforce repeated race stress and performance checks")
	}
	if !strings.Contains(contents, "./scripts/api-compat.sh") {
		t.Fatal("CI should reject exported API breaks against the last release")
	}
	if strings.Contains(contents, "docker compose up") {
		t.Fatal("CI should not bypass the version-pinned integration runner")
	}
}

func TestHardeningScriptsExistAndAreStrict(t *testing.T) {
	for _, path := range []string{"scripts/api-compat.sh", "scripts/fuzz-smoke.sh", "scripts/stress.sh"} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode()&0o111 == 0 {
			t.Fatalf("%s must be executable", path)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "set -euo pipefail") {
			t.Fatalf("%s must fail closed", path)
		}
	}
}

func TestReleaseWorkflowCannotBypassProductionGates(t *testing.T) {
	body, err := os.ReadFile(".github/workflows/release.yml")
	if err != nil {
		t.Fatal(err)
	}
	contents := string(body)
	for _, required := range []string{
		"go vet ./...",
		"go test -race ./...",
		"./scripts/api-compat.sh",
		"./scripts/fuzz-smoke.sh",
		"./scripts/stress.sh",
		"./scripts/integration-docker.sh",
		"./scripts/integration-security-docker.sh",
		"./scripts/integration-cluster-docker.sh",
		"govulncheck ./...",
	} {
		if !strings.Contains(contents, required) {
			t.Fatalf("release workflow must enforce %q", required)
		}
	}
	if !strings.Contains(contents, "needs:") {
		t.Fatal("release publication must depend on verification jobs")
	}
}

func TestSecurityScannerVersionIsPinned(t *testing.T) {
	body, err := os.ReadFile(".github/workflows/security.yml")
	if err != nil {
		t.Fatal(err)
	}
	contents := string(body)
	if strings.Contains(contents, "govulncheck@latest") {
		t.Fatal("security workflow must not install a mutable govulncheck version")
	}
	if !strings.Contains(contents, "golang.org/x/vuln/cmd/govulncheck@v") {
		t.Fatal("security workflow must pin govulncheck")
	}
}

func TestToolchainPinsIncludeTLSVulnerabilityFix(t *testing.T) {
	for _, path := range []string{
		"mise.toml",
		".github/workflows/test.yml",
		".github/workflows/security.yml",
		".github/workflows/release.yml",
	} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		contents := string(body)
		if strings.Contains(contents, "1.26.4") {
			t.Fatalf("%s pins Go 1.26.4, which is affected by GO-2026-5856", path)
		}
		if !strings.Contains(contents, "1.26.5") {
			t.Fatalf("%s must pin Go 1.26.5 or newer", path)
		}
	}
}

func TestREADMEUsesCurrentScheduleAPI(t *testing.T) {
	body, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	contents := string(body)
	if strings.Contains(contents, `"id":    "report-{{fire_id}}"`) {
		t.Fatal("README recurring schedule example must use id_prefix instead of a fixed id")
	}
	if !strings.Contains(contents, `"id_prefix": "report-"`) {
		t.Fatal("README schedule example should demonstrate a valid recurring id_prefix")
	}
	if strings.Contains(contents, "ScheduleListOptions{Limit:") {
		t.Fatal("README uses the removed ScheduleListOptions.Limit field")
	}
	if !strings.Contains(contents, "ScheduleListOptions{Count:") {
		t.Fatal("README should use ScheduleListOptions.Count")
	}
}

func TestProjectDocsDescribeProductionGates(t *testing.T) {
	for _, path := range []string{"README.md", "CONTRIBUTING.md", "RELEASE.md"} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		contents := string(body)
		for _, command := range []string{
			"./scripts/api-compat.sh",
			"./scripts/fuzz-smoke.sh",
			"./scripts/stress.sh",
			"./scripts/integration-docker.sh",
			"./scripts/integration-security-docker.sh",
			"./scripts/integration-cluster-docker.sh",
		} {
			if !strings.Contains(contents, command) {
				t.Errorf("%s must document production gate %q", path, command)
			}
		}
	}
}

func TestDesignDocsDescribeCurrentWorkerLifecycle(t *testing.T) {
	body, err := os.ReadFile("docs/design.md")
	if err != nil {
		t.Fatal(err)
	}
	contents := string(body)
	if strings.Contains(contents, "Long-running worker lifecycle helpers are intentionally not hidden yet") {
		t.Fatal("design docs still claim that implemented worker lifecycle helpers are missing")
	}
	for _, method := range []string{"RunForever", "Start", "Stop", "Join", "Stats"} {
		if !strings.Contains(contents, method) {
			t.Fatalf("design docs must describe worker lifecycle method %s", method)
		}
	}
}

func TestDesignDocsDescribeQueueCompletionBatching(t *testing.T) {
	body, err := os.ReadFile("docs/design.md")
	if err != nil {
		t.Fatal(err)
	}
	contents := string(body)
	if strings.Contains(contents, "Completion, retry, fail, and transition commands are sent per job") {
		t.Fatal("design docs incorrectly claim that queue completions are always sent per job")
	}
	if !strings.Contains(contents, "batches successful queue completions") {
		t.Fatal("design docs must describe queue completion batching")
	}
}

func TestIntegrationFailureLogsAreBounded(t *testing.T) {
	body, err := os.ReadFile("scripts/integration-security-docker.sh")
	if err != nil {
		t.Fatal(err)
	}
	contents := string(body)
	if strings.Contains(contents, `docker logs "$bootstrap_name"`) || strings.Contains(contents, `docker logs "$server_name"`) {
		t.Fatal("security integration cleanup must bound potentially large container logs")
	}
	if strings.Count(contents, "docker logs --tail 200") < 2 {
		t.Fatal("security integration cleanup should retain the final 200 lines from both containers")
	}
}
