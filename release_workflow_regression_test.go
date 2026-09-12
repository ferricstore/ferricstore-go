package ferricstore

import (
	"os"
	"strings"
	"testing"
)

func TestReleaseWorkflowGuardsVersionBeforePublishingTag(t *testing.T) {
	contents, err := os.ReadFile(".github/workflows/release.yml")
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(contents)
	required := []string{
		"workflow_dispatch:",
		"version:",
		"preflight:",
		"./scripts/release-preflight.sh",
		"needs: [preflight]",
		"needs: [preflight, quality, vulnerability, integration]",
		`git push origin "refs/tags/${VERSION}"`,
		"for attempt in",
		"GOPROXY=https://proxy.golang.org go list -m",
	}
	for _, text := range required {
		if !strings.Contains(workflow, text) {
			t.Errorf("release workflow is missing %q", text)
		}
	}
	if strings.Contains(workflow, "push:\n    tags:") {
		t.Fatal("release starts from an already-published tag instead of guarding the version first")
	}
	guard := strings.Index(workflow, "./scripts/release-preflight.sh")
	publish := strings.Index(workflow, `git push origin "refs/tags/${VERSION}"`)
	if guard < 0 || publish < 0 || guard >= publish {
		t.Fatal("release version guard must run before the tag is published")
	}
}

func TestReleaseWorkflowAvoidsNegativePerVersionProxyCacheBeforeTagging(t *testing.T) {
	preflight, err := os.ReadFile("scripts/release-preflight.sh")
	if err != nil {
		t.Fatal(err)
	}
	workflow, err := os.ReadFile(".github/workflows/release.yml")
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(preflight), "/@v/list") {
		t.Fatal("release preflight must inspect the proxy version list")
	}
	if strings.Contains(string(preflight), "${version}.info") {
		t.Fatal("release preflight must not seed a negative per-version proxy cache before tagging")
	}
	if strings.Contains(string(workflow), "/@v/list") {
		t.Fatal("post-tag release must resolve the new module directly instead of waiting for prior proxy knowledge")
	}
	publish := strings.Index(string(workflow), `git push origin "refs/tags/${VERSION}"`)
	resolve := strings.Index(string(workflow), `GOPROXY=https://proxy.golang.org go list -m`)
	if publish < 0 || resolve < 0 || publish >= resolve {
		t.Fatal("the module may be resolved through the proxy only after its tag is published")
	}
}

func TestReleaseInstructionsUseGuardedWorkflowInsteadOfManualTagPush(t *testing.T) {
	contents, err := os.ReadFile("RELEASE.md")
	if err != nil {
		t.Fatal(err)
	}
	instructions := string(contents)
	if !strings.Contains(instructions, "workflow_dispatch") ||
		!strings.Contains(instructions, "release-preflight.sh") {
		t.Fatal("release instructions do not describe the guarded workflow")
	}
	if strings.Contains(instructions, "git push origin main --tags") {
		t.Fatal("release instructions still publish an unguarded tag")
	}
}

func TestDurableStepDocsDistinguishClientAndServerTime(t *testing.T) {
	contents, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	docs := strings.Join(strings.Fields(string(contents)), " ")
	for _, text := range []string{
		"client process wall clock",
		"FerricStore evaluates the supplied `NOW`",
		"server time is not returned",
	} {
		if !strings.Contains(docs, text) {
			t.Errorf("durable-step clock documentation is missing %q", text)
		}
	}
}

func TestDurableStepDocsCoverVersionMigrationAndWorkerRecovery(t *testing.T) {
	contents, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	docs := strings.Join(strings.Fields(string(contents)), " ")
	for _, text := range []string{
		"Go SDK 0.12.2 requires FerricStore 0.11.4",
		"The step name is a stable replay identity",
		"External providers still need a stable idempotency key",
		"A waiting workflow does not occupy a worker",
		"any available worker can claim a fresh lease",
		"`StepContinue` remains available only as a deprecated low-level migration API",
	} {
		if !strings.Contains(docs, text) {
			t.Errorf("durable-step documentation is missing %q", text)
		}
	}
}
