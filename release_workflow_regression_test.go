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
	docs := string(contents)
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
