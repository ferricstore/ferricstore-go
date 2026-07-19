package ferricstore

import (
	"os"
	"strings"
	"testing"
)

func TestAPICompatibilityUsesRootPackageFacts(t *testing.T) {
	body, err := os.ReadFile("scripts/api-compat.sh")
	if err != nil {
		t.Fatal(err)
	}
	contents := string(body)
	if strings.Contains(contents, `apidiff" -m -w`) {
		t.Fatal("module facts include commands and examples outside the supported SDK package")
	}
	for _, required := range []string{
		`apidiff" -w "$tmp_dir/old.api" "$module"`,
		`apidiff" -w "$tmp_dir/new.api" "$module"`,
		`apidiff" -incompatible "$tmp_dir/old.api" "$tmp_dir/new.api"`,
	} {
		if !strings.Contains(contents, required) {
			t.Fatalf("API compatibility script must contain %q", required)
		}
	}
}

func TestAPICompatibilityPatchesAPIDiffMapOrdering(t *testing.T) {
	script, err := os.ReadFile("scripts/api-compat.sh")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(script), "scripts/patches/apidiff-deterministic.patch") {
		t.Fatal("API compatibility script must apply the deterministic apidiff patch")
	}
	if !strings.Contains(string(script), `git -C "$tmp_dir/apidiff" apply --unidiff-zero "$apidiff_patch"`) {
		t.Fatal("API compatibility script must apply the patch without a nonstandard patch executable")
	}

	patch, err := os.ReadFile("scripts/patches/apidiff-deterministic.patch")
	if err != nil {
		t.Fatal(err)
	}
	contents := string(patch)
	for _, required := range []string{
		`for _, name := range sortedObjectNames(old)`,
		`for _, name := range sortedObjectNames(oldMethodSet)`,
		`for _, name := range sortedObjectNames(newMethodSet)`,
	} {
		if !strings.Contains(contents, required) {
			t.Fatalf("deterministic apidiff patch must contain %q", required)
		}
	}
}
