package ferricstore

import "testing"

func TestCompactQueryStorageReleaseVersions(t *testing.T) {
	if SDKVersion != "0.12.5" {
		t.Fatalf("SDKVersion = %q", SDKVersion)
	}
	if MinimumServerVersion != "0.11.4" {
		t.Fatalf("MinimumServerVersion = %q", MinimumServerVersion)
	}
}
