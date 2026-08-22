package ferricstore

import "testing"

func TestCompactQueryStorageReleaseVersions(t *testing.T) {
	if SDKVersion != "0.11.10" {
		t.Fatalf("SDKVersion = %q", SDKVersion)
	}
	if MinimumServerVersion != "0.11.4" {
		t.Fatalf("MinimumServerVersion = %q", MinimumServerVersion)
	}
}
