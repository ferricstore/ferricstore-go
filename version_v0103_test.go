package ferricstore

import "testing"

func TestProjectionReleaseVersions(t *testing.T) {
	if SDKVersion != "0.10.1" {
		t.Fatalf("SDKVersion = %q", SDKVersion)
	}
	if MinimumServerVersion != "0.10.3" {
		t.Fatalf("MinimumServerVersion = %q", MinimumServerVersion)
	}
}
