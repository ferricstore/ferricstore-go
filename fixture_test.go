package ferricstore

import (
	"os"
	"strings"
	"testing"
)

func TestIntegrationDockerScriptDefaultsToFerricStore075(t *testing.T) {
	body, err := os.ReadFile("scripts/integration-docker.sh")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "ghcr.io/ferricstore/ferricstore:0.7.5") {
		t.Fatalf("integration docker script should default to FerricStore 0.7.5")
	}
}
