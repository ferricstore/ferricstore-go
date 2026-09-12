//go:build integration

package ferricstore

import (
	"strings"
	"testing"
)

func TestExpectedIntegrationCommandsForHTTPExcludeOnlyNativeSessionCommands(t *testing.T) {
	commands := make(map[string]struct{})
	for _, command := range expectedIntegrationCommandsForTransport(true) {
		commands[command] = struct{}{}
	}
	for _, supported := range []string{"GET", "GEOSEARCHSTORE", "FLOW.QUERY"} {
		if _, ok := commands[supported]; !ok {
			t.Fatalf("HTTP coverage omitted supported command %s", supported)
		}
	}
	for _, nativeOnly := range []string{"CLIENT INFO", "FETCH_OR_COMPUTE", "WATCH"} {
		if _, ok := commands[nativeOnly]; ok {
			t.Fatalf("HTTP coverage retained native-only command %s", nativeOnly)
		}
	}
}

func expectedIntegrationCommandsForTransport(http bool) []string {
	commands := expectedIntegrationCommands()
	if !http {
		return commands
	}
	filtered := make([]string, 0, len(commands))
	for _, command := range commands {
		name := strings.Fields(command)[0]
		if HTTPCommandDisposition(name) == "supported" {
			filtered = append(filtered, command)
		}
	}
	return filtered
}
