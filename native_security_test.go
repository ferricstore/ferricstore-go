package ferricstore

import (
	"strings"
	"testing"
)

func TestNativeCommandExecRejectsMalformedRequestContext(t *testing.T) {
	tests := []struct {
		name    string
		context any
	}{
		{name: "scalar", context: "proxy"},
		{name: "unknown field", context: map[string]any{"scope": "invocation:create:*"}},
		{name: "non string subject", context: map[string]any{"subject": 42}},
		{name: "non string scope", context: map[string]any{"scopes": []any{"invocation:create:*", 42}}},
		{name: "nil pointer", context: (*RequestContext)(nil)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := appendNativeRequestContext([]any{
				"COMMAND_EXEC",
				"INVOCATION.CREATE",
				"send-email",
				"{}",
			}, tt.context)
			_, err := buildNativeCommand(args)
			if err == nil || !strings.Contains(err.Error(), "request context") {
				t.Fatalf("malformed request context error = %v", err)
			}
		})
	}
}
