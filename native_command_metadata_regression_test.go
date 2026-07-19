package ferricstore

import (
	"testing"
	"time"
)

func TestCommandExecPreservesNestedCASReplayPolicy(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"COMMAND_EXEC", "FLOW.POLICY.SET", "orders",
		"EXPECTED_GENERATION", int64(7),
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.replayPolicy != nativeReplayNever {
		t.Fatalf("wrapped policy CAS replay policy = %d, want never", command.replayPolicy)
	}

	_, _, replayPolicy, err := nativePipelinePayloadWithReplayPolicy(
		[][]any{{
			"COMMAND_EXEC", "FLOW.POLICY.SET", "orders",
			"EXPECTED_GENERATION", int64(7),
		}},
		1,
		nativeDefaultRequestFrameBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	if replayPolicy != nativeReplayNever {
		t.Fatalf("pipeline containing wrapped policy CAS replay policy = %d, want never", replayPolicy)
	}
}

func TestCommandExecDoesNotMistakeStatePolicyValueForGenerationCAS(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"COMMAND_EXEC", "FLOW.POLICY.SET", "orders",
		"STATE", "queued", "EXHAUSTED_TO", "EXPECTED_GENERATION",
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.replayPolicy != nativeReplayDefault {
		t.Fatalf("non-CAS state policy replay policy = %d, want default", command.replayPolicy)
	}
}

func TestCommandExecPreservesNestedBlockingBudget(t *testing.T) {
	tests := []struct {
		name string
		args []any
		want nativeRequestBudget
	}{
		{
			name: "indefinite stream read",
			args: []any{"COMMAND_EXEC", "XREAD", "BLOCK", 0, "STREAMS", "events", "$"},
			want: nativeRequestBudget{disableDefault: true},
		},
		{
			name: "finite list pop",
			args: []any{"COMMAND_EXEC", "BLPOP", "queue", 2},
			want: nativeRequestBudget{extension: 2 * time.Second},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := blockingCommandBudget(test.args); got != test.want {
				t.Fatalf("wrapped blocking budget = %+v, want %+v", got, test.want)
			}
		})
	}
}
