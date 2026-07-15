package ferricstore

import (
	"math"
	"testing"
	"time"
)

func TestBlockingSortedSetPopExtendsNativeRequestBudget(t *testing.T) {
	for _, command := range []string{"BZPOPMIN", "BZPOPMAX"} {
		budget := blockingCommandBudget([]any{command, "one", "two", 1.5})
		if budget.disableDefault || budget.extension != 1500*time.Millisecond {
			t.Fatalf("%s budget = %+v, want 1.5s extension", command, budget)
		}
		budget = blockingCommandBudget([]any{command, "one", 0})
		if !budget.disableDefault || budget.extension != 0 {
			t.Fatalf("%s zero-timeout budget = %+v, want default timeout disabled", command, budget)
		}
	}
}

func TestStreamBlockingBudgetStopsParsingAtStreamOperands(t *testing.T) {
	for _, args := range [][]any{
		{"XREAD", "STREAMS", "BLOCK", "0"},
		{"XREADGROUP", "GROUP", "BLOCK", "0", "STREAMS", "orders", ">"},
	} {
		if budget := blockingCommandBudget(args); budget != (nativeRequestBudget{}) {
			t.Fatalf("non-blocking stream command %#v received budget %+v", args, budget)
		}
	}

	for _, args := range [][]any{
		{"XREAD", "BLOCK", 0, "STREAMS", "BLOCK", "0"},
		{"XREADGROUP", "GROUP", "group", "consumer", "BLOCK", 0, "STREAMS", "orders", ">"},
	} {
		if budget := blockingCommandBudget(args); !budget.disableDefault || budget.extension != 0 {
			t.Fatalf("blocking stream command %#v received budget %+v", args, budget)
		}
	}
}

func TestNonFiniteBlockingTimeoutsDoNotDisableNativeDeadlines(t *testing.T) {
	for _, timeout := range []any{
		"NaN", "+Inf", "Inf", "Infinity", "-Inf",
		math.NaN(), math.Inf(1), math.Inf(-1),
	} {
		budget := blockingCommandBudget([]any{"BLPOP", "queue", timeout})
		if budget != (nativeRequestBudget{}) {
			t.Fatalf("non-finite timeout %#v received native budget %+v", timeout, budget)
		}
	}
}
