package ferricstore

import (
	"context"
	"testing"
)

func TestClaimDueRejectsInvalidOptionsBeforeTransport(t *testing.T) {
	tests := []struct {
		name string
		opt  ClaimDueOptions
	}{
		{name: "missing type", opt: ClaimDueOptions{Worker: "worker"}},
		{name: "missing worker", opt: ClaimDueOptions{Type: "email"}},
		{name: "negative lease", opt: ClaimDueOptions{Type: "email", Worker: "worker", LeaseMS: -1}},
		{name: "negative limit", opt: ClaimDueOptions{Type: "email", Worker: "worker", Limit: -1}},
		{name: "negative now", opt: ClaimDueOptions{Type: "email", Worker: "worker", NowMS: -1}},
		{name: "negative block", opt: ClaimDueOptions{Type: "email", Worker: "worker", BlockMS: Int64(-1)}},
		{name: "negative reclaim ratio", opt: ClaimDueOptions{Type: "email", Worker: "worker", ReclaimRatio: Int64(-1)}},
		{name: "large reclaim ratio", opt: ClaimDueOptions{Type: "email", Worker: "worker", ReclaimRatio: Int64(101)}},
		{name: "negative priority", opt: ClaimDueOptions{Type: "email", Worker: "worker", Priority: Int64(-1)}},
		{name: "large priority", opt: ClaimDueOptions{Type: "email", Worker: "worker", Priority: Int64(3)}},
		{name: "empty state", opt: ClaimDueOptions{Type: "email", Worker: "worker", States: []string{"queued", ""}}},
		{name: "ANY with state", opt: ClaimDueOptions{Type: "email", Worker: "worker", States: []string{"ANY", "queued"}}},
		{name: "empty partition", opt: ClaimDueOptions{Type: "email", Worker: "worker", PartitionKeys: []string{"tenant", ""}}},
		{name: "empty value name", opt: ClaimDueOptions{Type: "email", Worker: "worker", Values: []string{"result", ""}}},
		{name: "negative payload cap", opt: ClaimDueOptions{Type: "email", Worker: "worker", PayloadMaxBytes: Int64(-1)}},
		{name: "negative value cap", opt: ClaimDueOptions{Type: "email", Worker: "worker", ValueMaxBytes: Int64(-1)}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []any{}}
			if _, err := NewClientWithExecutor(exec).ClaimDue(context.Background(), tc.opt); err == nil {
				t.Fatal("invalid claim options were accepted")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid claim reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestReclaimRejectsInvalidOptionsBeforeTransport(t *testing.T) {
	tests := []struct {
		name string
		opt  ReclaimOptions
	}{
		{name: "missing type", opt: ReclaimOptions{Worker: "worker"}},
		{name: "missing worker", opt: ReclaimOptions{Type: "email"}},
		{name: "negative lease", opt: ReclaimOptions{Type: "email", Worker: "worker", LeaseMS: -1}},
		{name: "negative limit", opt: ReclaimOptions{Type: "email", Worker: "worker", Limit: -1}},
		{name: "negative now", opt: ReclaimOptions{Type: "email", Worker: "worker", NowMS: -1}},
		{name: "large priority", opt: ReclaimOptions{Type: "email", Worker: "worker", Priority: Int64(3)}},
		{name: "empty partition", opt: ReclaimOptions{Type: "email", Worker: "worker", PartitionKeys: []string{"tenant", ""}}},
		{name: "empty value name", opt: ReclaimOptions{Type: "email", Worker: "worker", Values: []string{"result", ""}}},
		{name: "negative payload cap", opt: ReclaimOptions{Type: "email", Worker: "worker", PayloadMaxBytes: Int64(-1)}},
		{name: "negative value cap", opt: ReclaimOptions{Type: "email", Worker: "worker", ValueMaxBytes: Int64(-1)}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []any{}}
			if _, err := NewClientWithExecutor(exec).Reclaim(context.Background(), tc.opt); err == nil {
				t.Fatal("invalid reclaim options were accepted")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid reclaim reached transport: %#v", exec.calls)
			}
		})
	}
}
