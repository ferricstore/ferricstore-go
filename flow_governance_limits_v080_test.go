package ferricstore

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestV080GovernanceRejectsExactIntegerOverflowBeforeTransport(t *testing.T) {
	t.Parallel()

	tooLarge := maxFlowExactIntegerV080 + 1
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "effect fencing token", call: func(client *Client) error {
			_, err := client.EffectReserve(context.Background(), "flow", "send", "email", EffectReserveOptions{
				LeaseToken: "lease", FencingToken: &tooLarge, OperationDigest: "digest",
			})
			return err
		}},
		{name: "effect latency", call: func(client *Client) error {
			_, err := client.EffectConfirm(context.Background(), "flow", "send", EffectStatusOptions{
				LeaseToken: "lease", FencingToken: Int64(1), LatencyMS: &tooLarge,
			})
			return err
		}},
		{name: "approval timeout", call: func(client *Client) error {
			_, err := client.ApprovalRequest(context.Background(), "approval", ApprovalRequestOptions{
				FlowID: "flow", Scope: "scope", TimeoutMS: &tooLarge,
			})
			return err
		}},
		{name: "approval policy version", call: func(client *Client) error {
			_, err := client.ApprovalRequest(context.Background(), "approval", ApprovalRequestOptions{
				FlowID: "flow", Scope: "scope", PolicyVersion: tooLarge,
			})
			return err
		}},
		{name: "circuit open duration", call: func(client *Client) error {
			_, err := client.CircuitOpenWithOptions(context.Background(), "scope", CircuitOpenOptions{OpenMS: &tooLarge})
			return err
		}},
		{name: "budget amount", call: func(client *Client) error {
			_, err := client.BudgetReserve(context.Background(), "scope", tooLarge, Int64(10), Int64(10), "reservation", nil)
			return err
		}},
		{name: "budget actual amount", call: func(client *Client) error {
			_, err := client.BudgetCommit(context.Background(), "scope", "reservation", tooLarge, nil, nil)
			return err
		}},
		{name: "limit lease ttl", call: func(client *Client) error {
			_, err := client.LimitLease(context.Background(), "scope", 0, 1, tooLarge, Int64(10), nil)
			return err
		}},
		{name: "limit lease deadline", call: func(client *Client) error {
			_, err := client.LimitLease(context.Background(), "scope", 0, 1, 1, Int64(10), Int64(maxFlowExactIntegerV080))
			return err
		}},
		{name: "limit get now", call: func(client *Client) error {
			_, err := client.LimitGet(context.Background(), "scope", &tooLarge)
			return err
		}},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			exec := &fakeExecutor{}
			if err := test.call(NewClientWithExecutor(exec)); err == nil {
				t.Fatal("invalid v0.8 governance request succeeded")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid governance request reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestV080GovernanceRejectsBoundedFieldViolationsBeforeTransport(t *testing.T) {
	t.Parallel()

	dimension := strings.Repeat("d", maxGovernanceDimensionBytesV080+1)
	field := strings.Repeat("f", maxGovernanceFieldBytesV080+1)
	reservation := strings.Repeat("r", maxGovernanceReservationIDBytesV080+1)
	errorClass := strings.Repeat("e", maxCircuitErrorClassBytesV080+1)
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "effect id", call: func(client *Client) error {
			_, err := client.EffectGet(context.Background(), dimension, "effect", "")
			return err
		}},
		{name: "effect operation digest", call: func(client *Client) error {
			_, err := client.EffectReserve(context.Background(), "flow", "effect", "type", EffectReserveOptions{
				LeaseToken: "lease", FencingToken: Int64(1), OperationDigest: field,
			})
			return err
		}},
		{name: "effect terminal reason", call: func(client *Client) error {
			_, err := client.EffectFail(context.Background(), "flow", "effect", EffectStatusOptions{
				LeaseToken: "lease", FencingToken: Int64(1), Reason: field,
			})
			return err
		}},
		{name: "approval scope", call: func(client *Client) error {
			_, err := client.ApprovalRequest(context.Background(), "approval", ApprovalRequestOptions{FlowID: "flow", Scope: dimension})
			return err
		}},
		{name: "approval reason", call: func(client *Client) error {
			_, err := client.ApprovalRequest(context.Background(), "approval", ApprovalRequestOptions{FlowID: "flow", Scope: "scope", Reason: field})
			return err
		}},
		{name: "approval too many assignees", call: func(client *Client) error {
			_, err := client.ApprovalRequest(context.Background(), "approval", ApprovalRequestOptions{
				FlowID: "flow", Scope: "scope", Assignees: make([]string, maxApprovalAssigneesV080+1),
			})
			return err
		}},
		{name: "circuit error class", call: func(client *Client) error {
			_, err := client.CircuitOpenWithOptions(context.Background(), "scope", CircuitOpenOptions{ErrorClasses: []string{errorClass}})
			return err
		}},
		{name: "circuit too many error classes", call: func(client *Client) error {
			classes := make([]string, maxCircuitErrorClassesV080+1)
			for index := range classes {
				classes[index] = "class"
			}
			_, err := client.CircuitOpenWithOptions(context.Background(), "scope", CircuitOpenOptions{ErrorClasses: classes})
			return err
		}},
		{name: "budget reservation id", call: func(client *Client) error {
			_, err := client.BudgetRelease(context.Background(), "scope", reservation, nil)
			return err
		}},
		{name: "limit scope", call: func(client *Client) error {
			_, err := client.LimitGet(context.Background(), dimension, nil)
			return err
		}},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			exec := &fakeExecutor{}
			if err := test.call(NewClientWithExecutor(exec)); err == nil {
				t.Fatal("oversized v0.8 governance request succeeded")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("oversized governance request reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestV080ApprovalExpiryAndExpiredListStatus(t *testing.T) {
	t.Parallel()

	exec := &fakeExecutor{}
	client := NewClientWithExecutor(exec)
	_, err := client.ApprovalRequest(context.Background(), "approval", ApprovalRequestOptions{
		FlowID: "flow", Scope: "scope", NowMS: Int64(10), ExpiresAtMS: Int64(10),
	})
	if err == nil || len(exec.calls) != 0 {
		t.Fatalf("non-future expiry: err=%v calls=%#v", err, exec.calls)
	}

	exec.value = []any{}
	if _, err := client.ApprovalList(context.Background(), ApprovalListOptions{Status: "expired"}); err != nil {
		t.Fatalf("v0.8 expired approval filter failed: %v", err)
	}
}

func TestV080BudgetUsageRejectsUnboundedTermsBeforeTransport(t *testing.T) {
	t.Parallel()

	deep := map[string]any{"leaf": "value"}
	for range maxGovernanceUsageDepthV080 + 1 {
		deep = map[string]any{"child": deep}
	}
	wide := make(map[string]any, maxGovernanceUsageNodesV080+1)
	for index := 0; index < maxGovernanceUsageNodesV080+1; index++ {
		wide[string(rune(index+1))] = index
	}
	tests := []map[string]any{
		deep,
		wide,
		{"large": strings.Repeat("x", maxGovernanceUsageBytesV080+1)},
		{"unsupported": make(chan int)},
	}
	for index, usage := range tests {
		exec := &fakeExecutor{}
		_, err := NewClientWithExecutor(exec).BudgetCommit(context.Background(), "scope", "reservation", 1, usage, nil)
		if err == nil {
			t.Fatalf("case %d accepted unbounded usage", index)
		}
		if len(exec.calls) != 0 {
			t.Fatalf("case %d reached transport: %#v", index, exec.calls)
		}
	}
}

func TestV080BudgetUsageUsesServerExternalTermByteLimit(t *testing.T) {
	usage := make(map[string]any, 1_001)
	for index := 0; index < 1_000; index++ {
		usage[fmt.Sprintf("k%04d", index)] = 1
	}
	// FerricStore v0.8.0 measures :erlang.external_size/1. This term is
	// exactly 262,144 bytes in ETF even though its native request encoding is
	// larger because native integers always occupy eight payload bytes.
	usage["blob"] = strings.Repeat("x", 250_124)
	if err := validateGovernanceUsage(usage); err != nil {
		t.Fatalf("server-valid boundary usage was rejected: %v", err)
	}

	usage["blob"] = strings.Repeat("x", 250_125)
	if err := validateGovernanceUsage(usage); err == nil {
		t.Fatal("usage above the server ETF limit was accepted")
	}
}

func TestV080GovernanceUsageExternalSizesMatchServerTerms(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  int
	}{
		{name: "nil", value: nil, want: 6},
		{name: "true", value: true, want: 7},
		{name: "false", value: false, want: 8},
		{name: "small integer", value: 255, want: 3},
		{name: "integer", value: 256, want: 6},
		{name: "big integer", value: int64(2_147_483_648), want: 8},
		{name: "float", value: 1.0, want: 10},
		{name: "binary", value: []byte{1}, want: 7},
		{name: "empty list", value: []any{}, want: 2},
		{name: "byte list", value: []any{1}, want: 5},
		{name: "empty map", value: map[string]any{}, want: 6},
		{name: "map", value: map[string]any{"a": 1}, want: 14},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := governanceUsageValidation{remaining: maxGovernanceUsageNodesV080}
			result, err := state.value(reflect.ValueOf(test.value), 0)
			if err != nil {
				t.Fatal(err)
			}
			if got := result.externalSize + 1; got != test.want {
				t.Fatalf("external size = %d; want %d", got, test.want)
			}
		})
	}
}
