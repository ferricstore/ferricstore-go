package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestV080GovernanceLedgerUsesLedgerSchema(t *testing.T) {
	exec := &fakeExecutor{value: []any{}}
	client := NewClientWithExecutor(exec)

	_, err := client.GovernanceLedger(context.Background(), "flow-1", GovernanceLedgerOptions{
		PartitionKey: "partition-1",
		Limit:        Int(10),
		FromMS:       Int64(100),
		ToMS:         Int64(200),
		Rev:          Bool(true),
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []any{
		"FLOW.GOVERNANCE.LEDGER", "flow-1",
		"PARTITION", "partition-1",
		"LIMIT", 10,
		"FROM_MS", int64(100),
		"TO_MS", int64(200),
		"REV", "true",
	}
	if !reflect.DeepEqual(exec.calls[0], want) {
		t.Fatalf("ledger call = %#v; want %#v", exec.calls[0], want)
	}
}

func TestV080GovernanceLedgerRejectsInvalidBoundsBeforeTransport(t *testing.T) {
	tests := []struct {
		name string
		opt  GovernanceLedgerOptions
	}{
		{name: "non-positive limit", opt: GovernanceLedgerOptions{Limit: Int(0)}},
		{name: "negative from", opt: GovernanceLedgerOptions{FromMS: Int64(-1)}},
		{name: "negative to", opt: GovernanceLedgerOptions{ToMS: Int64(-1)}},
		{name: "inverted window", opt: GovernanceLedgerOptions{FromMS: Int64(2), ToMS: Int64(1)}},
		{name: "from above exact range", opt: GovernanceLedgerOptions{FromMS: Int64(maxFlowExactIntegerV080 + 1)}},
		{name: "to above exact range", opt: GovernanceLedgerOptions{ToMS: Int64(maxFlowExactIntegerV080 + 1)}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []any{}}
			_, err := NewClientWithExecutor(exec).GovernanceLedger(context.Background(), "flow-1", test.opt)
			if err == nil {
				t.Fatal("invalid ledger options succeeded")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid ledger options reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestV080GovernanceListsUseScopeBeforePartition(t *testing.T) {
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "approvals", call: func(client *Client) error {
			_, err := client.ApprovalList(context.Background(), ApprovalListOptions{
				Scope: "scope", PartitionKey: "  ", Limit: Int(10),
			})
			return err
		}},
		{name: "overview", call: func(client *Client) error {
			_, err := client.GovernanceOverview(context.Background(), ApprovalListOptions{
				Scope: "scope", PartitionKey: "  ", Limit: Int(10),
			})
			return err
		}},
		{name: "budgets", call: func(client *Client) error {
			_, err := client.BudgetList(context.Background(), "scope", "  ", Int(10))
			return err
		}},
		{name: "limits", call: func(client *Client) error {
			_, err := client.LimitList(context.Background(), "scope", "  ", Int(10), nil)
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []any{}}
			if test.name == "overview" {
				exec.value = map[string]any{}
			}
			if err := test.call(NewClientWithExecutor(exec)); err != nil {
				t.Fatalf("scope-precedence request failed: %v", err)
			}
			if len(exec.calls) != 1 {
				t.Fatalf("calls = %#v", exec.calls)
			}
			for _, argument := range exec.calls[0] {
				if argument == "PARTITION" {
					t.Fatalf("scope-precedence request retained ignored partition: %#v", exec.calls[0])
				}
			}
		})
	}
}
