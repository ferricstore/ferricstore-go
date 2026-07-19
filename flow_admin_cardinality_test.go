package ferricstore

import (
	"context"
	"strings"
	"testing"
)

func TestFlowAdminReadResponsesCannotExceedRequestedLimit(t *testing.T) {
	items := []any{map[string]any{}, map[string]any{}}
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "attributes", call: func(client *Client) error {
			_, err := client.Attributes(context.Background(), "work", ReadOptions{Count: Int(1)})
			return err
		}},
		{name: "attribute values", call: func(client *Client) error {
			_, err := client.AttributeValues(context.Background(), "work", "region", ReadOptions{Count: Int(1)})
			return err
		}},
		{name: "governance ledger", call: func(client *Client) error {
			_, err := client.GovernanceLedger(context.Background(), "flow", GovernanceLedgerOptions{Limit: Int(1)})
			return err
		}},
		{name: "approvals", call: func(client *Client) error {
			_, err := client.ApprovalList(context.Background(), ApprovalListOptions{Limit: Int(1)})
			return err
		}},
		{name: "budgets", call: func(client *Client) error {
			_, err := client.BudgetList(context.Background(), "", "", Int(1))
			return err
		}},
		{name: "limits", call: func(client *Client) error {
			_, err := client.LimitList(context.Background(), "", "", Int(1), nil)
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.call(NewClientWithExecutor(&fakeExecutor{value: items}))
			if err == nil || !strings.Contains(err.Error(), "returned 2 items, limit is 1") {
				t.Fatalf("cardinality error = %v", err)
			}
		})
	}
}

func TestGovernanceOverviewCollectionsCannotExceedRequestedLimit(t *testing.T) {
	for _, collection := range []string{"approvals", "budgets", "limits", "circuits"} {
		t.Run(collection, func(t *testing.T) {
			response := map[string]any{
				"approvals": []any{},
				"budgets":   []any{},
				"limits":    []any{},
				"circuits":  []any{},
			}
			response[collection] = []any{map[string]any{}, map[string]any{}}
			client := NewClientWithExecutor(&fakeExecutor{value: response})
			_, err := client.GovernanceOverview(context.Background(), ApprovalListOptions{Limit: Int(1)})
			if err == nil || !strings.Contains(err.Error(), collection+" returned 2 items, limit is 1") {
				t.Fatalf("cardinality error = %v", err)
			}
		})
	}
}

func TestFlowAdminResponseCannotExceedV080DefaultLimit(t *testing.T) {
	items := make([]any, 101)
	for index := range items {
		items[index] = map[string]any{}
	}
	client := NewClientWithExecutor(&fakeExecutor{value: items})
	if _, err := client.GovernanceLedger(context.Background(), "flow", GovernanceLedgerOptions{}); err == nil ||
		!strings.Contains(err.Error(), "returned 101 items, limit is 100") {
		t.Fatalf("default cardinality error = %v", err)
	}
}

func TestGovernanceResponseUsesV080ClampedLimit(t *testing.T) {
	items := make([]any, 1_001)
	for index := range items {
		items[index] = map[string]any{}
	}
	client := NewClientWithExecutor(&fakeExecutor{value: items})
	if _, err := client.BudgetList(context.Background(), "", "", Int(2_000)); err == nil ||
		!strings.Contains(err.Error(), "returned 1001 items, limit is 1000") {
		t.Fatalf("clamped cardinality error = %v", err)
	}
}
