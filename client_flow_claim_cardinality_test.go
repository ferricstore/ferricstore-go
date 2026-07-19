package ferricstore

import (
	"context"
	"strings"
	"testing"
)

func TestFlowClaimResponsesCannotExceedRequestedLimit(t *testing.T) {
	records := []any{map[string]any{"id": "one"}, map[string]any{"id": "two"}}
	jobs := []any{
		map[string]any{"id": "one", "lease_token": "lease-1", "fencing_token": int64(1)},
		map[string]any{"id": "two", "lease_token": "lease-2", "fencing_token": int64(2)},
	}
	tests := []struct {
		name     string
		response any
		call     func(*Client) error
	}{
		{name: "claim records", response: records, call: func(client *Client) error {
			_, err := client.ClaimDue(context.Background(), ClaimDueOptions{Type: "work", Worker: "worker", Limit: 1})
			return err
		}},
		{name: "claim jobs", response: jobs, call: func(client *Client) error {
			_, err := client.ClaimJobs(context.Background(), ClaimDueOptions{Type: "work", Worker: "worker", Limit: 1})
			return err
		}},
		{name: "reclaim records", response: records, call: func(client *Client) error {
			_, err := client.Reclaim(context.Background(), ReclaimOptions{Type: "work", Worker: "worker", Limit: 1})
			return err
		}},
		{name: "reclaim jobs", response: jobs, call: func(client *Client) error {
			_, err := client.ReclaimJobs(context.Background(), ReclaimOptions{Type: "work", Worker: "worker", Limit: 1})
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.call(NewClientWithExecutor(&fakeExecutor{value: test.response}))
			if err == nil || !strings.Contains(err.Error(), "returned 2 items, limit is 1") {
				t.Fatalf("cardinality error = %v", err)
			}
		})
	}
}

func TestFlowClaimDefaultLimitBoundsResponsesToOneItem(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{value: []any{
		map[string]any{"id": "one"}, map[string]any{"id": "two"},
	}})
	if _, err := client.ClaimDue(context.Background(), ClaimDueOptions{
		Type: "work", Worker: "worker",
	}); err == nil || !strings.Contains(err.Error(), "limit is 1") {
		t.Fatalf("default-limit error = %v", err)
	}
}
