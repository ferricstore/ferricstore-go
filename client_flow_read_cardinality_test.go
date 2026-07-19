package ferricstore

import (
	"context"
	"strings"
	"testing"
)

func TestFlowReadResponsesCannotExceedRequestedCount(t *testing.T) {
	records := []any{map[string]any{"id": "one"}, map[string]any{"id": "two"}}
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "list", call: func(client *Client) error {
			_, err := client.List(context.Background(), "work", ReadOptions{Count: Int(1)})
			return err
		}},
		{name: "search", call: func(client *Client) error {
			_, err := client.Search(context.Background(), SearchOptions{Count: Int(1)})
			return err
		}},
		{name: "lineage", call: func(client *Client) error {
			_, err := client.ByParent(context.Background(), "parent", ReadOptions{Count: Int(1)})
			return err
		}},
		{name: "stuck", call: func(client *Client) error {
			_, err := client.Stuck(context.Background(), "work", "", Int(1), nil, nil)
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.call(NewClientWithExecutor(&fakeExecutor{value: records}))
			if err == nil || !strings.Contains(err.Error(), "returned 2 items, limit is 1") {
				t.Fatalf("cardinality error = %v", err)
			}
		})
	}
}

func TestFlowHistoryResponseCannotExceedRequestedCount(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{value: []any{
		map[string]any{"event": "one"}, map[string]any{"event": "two"},
	}})
	if _, err := client.History(context.Background(), HistoryOptions{
		ID: "flow", Count: 1,
	}); err == nil || !strings.Contains(err.Error(), "returned 2 items, limit is 1") {
		t.Fatalf("history cardinality error = %v", err)
	}
}

func TestFlowReadResponseCannotExceedV080DefaultCount(t *testing.T) {
	records := make([]any, 101)
	for index := range records {
		records[index] = map[string]any{"id": "flow"}
	}
	client := NewClientWithExecutor(&fakeExecutor{value: records})
	if _, err := client.List(context.Background(), "work", ReadOptions{}); err == nil ||
		!strings.Contains(err.Error(), "returned 101 items, limit is 100") {
		t.Fatalf("default cardinality error = %v", err)
	}
}
