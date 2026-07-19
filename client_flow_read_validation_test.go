package ferricstore

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestFlowReadCommandsRejectInvalidArgumentsBeforeTransport(t *testing.T) {
	ctx := context.Background()
	oversizedStateMeta := make(map[string]map[string]any, 64)
	for index := range 64 {
		state := fmt.Sprintf("%02d%s", index, strings.Repeat("s", 62))
		oversizedStateMeta[state] = map[string]any{
			strings.Repeat("k", 64): strings.Repeat("v", 256),
		}
	}
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "get empty id", call: func(c *Client) error { _, err := c.Get(ctx, "", "", nil); return err }},
		{name: "get empty value name", call: func(c *Client) error { _, err := c.Get(ctx, "id", "", []string{""}); return err }},
		{name: "list empty type", call: func(c *Client) error { _, err := c.List(ctx, "", ReadOptions{}); return err }},
		{name: "list zero count", call: func(c *Client) error { _, err := c.List(ctx, "type", ReadOptions{Count: Int(0)}); return err }},
		{name: "list negative time", call: func(c *Client) error { _, err := c.List(ctx, "type", ReadOptions{FromMS: Int64(-1)}); return err }},
		{name: "list reversed range", call: func(c *Client) error {
			_, err := c.List(ctx, "type", ReadOptions{FromMS: Int64(2), ToMS: Int64(1)})
			return err
		}},
		{name: "list invalid attribute", call: func(c *Client) error {
			_, err := c.List(ctx, "type", ReadOptions{Attributes: map[string]any{"bad": map[string]any{"nested": true}}})
			return err
		}},
		{name: "search zero count", call: func(c *Client) error { _, err := c.Search(ctx, SearchOptions{Count: Int(0)}); return err }},
		{name: "search negative time", call: func(c *Client) error { _, err := c.Search(ctx, SearchOptions{ToMS: Int64(-1)}); return err }},
		{name: "search reversed range", call: func(c *Client) error {
			_, err := c.Search(ctx, SearchOptions{FromMS: Int64(2), ToMS: Int64(1)})
			return err
		}},
		{name: "search invalid state meta state", call: func(c *Client) error {
			_, err := c.Search(ctx, SearchOptions{StateMeta: map[string]map[string]any{"": {"attempt": 1}}})
			return err
		}},
		{name: "search invalid state meta value", call: func(c *Client) error {
			_, err := c.Search(ctx, SearchOptions{StateMeta: map[string]map[string]any{"queued": {"attempt": []string{"one"}}}})
			return err
		}},
		{name: "search state meta above aggregate limit", call: func(c *Client) error {
			_, err := c.Search(ctx, SearchOptions{StateMeta: oversizedStateMeta})
			return err
		}},
		{name: "search normalized state collision", call: func(c *Client) error {
			_, err := c.Search(ctx, SearchOptions{StateMeta: map[string]map[string]any{
				"queued": {"attempt": 1}, " queued ": {"attempt": 2},
			}})
			return err
		}},
		{name: "by parent empty id", call: func(c *Client) error { _, err := c.ByParent(ctx, "", ReadOptions{}); return err }},
		{name: "by root empty id", call: func(c *Client) error { _, err := c.ByRoot(ctx, "", ReadOptions{}); return err }},
		{name: "by correlation empty id", call: func(c *Client) error { _, err := c.ByCorrelation(ctx, "", ReadOptions{}); return err }},
		{name: "terminals empty type", call: func(c *Client) error { _, err := c.Terminals(ctx, "", ReadOptions{}); return err }},
		{name: "failures invalid count", call: func(c *Client) error { _, err := c.Failures(ctx, "type", ReadOptions{Count: Int(-1)}); return err }},
		{name: "info empty type", call: func(c *Client) error { _, err := c.Info(ctx, "", "", nil, nil); return err }},
		{name: "stuck empty type", call: func(c *Client) error { _, err := c.Stuck(ctx, "", "", nil, nil, nil); return err }},
		{name: "stuck zero count", call: func(c *Client) error { _, err := c.Stuck(ctx, "type", "", Int(0), nil, nil); return err }},
		{name: "stuck negative age", call: func(c *Client) error { _, err := c.Stuck(ctx, "type", "", nil, Int64(-1), nil); return err }},
		{name: "stuck negative now", call: func(c *Client) error { _, err := c.Stuck(ctx, "type", "", nil, nil, Int64(-1)); return err }},
		{name: "history empty id", call: func(c *Client) error { _, err := c.History(ctx, HistoryOptions{}); return err }},
		{name: "history negative count", call: func(c *Client) error { _, err := c.History(ctx, HistoryOptions{ID: "id", Count: -1}); return err }},
		{name: "history negative time", call: func(c *Client) error {
			_, err := c.History(ctx, HistoryOptions{ID: "id", FromMS: Int64(-1)})
			return err
		}},
		{name: "history reversed time range", call: func(c *Client) error {
			_, err := c.History(ctx, HistoryOptions{ID: "id", FromMS: Int64(2), ToMS: Int64(1)})
			return err
		}},
		{name: "history negative version", call: func(c *Client) error {
			_, err := c.History(ctx, HistoryOptions{ID: "id", FromVersion: Int64(-1)})
			return err
		}},
		{name: "history reversed version range", call: func(c *Client) error {
			_, err := c.History(ctx, HistoryOptions{ID: "id", FromVersion: Int64(2), ToVersion: Int64(1)})
			return err
		}},
		{name: "history negative payload maximum", call: func(c *Client) error {
			_, err := c.History(ctx, HistoryOptions{ID: "id", PayloadMaxBytes: Int64(-1)})
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []any{}}
			if err := test.call(NewClientWithExecutor(exec)); err == nil {
				t.Fatal("invalid Flow read succeeded")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid Flow read reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestV080FlowNamedValueReadsUseOnlySupportedWireOptions(t *testing.T) {
	exec := &fakeExecutor{values: []any{nil, []any{}}}
	client := NewClientWithExecutor(exec)
	if _, err := client.Get(context.Background(), "flow", "partition", []string{"result"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ClaimDue(context.Background(), ClaimDueOptions{
		Type: "type", Worker: "worker", Values: []string{"result"},
	}); err != nil {
		t.Fatal(err)
	}
	if len(exec.calls) != 2 {
		t.Fatalf("wire calls = %d, want 2", len(exec.calls))
	}
	for _, call := range exec.calls {
		for _, arg := range call {
			if arg == "VALUE_MAX_BYTES" {
				t.Fatalf("v0.8-unsupported VALUE_MAX_BYTES reached wire: %#v", call)
			}
		}
	}
}
