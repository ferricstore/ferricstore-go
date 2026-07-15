package ferricstore

import (
	"context"
	"testing"
)

func TestScanHelpersRejectInvalidCursorAndCountBeforeTransport(t *testing.T) {
	zero := 0
	negative := -1
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{
			name: "scan negative cursor",
			call: func(client *Client) error {
				_, err := client.Scan(context.Background(), -1, "", nil)
				return err
			},
		},
		{
			name: "scan zero count",
			call: func(client *Client) error {
				_, err := client.Scan(context.Background(), 0, "", &zero)
				return err
			},
		},
		{
			name: "hscan negative count",
			call: func(client *Client) error {
				_, err := client.Hash().Scan(context.Background(), "key", 0, "", &negative)
				return err
			},
		},
		{
			name: "sscan negative cursor",
			call: func(client *Client) error {
				_, err := client.SetStore().Scan(context.Background(), "key", -1, "", nil)
				return err
			},
		},
		{
			name: "zscan zero count",
			call: func(client *Client) error {
				_, err := client.SortedSet().Scan(context.Background(), "key", 0, "", &zero)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []any{"0", []any{}}}
			if err := test.call(NewClientWithExecutor(exec)); err == nil {
				t.Fatal("invalid scan input succeeded")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid scan reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestDBSizeRejectsNegativeCardinality(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{value: int64(-1)})
	if _, err := client.DBSize(context.Background()); err == nil {
		t.Fatal("DBSIZE accepted a negative cardinality")
	}
}

func TestScanHelpersForwardServerCursorTokens(t *testing.T) {
	tests := []struct {
		name       string
		cursor     string
		wantPrefix []any
		call       func(*Client, string) error
	}{
		{
			name:       "scan key cursor",
			cursor:     "last:key",
			wantPrefix: []any{"SCAN", "last:key"},
			call: func(client *Client, cursor string) error {
				_, err := client.Scan(context.Background(), cursor, "", nil)
				return err
			},
		},
		{
			name:       "hscan opaque cursor",
			cursor:     "~ZmllbGQ",
			wantPrefix: []any{"HSCAN", "key", "~ZmllbGQ"},
			call: func(client *Client, cursor string) error {
				_, err := client.Hash().Scan(context.Background(), "key", cursor, "", nil)
				return err
			},
		},
		{
			name:       "sscan opaque cursor",
			cursor:     "~bWVtYmVy",
			wantPrefix: []any{"SSCAN", "key", "~bWVtYmVy"},
			call: func(client *Client, cursor string) error {
				_, err := client.SetStore().Scan(context.Background(), "key", cursor, "", nil)
				return err
			},
		},
		{
			name:       "zscan opaque cursor",
			cursor:     "~c29ydGVk",
			wantPrefix: []any{"ZSCAN", "key", "~c29ydGVk"},
			call: func(client *Client, cursor string) error {
				_, err := client.SortedSet().Scan(context.Background(), "key", cursor, "", nil)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []any{"0", []any{}}}
			if err := test.call(NewClientWithExecutor(exec), test.cursor); err != nil {
				t.Fatal(err)
			}
			assertCall(t, exec, test.wantPrefix)
		})
	}
}
