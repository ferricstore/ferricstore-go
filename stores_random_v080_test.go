package ferricstore

import (
	"context"
	"testing"
)

func TestV080RandomReplacementCountsAreBoundedLocally(t *testing.T) {
	tooMany := -10_001
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{
			name: "HRANDFIELD",
			call: func(client *Client) error {
				_, err := client.Hash().RandField(context.Background(), "hash", &tooMany, false)
				return err
			},
		},
		{
			name: "SRANDMEMBER",
			call: func(client *Client) error {
				_, err := client.SetStore().RandMember(context.Background(), "set", &tooMany)
				return err
			},
		},
		{
			name: "ZRANDMEMBER",
			call: func(client *Client) error {
				_, err := client.SortedSet().RandMember(context.Background(), "zset", &tooMany, false)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []any{}}
			if err := test.call(NewClientWithExecutor(exec)); err == nil {
				t.Fatal("oversized replacement count succeeded")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("oversized replacement count reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestV080RandomReplacementBoundaryRemainsSupported(t *testing.T) {
	count := -10_000
	tests := []func(*Client) error{
		func(client *Client) error {
			_, err := client.Hash().RandField(context.Background(), "hash", &count, false)
			return err
		},
		func(client *Client) error {
			_, err := client.SetStore().RandMember(context.Background(), "set", &count)
			return err
		},
		func(client *Client) error {
			_, err := client.SortedSet().RandMember(context.Background(), "zset", &count, false)
			return err
		},
	}
	for index, call := range tests {
		exec := &fakeExecutor{value: []any{}}
		if err := call(NewClientWithExecutor(exec)); err != nil {
			t.Fatalf("boundary call %d: %v", index, err)
		}
		if len(exec.calls) != 1 {
			t.Fatalf("boundary call %d did not reach transport", index)
		}
	}
}
