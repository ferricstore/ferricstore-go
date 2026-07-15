package ferricstore

import (
	"context"
	"strconv"
	"testing"
)

func TestCollectionMutationsRejectImpossibleCounts(t *testing.T) {
	tests := []struct {
		name      string
		requested int
		call      func(*Client) error
	}{
		{name: "HSET", requested: 1, call: func(client *Client) error {
			_, err := client.Hash().Set(context.Background(), "key", "field", "value")
			return err
		}},
		{name: "HDEL", requested: 2, call: func(client *Client) error {
			_, err := client.Hash().Del(context.Background(), "key", "one", "two")
			return err
		}},
		{name: "SADD", requested: 2, call: func(client *Client) error {
			_, err := client.SetStore().Add(context.Background(), "key", "one", "two")
			return err
		}},
		{name: "SREM", requested: 2, call: func(client *Client) error {
			_, err := client.SetStore().Remove(context.Background(), "key", "one", "two")
			return err
		}},
		{name: "ZADD", requested: 2, call: func(client *Client) error {
			_, err := client.SortedSet().Add(context.Background(), "key",
				ZAddMember{Score: 1, Member: "one"}, ZAddMember{Score: 2, Member: "two"})
			return err
		}},
		{name: "ZADD CH", requested: 2, call: func(client *Client) error {
			_, err := client.SortedSet().AddWithOptions(context.Background(), "key", ZAddOptions{CH: true},
				ZAddMember{Score: 1, Member: "one"}, ZAddMember{Score: 2, Member: "two"})
			return err
		}},
		{name: "ZREM", requested: 2, call: func(client *Client) error {
			_, err := client.SortedSet().Rem(context.Background(), "key", "one", "two")
			return err
		}},
		{name: "XDEL", requested: 2, call: func(client *Client) error {
			_, err := client.Stream().Del(context.Background(), "key", "1-0", "2-0")
			return err
		}},
		{name: "XACK", requested: 2, call: func(client *Client) error {
			_, err := client.Stream().Ack(context.Background(), "key", "group", "1-0", "2-0")
			return err
		}},
	}

	for _, tc := range tests {
		for _, response := range []int64{-1, int64(tc.requested + 1)} {
			t.Run(tc.name+"/"+strconv.FormatInt(response, 10), func(t *testing.T) {
				client := NewClientWithExecutor(&fakeExecutor{value: response})
				if err := tc.call(client); err == nil {
					t.Fatalf("%s accepted count %d for %d requested items", tc.name, response, tc.requested)
				}
			})
		}
	}
}

func TestEmptyCollectionMutationsAreLocalNoops(t *testing.T) {
	tests := []struct {
		name string
		call func(*Client) (int64, error)
	}{
		{name: "HDEL", call: func(client *Client) (int64, error) {
			return client.Hash().Del(context.Background(), "key")
		}},
		{name: "SADD", call: func(client *Client) (int64, error) {
			return client.SetStore().Add(context.Background(), "key")
		}},
		{name: "SREM", call: func(client *Client) (int64, error) {
			return client.SetStore().Remove(context.Background(), "key")
		}},
		{name: "ZADD", call: func(client *Client) (int64, error) {
			return client.SortedSet().Add(context.Background(), "key")
		}},
		{name: "ZADD options", call: func(client *Client) (int64, error) {
			return client.SortedSet().AddWithOptions(context.Background(), "key", ZAddOptions{})
		}},
		{name: "ZREM", call: func(client *Client) (int64, error) {
			return client.SortedSet().Rem(context.Background(), "key")
		}},
		{name: "XDEL", call: func(client *Client) (int64, error) {
			return client.Stream().Del(context.Background(), "key")
		}},
		{name: "XACK", call: func(client *Client) (int64, error) {
			return client.Stream().Ack(context.Background(), "key", "group")
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exec := &fakeExecutor{value: int64(99)}
			count, err := tc.call(NewClientWithExecutor(exec))
			if err != nil || count != 0 {
				t.Fatalf("empty mutation = %d, %v; want 0, nil", count, err)
			}
			if len(exec.calls) != 0 {
				t.Fatalf("empty mutation dispatched %d commands; want none", len(exec.calls))
			}
		})
	}
}
