package ferricstore

import (
	"context"
	"errors"
	"testing"
)

func TestTypedStoreReplyMethodsRejectLegacyMultiBeforeQueueing(t *testing.T) {
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "hash", call: func(client *Client) error {
			_, err := client.Hash().IncrBy(context.Background(), "key", "field", 1)
			return err
		}},
		{name: "list", call: func(client *Client) error {
			_, err := client.ListStore().LPush(context.Background(), "key", "value")
			return err
		}},
		{name: "set", call: func(client *Client) error {
			_, err := client.SetStore().Add(context.Background(), "key", "value")
			return err
		}},
		{name: "sorted set", call: func(client *Client) error {
			_, err := client.SortedSet().Add(context.Background(), "key", ZAddMember{Score: 1, Member: "value"})
			return err
		}},
		{name: "stream", call: func(client *Client) error {
			_, err := client.Stream().Add(context.Background(), "key", "*", map[string]any{"field": "value"})
			return err
		}},
		{name: "bitmap", call: func(client *Client) error {
			_, err := client.Bitmap().SetBit(context.Background(), "key", 1, 1)
			return err
		}},
		{name: "hyperloglog", call: func(client *Client) error {
			_, err := client.HyperLogLog().Add(context.Background(), "key", "value")
			return err
		}},
		{name: "geo", call: func(client *Client) error {
			_, err := client.Geo().Add(context.Background(), "key", 1, 2, "member")
			return err
		}},
		{name: "bloom", call: func(client *Client) error {
			_, err := client.Bloom().Add(context.Background(), "key", "value")
			return err
		}},
		{name: "cuckoo", call: func(client *Client) error {
			_, err := client.Cuckoo().Add(context.Background(), "key", "value")
			return err
		}},
		{name: "count-min sketch", call: func(client *Client) error {
			_, err := client.CountMinSketch().IncrBy(context.Background(), "key", "value", 1)
			return err
		}},
		{name: "top-k", call: func(client *Client) error {
			_, err := client.TopK().Add(context.Background(), "key", "value")
			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exec := &affineBulkTestExecutor{}
			client := NewClientWithExecutor(exec)
			if err := client.Multi(context.Background()); err != nil {
				t.Fatal(err)
			}
			exec.mu.Lock()
			callsBefore := len(exec.calls)
			exec.mu.Unlock()

			err := tc.call(client)
			if !errors.Is(err, ErrTypedReplyInTransaction) {
				t.Fatalf("typed store error = %T %v; want ErrTypedReplyInTransaction", err, err)
			}
			exec.mu.Lock()
			callsAfter := len(exec.calls)
			exec.mu.Unlock()
			if callsAfter != callsBefore {
				t.Fatalf("typed store queued %d commands; want none", callsAfter-callsBefore)
			}
		})
	}
}

func TestTypedStoreStatusMethodsValidateQueuedReply(t *testing.T) {
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "list set", call: func(client *Client) error {
			return client.ListStore().Set(context.Background(), "key", 0, "value")
		}},
		{name: "list trim", call: func(client *Client) error {
			return client.ListStore().Trim(context.Background(), "key", 0, 1)
		}},
		{name: "hyperloglog merge", call: func(client *Client) error {
			return client.HyperLogLog().Merge(context.Background(), "destination", "source")
		}},
		{name: "stream group create", call: func(client *Client) error {
			return client.Stream().GroupCreate(context.Background(), "key", "group", "0", false)
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exec := &affineBulkTestExecutor{queueResponse: []byte("NOT-QUEUED")}
			client := NewClientWithExecutor(exec)
			if err := client.Multi(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := tc.call(client); err == nil {
				t.Fatal("typed status method accepted malformed queue acknowledgement")
			}
		})
	}
}

func TestTypedClientReplyMethodsRejectLegacyMultiBeforeQueueing(t *testing.T) {
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "rename nx", call: func(client *Client) error {
			_, err := client.RenameNX(context.Background(), "source", "destination")
			return err
		}},
		{name: "compare and set", call: func(client *Client) error {
			_, err := client.CAS(context.Background(), "key", "old", "new", nil)
			return err
		}},
		{name: "flow create", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{ID: "flow", Type: "type"})
			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exec := &affineBulkTestExecutor{}
			client := NewClientWithExecutor(exec)
			if err := client.Multi(context.Background()); err != nil {
				t.Fatal(err)
			}
			exec.mu.Lock()
			callsBefore := len(exec.calls)
			exec.mu.Unlock()

			err := tc.call(client)
			if !errors.Is(err, ErrTypedReplyInTransaction) {
				t.Fatalf("typed client error = %T %v; want ErrTypedReplyInTransaction", err, err)
			}
			exec.mu.Lock()
			callsAfter := len(exec.calls)
			exec.mu.Unlock()
			if callsAfter != callsBefore {
				t.Fatalf("typed client queued %d commands; want none", callsAfter-callsBefore)
			}
		})
	}
}
