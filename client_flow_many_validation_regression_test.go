package ferricstore

import (
	"context"
	"testing"
)

func TestFlowManyMutationsRejectInvalidItemsBeforeCodecOrTransport(t *testing.T) {
	zero := int64(0)
	priority := int64(3)
	claimed := func() []ClaimedItem {
		return []ClaimedItem{{ID: "flow", PartitionKey: "tenant", LeaseToken: "lease", FencingToken: 1}}
	}
	fenced := func() []FencedItem {
		return []FencedItem{{ID: "flow", PartitionKey: "tenant", LeaseToken: "lease", FencingToken: 1}}
	}
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "create missing type", call: func(client *Client) error {
			_, err := client.CreateMany(context.Background(), CreateManyOptions{Items: []CreateItem{{ID: "flow", Payload: "payload"}}})
			return err
		}},
		{name: "create empty id", call: func(client *Client) error {
			_, err := client.CreateMany(context.Background(), CreateManyOptions{Type: "order", Items: []CreateItem{{Payload: "payload"}}})
			return err
		}},
		{name: "create duplicate id", call: func(client *Client) error {
			_, err := client.CreateMany(context.Background(), CreateManyOptions{Type: "order", Items: []CreateItem{{ID: "flow", Payload: "payload"}, {ID: "flow", Payload: "payload"}}})
			return err
		}},
		{name: "create invalid priority", call: func(client *Client) error {
			_, err := client.CreateMany(context.Background(), CreateManyOptions{Type: "order", Priority: &priority, Items: []CreateItem{{ID: "flow", Payload: "payload"}}})
			return err
		}},
		{name: "complete empty id", call: func(client *Client) error {
			items := claimed()
			items[0].ID = ""
			_, err := client.CompleteMany(context.Background(), CompleteManyOptions{PartitionKey: "tenant", Items: items, Payload: "payload"})
			return err
		}},
		{name: "complete empty lease", call: func(client *Client) error {
			items := claimed()
			items[0].LeaseToken = ""
			_, err := client.CompleteMany(context.Background(), CompleteManyOptions{PartitionKey: "tenant", Items: items, Payload: "payload"})
			return err
		}},
		{name: "complete negative fencing", call: func(client *Client) error {
			items := claimed()
			items[0].FencingToken = -1
			_, err := client.CompleteMany(context.Background(), CompleteManyOptions{PartitionKey: "tenant", Items: items, Payload: "payload"})
			return err
		}},
		{name: "complete invalid ttl", call: func(client *Client) error {
			_, err := client.CompleteMany(context.Background(), CompleteManyOptions{PartitionKey: "tenant", Items: claimed(), TTLMS: &zero, Payload: "payload"})
			return err
		}},
		{name: "transition missing state", call: func(client *Client) error {
			_, err := client.TransitionMany(context.Background(), TransitionManyOptions{PartitionKey: "tenant", ToState: "ready", Items: fenced(), Payload: "payload"})
			return err
		}},
		{name: "transition to running", call: func(client *Client) error {
			_, err := client.TransitionMany(context.Background(), TransitionManyOptions{PartitionKey: "tenant", FromState: "queued", ToState: "running", Items: fenced(), Payload: "payload"})
			return err
		}},
		{name: "transition negative fencing", call: func(client *Client) error {
			items := fenced()
			items[0].FencingToken = -1
			_, err := client.TransitionMany(context.Background(), TransitionManyOptions{PartitionKey: "tenant", FromState: "queued", ToState: "ready", Items: items, Payload: "payload"})
			return err
		}},
		{name: "retry missing lease", call: func(client *Client) error {
			items := claimed()
			items[0].LeaseToken = ""
			_, err := client.RetryMany(context.Background(), RetryManyOptions{PartitionKey: "tenant", Items: items, Payload: "payload"})
			return err
		}},
		{name: "retry unsupported named values", call: func(client *Client) error {
			_, err := client.RetryMany(context.Background(), RetryManyOptions{PartitionKey: "tenant", Items: claimed(), Payload: "payload", NamedValues: NamedValues{Values: map[string]any{"v": "payload"}}})
			return err
		}},
		{name: "fail negative now", call: func(client *Client) error {
			_, err := client.FailMany(context.Background(), FailManyOptions{PartitionKey: "tenant", Items: claimed(), NowMS: -1, Payload: "payload"})
			return err
		}},
		{name: "cancel empty id", call: func(client *Client) error {
			items := fenced()
			items[0].ID = ""
			_, err := client.CancelMany(context.Background(), CancelManyOptions{PartitionKey: "tenant", Items: items, Reason: "payload"})
			return err
		}},
		{name: "cancel negative fencing", call: func(client *Client) error {
			items := fenced()
			items[0].FencingToken = -1
			_, err := client.CancelMany(context.Background(), CancelManyOptions{PartitionKey: "tenant", Items: items, Reason: "payload"})
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			codec := &countingKVCodec{}
			if err := test.call(NewClientWithExecutor(exec, WithCodec(codec))); err == nil {
				t.Fatal("invalid batch mutation was accepted")
			}
			if calls := codec.encodes.Load(); calls != 0 {
				t.Fatalf("invalid batch mutation invoked codec %d times", calls)
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid batch mutation reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestCreateManyRejectsServerBatchLimitBeforeTransport(t *testing.T) {
	items := make([]CreateItem, 1001)
	for index := range items {
		items[index] = CreateItem{ID: string(rune(index + 1))}
	}
	exec := &fakeExecutor{value: []byte("OK")}
	_, err := NewClientWithExecutor(exec).CreateMany(context.Background(), CreateManyOptions{Type: "order", Items: items})
	if err == nil {
		t.Fatal("oversized create batch was accepted")
	}
	if len(exec.calls) != 0 {
		t.Fatalf("oversized batch reached transport: %#v", exec.calls)
	}
}
