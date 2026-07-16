package ferricstore

import (
	"context"
	"math"
	"reflect"
	"testing"
)

func TestFlowMutationsRejectInvalidMetadataBeforeCodecOrTransport(t *testing.T) {
	tooMany := make(map[string]any, 17)
	for index := range 17 {
		tooMany[string(rune('a'+index))] = index
	}
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "too many attributes", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{ID: "flow", Type: "order", Payload: "payload", Attributes: tooMany})
			return err
		}},
		{name: "reserved attribute", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{ID: "flow", Type: "order", Payload: "payload", Attributes: map[string]any{"__internal": "value"}})
			return err
		}},
		{name: "normalized attribute collision", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{
				ID: "flow", Type: "order", Payload: "payload",
				Attributes: map[string]any{"name": "one", " name ": "two"},
			})
			return err
		}},
		{name: "oversized attribute", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{ID: "flow", Type: "order", Payload: "payload", Attributes: map[string]any{"name": string(make([]byte, 257))}})
			return err
		}},
		{name: "non-finite attribute", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{ID: "flow", Type: "order", Payload: "payload", Attributes: map[string]any{"score": math.NaN()}})
			return err
		}},
		{name: "invalid attribute list", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{ID: "flow", Type: "order", Payload: "payload", Attributes: map[string]any{"tags": []string{}}})
			return err
		}},
		{name: "invalid attribute delete", call: func(client *Client) error {
			_, err := client.Transition(context.Background(), TransitionOptions{ID: "flow", FromState: "queued", ToState: "ready", Payload: "payload", NamedValues: NamedValues{AttributesDelete: []string{""}}})
			return err
		}},
		{name: "invalid state meta key", call: func(client *Client) error {
			_, err := client.Complete(context.Background(), CompleteOptions{ID: "flow", LeaseToken: "lease", Result: "payload", StateMeta: map[string]any{"__internal": "value"}})
			return err
		}},
		{name: "invalid state meta value", call: func(client *Client) error {
			_, err := client.Fail(context.Background(), FailOptions{ID: "flow", LeaseToken: "lease", Error: "payload", StateMeta: map[string]any{"attempt": []string{"one"}}})
			return err
		}},
		{name: "invalid create many item metadata", call: func(client *Client) error {
			_, err := client.CreateMany(context.Background(), CreateManyOptions{Type: "order", Items: []CreateItem{{ID: "flow", Payload: "payload", Attributes: map[string]any{"bad": map[string]any{"nested": true}}}}})
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			codec := &countingKVCodec{}
			if err := test.call(NewClientWithExecutor(exec, WithCodec(codec))); err == nil {
				t.Fatal("invalid metadata was accepted")
			}
			if calls := codec.encodes.Load(); calls != 0 {
				t.Fatalf("invalid metadata invoked codec %d times", calls)
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid metadata reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestFlowMetadataKeysAreCanonicalizedOnTheWire(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	_, err := NewClientWithExecutor(exec).Transition(context.Background(), TransitionOptions{
		ID: "flow", FromState: "queued", ToState: "ready",
		NamedValues: NamedValues{
			AttributesMerge:  map[string]any{" plan ": "pro"},
			AttributesDelete: []string{" old_plan "},
		},
		StateMeta: map[string]any{" attempt ": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []any{
		"FLOW.TRANSITION", "flow", "queued", "ready", "FENCING", int64(0), "NOW", exec.calls[0][7],
		"RUN_AT", exec.calls[0][9], "ATTRIBUTE_MERGE", "plan", "pro", "ATTRIBUTE_DELETE", "old_plan",
		"STATE_META", "attempt", 1,
	}
	if !reflect.DeepEqual(exec.calls[0], want) {
		t.Fatalf("wire command = %#v; want %#v", exec.calls[0], want)
	}
}
