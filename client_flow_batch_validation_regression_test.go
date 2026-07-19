package ferricstore

import (
	"context"
	"fmt"
	"testing"
)

func TestFlowManyRejectsItemsOutsideBatchPartition(t *testing.T) {
	claimed := []ClaimedItem{{
		ID: "flow-1", PartitionKey: "tenant:b", LeaseToken: "lease-1", FencingToken: 1,
	}}
	fenced := []FencedItem{{
		ID: "flow-1", PartitionKey: "tenant:b", LeaseToken: "lease-1", FencingToken: 1,
	}}
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "create", call: func(client *Client) error {
			_, err := client.CreateMany(context.Background(), CreateManyOptions{
				PartitionKey: "tenant:a", Type: "order",
				Items: []CreateItem{{ID: "flow-1", PartitionKey: "tenant:b", Payload: "value"}},
			})
			return err
		}},
		{name: "complete", call: func(client *Client) error {
			_, err := client.CompleteMany(context.Background(), CompleteManyOptions{PartitionKey: "tenant:a", Items: claimed})
			return err
		}},
		{name: "transition", call: func(client *Client) error {
			_, err := client.TransitionMany(context.Background(), TransitionManyOptions{
				PartitionKey: "tenant:a", FromState: "queued", ToState: "running", Items: fenced,
			})
			return err
		}},
		{name: "retry", call: func(client *Client) error {
			_, err := client.RetryMany(context.Background(), RetryManyOptions{PartitionKey: "tenant:a", Items: claimed})
			return err
		}},
		{name: "fail", call: func(client *Client) error {
			_, err := client.FailMany(context.Background(), FailManyOptions{PartitionKey: "tenant:a", Items: claimed})
			return err
		}},
		{name: "cancel", call: func(client *Client) error {
			_, err := client.CancelMany(context.Background(), CancelManyOptions{PartitionKey: "tenant:a", Items: fenced})
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			if err := test.call(NewClientWithExecutor(exec)); err == nil {
				t.Fatal("mismatched item partition was accepted")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("mismatched batch reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestRunStepsManyRejectsInvalidOptionsBeforeCodecOrTransport(t *testing.T) {
	zero := int64(0)
	oversized := make([]RunStepsItem, maxFlowMutationBatchItemsV080+1)
	for index := range oversized {
		oversized[index].ID = fmt.Sprintf("flow-%d", index)
	}
	tests := []struct {
		name string
		opt  RunStepsManyOptions
	}{
		{
			name: "missing type",
			opt: RunStepsManyOptions{
				States: []string{"ready"}, Worker: "worker", Items: []RunStepsItem{{ID: "flow-1"}}, Payload: "value",
			},
		},
		{
			name: "missing worker",
			opt: RunStepsManyOptions{
				Type: "order", States: []string{"ready"}, Items: []RunStepsItem{{ID: "flow-1"}}, Payload: "value",
			},
		},
		{
			name: "empty state",
			opt: RunStepsManyOptions{
				Type: "order", States: []string{""}, Worker: "worker", Items: []RunStepsItem{{ID: "flow-1"}}, Payload: "value",
			},
		},
		{
			name: "empty id",
			opt: RunStepsManyOptions{
				Type: "order", States: []string{"ready"}, Worker: "worker", Items: []RunStepsItem{{}}, Payload: "value",
			},
		},
		{
			name: "negative lease",
			opt: RunStepsManyOptions{
				Type: "order", States: []string{"ready"}, Worker: "worker", LeaseMS: -1,
				Items: []RunStepsItem{{ID: "flow-1"}}, Payload: "value",
			},
		},
		{
			name: "negative now",
			opt: RunStepsManyOptions{
				Type: "order", States: []string{"ready"}, Worker: "worker", NowMS: -1,
				Items: []RunStepsItem{{ID: "flow-1"}}, Payload: "value",
			},
		},
		{
			name: "non-positive retention",
			opt: RunStepsManyOptions{
				Type: "order", States: []string{"ready"}, Worker: "worker", RetentionTTLMS: &zero,
				Items: []RunStepsItem{{ID: "flow-1"}}, Payload: "value",
			},
		},
		{
			name: "duplicate id",
			opt: RunStepsManyOptions{
				Type: "order", States: []string{"ready"}, Worker: "worker",
				Items: []RunStepsItem{{ID: "flow-1"}, {ID: "flow-1"}}, Payload: "value",
			},
		},
		{
			name: "batch above server maximum",
			opt: RunStepsManyOptions{
				Type: "order", States: []string{"ready"}, Worker: "worker", Items: oversized, Payload: "value",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			codec := &countingKVCodec{}
			client := NewClientWithExecutor(exec, WithCodec(codec))
			if err := client.RunStepsMany(context.Background(), test.opt); err == nil {
				t.Fatal("invalid run_steps_many options were accepted")
			}
			if calls := codec.encodes.Load(); calls != 0 {
				t.Fatalf("invalid options invoked codec %d times", calls)
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid options reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestFlowManyMutationsRejectPartialAcknowledgementLists(t *testing.T) {
	claimed := []ClaimedItem{
		{ID: "one", LeaseToken: "lease-one", FencingToken: 1},
		{ID: "two", LeaseToken: "lease-two", FencingToken: 2},
	}
	fenced := []FencedItem{
		{ID: "one", LeaseToken: "lease-one", FencingToken: 1},
		{ID: "two", LeaseToken: "lease-two", FencingToken: 2},
	}
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "create", call: func(client *Client) error {
			_, err := client.CreateMany(context.Background(), CreateManyOptions{
				Type: "order", Items: []CreateItem{{ID: "one"}, {ID: "two"}},
			})
			return err
		}},
		{name: "complete", call: func(client *Client) error {
			_, err := client.CompleteMany(context.Background(), CompleteManyOptions{PartitionKey: "tenant", Items: claimed})
			return err
		}},
		{name: "transition", call: func(client *Client) error {
			_, err := client.TransitionMany(context.Background(), TransitionManyOptions{
				PartitionKey: "tenant", FromState: "queued", ToState: "ready", Items: fenced,
			})
			return err
		}},
		{name: "retry", call: func(client *Client) error {
			_, err := client.RetryMany(context.Background(), RetryManyOptions{PartitionKey: "tenant", Items: claimed})
			return err
		}},
		{name: "fail", call: func(client *Client) error {
			_, err := client.FailMany(context.Background(), FailManyOptions{PartitionKey: "tenant", Items: claimed})
			return err
		}},
		{name: "cancel", call: func(client *Client) error {
			_, err := client.CancelMany(context.Background(), CancelManyOptions{PartitionKey: "tenant", Items: fenced})
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := NewClientWithExecutor(&fakeExecutor{value: []any{[]byte("OK")}})
			if err := test.call(client); err == nil {
				t.Fatal("partial one-item response was accepted for a two-item mutation")
			}
		})
	}
}
