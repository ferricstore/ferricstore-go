package ferricstore

import (
	"context"
	"strings"
	"testing"
)

const testMaxFlowExactIntegerV080 int64 = 9_007_199_254_740_991

func TestV080FlowReferenceLimitsRejectBeforeCodecOrTransport(t *testing.T) {
	tooLarge := strings.Repeat("r", 4_097)
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "create parent_flow_id", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{ID: "flow", Type: "type", ParentFlowID: tooLarge})
			return err
		}},
		{name: "create root_flow_id", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{ID: "flow", Type: "type", RootFlowID: tooLarge})
			return err
		}},
		{name: "create correlation_id", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{ID: "flow", Type: "type", CorrelationID: tooLarge})
			return err
		}},
		{name: "start parent_flow_id", call: func(client *Client) error {
			_, err := client.StartAndClaim(context.Background(), StartAndClaimOptions{
				ID: "flow", Type: "type", InitialState: "queued", Worker: "worker", ParentFlowID: tooLarge,
			})
			return err
		}},
		{name: "start root_flow_id", call: func(client *Client) error {
			_, err := client.StartAndClaim(context.Background(), StartAndClaimOptions{
				ID: "flow", Type: "type", InitialState: "queued", Worker: "worker", RootFlowID: tooLarge,
			})
			return err
		}},
		{name: "start correlation_id", call: func(client *Client) error {
			_, err := client.StartAndClaim(context.Background(), StartAndClaimOptions{
				ID: "flow", Type: "type", InitialState: "queued", Worker: "worker", CorrelationID: tooLarge,
			})
			return err
		}},
		{name: "signal idempotency_key", call: func(client *Client) error {
			_, err := client.Signal(context.Background(), SignalOptions{ID: "flow", Signal: "wake", IdempotencyKey: tooLarge})
			return err
		}},
		{name: "value put owner_flow_id", call: func(client *Client) error {
			_, err := client.ValuePut(context.Background(), "value", ValuePutOptions{OwnerFlowID: tooLarge})
			return err
		}},
		{name: "value put name", call: func(client *Client) error {
			_, err := client.ValuePut(context.Background(), "value", ValuePutOptions{Name: tooLarge})
			return err
		}},
	}
	assertFlowCallsRejectedBeforeCodecOrTransport(t, tests)
}

func TestV080FlowMutationExactIntegerLimitsRejectBeforeCodecOrTransport(t *testing.T) {
	tooLarge := testMaxFlowExactIntegerV080 + 1
	one := int64(1)
	claimed := []ClaimedItem{{ID: "flow", LeaseToken: "lease", FencingToken: 1, PartitionKey: "p"}}
	fenced := []FencedItem{{ID: "flow", LeaseToken: "lease", FencingToken: 1, PartitionKey: "p"}}
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "create now_ms", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{ID: "flow", Type: "type", NowMS: tooLarge})
			return err
		}},
		{name: "create run_at_ms", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{ID: "flow", Type: "type", RunAtMS: tooLarge})
			return err
		}},
		{name: "create many now_ms", call: func(client *Client) error {
			_, err := client.CreateMany(context.Background(), CreateManyOptions{
				Type: "type", NowMS: tooLarge, Items: []CreateItem{{ID: "flow"}},
			})
			return err
		}},
		{name: "signal now_ms", call: func(client *Client) error {
			_, err := client.Signal(context.Background(), SignalOptions{ID: "flow", Signal: "wake", NowMS: tooLarge})
			return err
		}},
		{name: "signal run_at_ms", call: func(client *Client) error {
			_, err := client.Signal(context.Background(), SignalOptions{ID: "flow", Signal: "wake", RunAtMS: tooLarge})
			return err
		}},
		{name: "start lease_ms", call: func(client *Client) error {
			_, err := client.StartAndClaim(context.Background(), StartAndClaimOptions{
				ID: "flow", Type: "type", InitialState: "queued", Worker: "worker", LeaseMS: tooLarge,
			})
			return err
		}},
		{name: "start lease deadline", call: func(client *Client) error {
			_, err := client.StartAndClaim(context.Background(), StartAndClaimOptions{
				ID: "flow", Type: "type", InitialState: "queued", Worker: "worker",
				LeaseMS: one, NowMS: testMaxFlowExactIntegerV080,
			})
			return err
		}},
		{name: "claim now_ms", call: func(client *Client) error {
			_, err := client.ClaimDue(context.Background(), ClaimDueOptions{Type: "type", Worker: "worker", NowMS: tooLarge})
			return err
		}},
		{name: "claim lease deadline", call: func(client *Client) error {
			_, err := client.ClaimDue(context.Background(), ClaimDueOptions{
				Type: "type", Worker: "worker", LeaseMS: one, NowMS: testMaxFlowExactIntegerV080,
			})
			return err
		}},
		{name: "reclaim lease_ms", call: func(client *Client) error {
			_, err := client.Reclaim(context.Background(), ReclaimOptions{Type: "type", Worker: "worker", LeaseMS: tooLarge})
			return err
		}},
		{name: "extend fencing_token", call: func(client *Client) error {
			_, err := client.ExtendLease(context.Background(), "flow", "lease", tooLarge, 1, "")
			return err
		}},
		{name: "extend lease_ms", call: func(client *Client) error {
			_, err := client.ExtendLease(context.Background(), "flow", "lease", 1, tooLarge, "")
			return err
		}},
		{name: "transition run_at_ms", call: func(client *Client) error {
			_, err := client.Transition(context.Background(), TransitionOptions{
				ID: "flow", FromState: "a", ToState: "b", LeaseToken: "lease", FencingToken: 1, RunAtMS: tooLarge,
			})
			return err
		}},
		{name: "step lease deadline", call: func(client *Client) error {
			_, err := client.StepContinue(context.Background(), StepContinueOptions{
				ID: "flow", LeaseToken: "lease", FromState: "a", ToState: "b", FencingToken: 1,
				LeaseMS: one, NowMS: testMaxFlowExactIntegerV080,
			})
			return err
		}},
		{name: "complete ttl deadline", call: func(client *Client) error {
			_, err := client.Complete(context.Background(), CompleteOptions{
				ID: "flow", LeaseToken: "lease", FencingToken: 1, TTLMS: &one, NowMS: testMaxFlowExactIntegerV080,
			})
			return err
		}},
		{name: "retry fencing_token", call: func(client *Client) error {
			_, err := client.Retry(context.Background(), RetryOptions{ID: "flow", LeaseToken: "lease", FencingToken: tooLarge})
			return err
		}},
		{name: "fail ttl_ms", call: func(client *Client) error {
			_, err := client.Fail(context.Background(), FailOptions{ID: "flow", LeaseToken: "lease", FencingToken: 1, TTLMS: &tooLarge})
			return err
		}},
		{name: "cancel now_ms", call: func(client *Client) error {
			_, err := client.Cancel(context.Background(), CancelOptions{ID: "flow", FencingToken: 1, NowMS: tooLarge})
			return err
		}},
		{name: "rewind run_at_ms", call: func(client *Client) error {
			_, err := client.Rewind(context.Background(), RewindOptions{ID: "flow", ToEvent: "1-0", RunAtMS: tooLarge})
			return err
		}},
		{name: "complete many now_ms", call: func(client *Client) error {
			_, err := client.CompleteMany(context.Background(), CompleteManyOptions{Items: claimed, NowMS: tooLarge})
			return err
		}},
		{name: "transition many item fencing_token", call: func(client *Client) error {
			items := append([]FencedItem(nil), fenced...)
			items[0].FencingToken = tooLarge
			_, err := client.TransitionMany(context.Background(), TransitionManyOptions{FromState: "a", ToState: "b", Items: items})
			return err
		}},
		{name: "spawn fencing_token", call: func(client *Client) error {
			_, err := client.SpawnChildren(context.Background(), SpawnChildrenOptions{
				ID: "parent", PartitionKey: "p", FencingToken: &tooLarge, Success: "done", Failure: "failed",
				Children: []ChildSpec{{ID: "child", Type: "type"}},
			})
			return err
		}},
		{name: "value put expiry deadline", call: func(client *Client) error {
			_, err := client.ValuePut(context.Background(), "value", ValuePutOptions{
				NowMS: testMaxFlowExactIntegerV080, TTLMS: &one,
			})
			return err
		}},
	}
	assertFlowCallsRejectedBeforeCodecOrTransport(t, tests)
}

func TestV080FlowReadExactIntegerAndHistoryCursorLimitsRejectBeforeTransport(t *testing.T) {
	tooLarge := testMaxFlowExactIntegerV080 + 1
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "list from_ms", call: func(client *Client) error {
			_, err := client.List(context.Background(), "type", ReadOptions{FromMS: &tooLarge})
			return err
		}},
		{name: "search to_ms", call: func(client *Client) error {
			_, err := client.Search(context.Background(), SearchOptions{ToMS: &tooLarge})
			return err
		}},
		{name: "stuck older_than_ms", call: func(client *Client) error {
			_, err := client.Stuck(context.Background(), "type", "", nil, &tooLarge, nil)
			return err
		}},
		{name: "history from_version", call: func(client *Client) error {
			_, err := client.History(context.Background(), HistoryOptions{ID: "flow", FromVersion: &tooLarge})
			return err
		}},
		{name: "history malformed from_event", call: func(client *Client) error {
			_, err := client.History(context.Background(), HistoryOptions{ID: "flow", FromEvent: "01-0"})
			return err
		}},
		{name: "history event component exceeds exact integer", call: func(client *Client) error {
			_, err := client.History(context.Background(), HistoryOptions{ID: "flow", FromEvent: "9007199254740992-0"})
			return err
		}},
		{name: "history reversed event range", call: func(client *Client) error {
			_, err := client.History(context.Background(), HistoryOptions{ID: "flow", FromEvent: "2-0", ToEvent: "1-9"})
			return err
		}},
	}
	assertFlowCallsRejectedBeforeCodecOrTransport(t, tests)
}

func TestV080RunStepsManyRejectsExcessiveExpandedWorkBeforeCodecOrTransport(t *testing.T) {
	items := make([]RunStepsItem, 1_001)
	for index := range items {
		items[index] = RunStepsItem{ID: string(rune(index + 1))}
	}
	states := make([]string, 100)
	for index := range states {
		states[index] = "state"
	}
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "generated states", call: func(client *Client) error {
			return client.RunStepsMany(context.Background(), RunStepsManyOptions{
				Items: items, Type: "type", Steps: 100, Worker: "worker",
			})
		}},
		{name: "explicit states", call: func(client *Client) error {
			return client.RunStepsMany(context.Background(), RunStepsManyOptions{
				Items: items, Type: "type", States: states, Worker: "worker",
			})
		}},
	}
	assertFlowCallsRejectedBeforeCodecOrTransport(t, tests)
}

func assertFlowCallsRejectedBeforeCodecOrTransport(
	t *testing.T,
	tests []struct {
		name string
		call func(*Client) error
	},
) {
	t.Helper()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []any{}}
			codec := &countingKVCodec{}
			err := test.call(NewClientWithExecutor(exec, WithConcurrentCodec(codec)))
			if err == nil {
				t.Fatal("request outside the FerricStore 0.8 Flow contract succeeded")
			}
			if calls := codec.encodes.Load(); calls != 0 {
				t.Fatalf("invalid Flow request invoked codec %d times", calls)
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid Flow request reached transport: %#v", exec.calls)
			}
		})
	}
}
