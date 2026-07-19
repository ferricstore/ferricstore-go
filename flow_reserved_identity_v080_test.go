package ferricstore

import (
	"context"
	"testing"
)

const (
	v080ReservedScheduleID   = "__ferricstore_schedule__:user-controlled"
	v080ReservedScheduleType = "__ferricstore_schedule"
)

func TestV080FlowMutationsRejectReservedScheduleIdentitiesBeforeWork(t *testing.T) {
	ctx := context.Background()
	claimed := []ClaimedItem{{ID: v080ReservedScheduleID, LeaseToken: "lease", FencingToken: 1}}
	fenced := []FencedItem{{ID: v080ReservedScheduleID, LeaseToken: "lease", FencingToken: 1}}
	tests := []struct {
		name string
		run  func(*Client) error
	}{
		{name: "create id", run: func(c *Client) error {
			_, err := c.Create(ctx, CreateOptions{ID: v080ReservedScheduleID, Type: "order"})
			return err
		}},
		{name: "create type", run: func(c *Client) error {
			_, err := c.Create(ctx, CreateOptions{ID: "flow-1", Type: v080ReservedScheduleType})
			return err
		}},
		{name: "create many id", run: func(c *Client) error {
			_, err := c.CreateMany(ctx, CreateManyOptions{Type: "order", Items: []CreateItem{{ID: v080ReservedScheduleID}}})
			return err
		}},
		{name: "create many type", run: func(c *Client) error {
			_, err := c.CreateMany(ctx, CreateManyOptions{Type: v080ReservedScheduleType, Items: []CreateItem{{ID: "flow-1"}}})
			return err
		}},
		{name: "start and claim id", run: func(c *Client) error {
			_, err := c.StartAndClaim(ctx, StartAndClaimOptions{
				ID: v080ReservedScheduleID, Type: "order", InitialState: "queued", Worker: "worker",
			})
			return err
		}},
		{name: "start and claim type", run: func(c *Client) error {
			_, err := c.StartAndClaim(ctx, StartAndClaimOptions{
				ID: "flow-1", Type: v080ReservedScheduleType, InitialState: "queued", Worker: "worker",
			})
			return err
		}},
		{name: "claim due type", run: func(c *Client) error {
			_, err := c.ClaimDue(ctx, ClaimDueOptions{Type: v080ReservedScheduleType, Worker: "worker"})
			return err
		}},
		{name: "reclaim type", run: func(c *Client) error {
			_, err := c.Reclaim(ctx, ReclaimOptions{Type: v080ReservedScheduleType, Worker: "worker"})
			return err
		}},
		{name: "extend lease", run: func(c *Client) error {
			_, err := c.ExtendLease(ctx, v080ReservedScheduleID, "lease", 1, 1, "")
			return err
		}},
		{name: "transition", run: func(c *Client) error {
			_, err := c.Transition(ctx, TransitionOptions{
				ID: v080ReservedScheduleID, FromState: "queued", ToState: "done", LeaseToken: "lease", FencingToken: 1,
			})
			return err
		}},
		{name: "step continue", run: func(c *Client) error {
			_, err := c.StepContinue(ctx, StepContinueOptions{
				ID: v080ReservedScheduleID, FromState: "queued", ToState: "done", LeaseToken: "lease", FencingToken: 1,
			})
			return err
		}},
		{name: "complete", run: func(c *Client) error {
			_, err := c.Complete(ctx, CompleteOptions{ID: v080ReservedScheduleID, LeaseToken: "lease", FencingToken: 1})
			return err
		}},
		{name: "retry", run: func(c *Client) error {
			_, err := c.Retry(ctx, RetryOptions{ID: v080ReservedScheduleID, LeaseToken: "lease", FencingToken: 1})
			return err
		}},
		{name: "fail", run: func(c *Client) error {
			_, err := c.Fail(ctx, FailOptions{ID: v080ReservedScheduleID, LeaseToken: "lease", FencingToken: 1})
			return err
		}},
		{name: "cancel", run: func(c *Client) error {
			_, err := c.Cancel(ctx, CancelOptions{ID: v080ReservedScheduleID, FencingToken: 1})
			return err
		}},
		{name: "rewind", run: func(c *Client) error {
			_, err := c.Rewind(ctx, RewindOptions{ID: v080ReservedScheduleID, ToEvent: "1-1"})
			return err
		}},
		{name: "complete many", run: func(c *Client) error {
			_, err := c.CompleteMany(ctx, CompleteManyOptions{Items: claimed})
			return err
		}},
		{name: "transition many", run: func(c *Client) error {
			_, err := c.TransitionMany(ctx, TransitionManyOptions{FromState: "queued", ToState: "done", Items: fenced})
			return err
		}},
		{name: "retry many", run: func(c *Client) error {
			_, err := c.RetryMany(ctx, RetryManyOptions{Items: claimed})
			return err
		}},
		{name: "fail many", run: func(c *Client) error {
			_, err := c.FailMany(ctx, FailManyOptions{Items: claimed})
			return err
		}},
		{name: "cancel many", run: func(c *Client) error {
			_, err := c.CancelMany(ctx, CancelManyOptions{Items: fenced})
			return err
		}},
		{name: "spawn parent id", run: func(c *Client) error {
			_, err := c.SpawnChildren(ctx, SpawnChildrenOptions{
				ID: v080ReservedScheduleID, PartitionKey: "p", Success: "done", Failure: "failed",
				FencingToken: Int64(1), Children: []ChildSpec{{ID: "child-1", Type: "order"}},
			})
			return err
		}},
		{name: "spawn child id", run: func(c *Client) error {
			_, err := c.SpawnChildren(ctx, SpawnChildrenOptions{
				ID: "parent-1", PartitionKey: "p", Success: "done", Failure: "failed",
				FencingToken: Int64(1), Children: []ChildSpec{{ID: v080ReservedScheduleID, Type: "order"}},
			})
			return err
		}},
		{name: "spawn child type", run: func(c *Client) error {
			_, err := c.SpawnChildren(ctx, SpawnChildrenOptions{
				ID: "parent-1", PartitionKey: "p", Success: "done", Failure: "failed",
				FencingToken: Int64(1), Children: []ChildSpec{{ID: "child-1", Type: v080ReservedScheduleType}},
			})
			return err
		}},
		{name: "run steps id", run: func(c *Client) error {
			return c.RunStepsMany(ctx, RunStepsManyOptions{
				Items: []RunStepsItem{{ID: v080ReservedScheduleID}}, Type: "order", Steps: 1, Worker: "worker",
			})
		}},
		{name: "run steps type", run: func(c *Client) error {
			return c.RunStepsMany(ctx, RunStepsManyOptions{
				Items: []RunStepsItem{{ID: "flow-1"}}, Type: v080ReservedScheduleType, Steps: 1, Worker: "worker",
			})
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: map[string]any{}}
			codec := &countingKVCodec{}
			err := test.run(NewClientWithExecutor(exec, WithCodec(codec)))
			if err == nil {
				t.Fatal("reserved Flow identity was accepted")
			}
			if codec.encodes.Load() != 0 || len(exec.calls) != 0 {
				t.Fatalf("reserved Flow identity performed work: encodes=%d calls=%#v", codec.encodes.Load(), exec.calls)
			}
		})
	}
}

func TestV080FlowReadsAndReferencesRejectReservedScheduleIdentitiesBeforeWork(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		run  func(*Client) error
	}{
		{name: "get id", run: func(c *Client) error {
			_, err := c.Get(ctx, v080ReservedScheduleID, "", nil)
			return err
		}},
		{name: "history id", run: func(c *Client) error {
			_, err := c.History(ctx, HistoryOptions{ID: v080ReservedScheduleID})
			return err
		}},
		{name: "signal id", run: func(c *Client) error {
			_, err := c.Signal(ctx, SignalOptions{ID: v080ReservedScheduleID, Signal: "ready"})
			return err
		}},
		{name: "list type", run: func(c *Client) error {
			_, err := c.List(ctx, v080ReservedScheduleType, ReadOptions{})
			return err
		}},
		{name: "search type", run: func(c *Client) error {
			_, err := c.Search(ctx, SearchOptions{Type: v080ReservedScheduleType})
			return err
		}},
		{name: "terminals type", run: func(c *Client) error {
			_, err := c.Terminals(ctx, v080ReservedScheduleType, ReadOptions{})
			return err
		}},
		{name: "failures type", run: func(c *Client) error {
			_, err := c.Failures(ctx, v080ReservedScheduleType, ReadOptions{})
			return err
		}},
		{name: "parent id", run: func(c *Client) error {
			_, err := c.ByParent(ctx, v080ReservedScheduleID, ReadOptions{})
			return err
		}},
		{name: "root id", run: func(c *Client) error {
			_, err := c.ByRoot(ctx, v080ReservedScheduleID, ReadOptions{})
			return err
		}},
		{name: "info type", run: func(c *Client) error {
			_, err := c.Info(ctx, v080ReservedScheduleType, "", nil, nil)
			return err
		}},
		{name: "stuck type", run: func(c *Client) error {
			_, err := c.Stuck(ctx, v080ReservedScheduleType, "", nil, nil, nil)
			return err
		}},
		{name: "stats type", run: func(c *Client) error {
			_, err := c.Stats(ctx, v080ReservedScheduleType, ReadOptions{})
			return err
		}},
		{name: "attributes type", run: func(c *Client) error {
			_, err := c.Attributes(ctx, v080ReservedScheduleType, ReadOptions{})
			return err
		}},
		{name: "attribute values type", run: func(c *Client) error {
			_, err := c.AttributeValues(ctx, v080ReservedScheduleType, "tenant", ReadOptions{})
			return err
		}},
		{name: "policy set type", run: func(c *Client) error {
			_, err := c.SetPolicy(ctx, v080ReservedScheduleType, PolicyOptions{})
			return err
		}},
		{name: "policy get type", run: func(c *Client) error {
			_, err := c.PolicyGet(ctx, v080ReservedScheduleType, "")
			return err
		}},
		{name: "value owner id", run: func(c *Client) error {
			_, err := c.ValuePut(ctx, "value", ValuePutOptions{
				OwnerFlowID: v080ReservedScheduleID,
				Name:        "result",
			})
			return err
		}},
		{name: "create parent id", run: func(c *Client) error {
			_, err := c.Create(ctx, CreateOptions{
				ID: "flow-1", Type: "order", ParentFlowID: v080ReservedScheduleID,
			})
			return err
		}},
		{name: "create root id", run: func(c *Client) error {
			_, err := c.Create(ctx, CreateOptions{
				ID: "flow-1", Type: "order", RootFlowID: v080ReservedScheduleID,
			})
			return err
		}},
		{name: "start and claim parent id", run: func(c *Client) error {
			_, err := c.StartAndClaim(ctx, StartAndClaimOptions{
				ID: "flow-1", Type: "order", InitialState: "queued", Worker: "worker",
				ParentFlowID: v080ReservedScheduleID,
			})
			return err
		}},
		{name: "start and claim root id", run: func(c *Client) error {
			_, err := c.StartAndClaim(ctx, StartAndClaimOptions{
				ID: "flow-1", Type: "order", InitialState: "queued", Worker: "worker",
				RootFlowID: v080ReservedScheduleID,
			})
			return err
		}},
		{name: "effect reserve id", run: func(c *Client) error {
			_, err := c.EffectReserve(ctx, v080ReservedScheduleID, "effect", "email", validEffectReserveOptions())
			return err
		}},
		{name: "effect status id", run: func(c *Client) error {
			_, err := c.EffectConfirm(ctx, v080ReservedScheduleID, "effect", validEffectStatusOptions())
			return err
		}},
		{name: "effect get id", run: func(c *Client) error {
			_, err := c.EffectGet(ctx, v080ReservedScheduleID, "effect", "")
			return err
		}},
		{name: "governance ledger id", run: func(c *Client) error {
			_, err := c.GovernanceLedger(ctx, v080ReservedScheduleID, GovernanceLedgerOptions{})
			return err
		}},
		{name: "approval request flow id", run: func(c *Client) error {
			_, err := c.ApprovalRequest(ctx, "approval", ApprovalRequestOptions{
				FlowID: v080ReservedScheduleID,
				Scope:  "tenant",
			})
			return err
		}},
		{name: "approval list flow id", run: func(c *Client) error {
			_, err := c.ApprovalList(ctx, ApprovalListOptions{FlowID: v080ReservedScheduleID})
			return err
		}},
		{name: "schedule target parent id", run: func(c *Client) error {
			_, err := c.ScheduleCreate(ctx, "schedule", ScheduleOptions{Target: map[string]any{
				"type": "order", "parent_flow_id": v080ReservedScheduleID,
			}})
			return err
		}},
		{name: "schedule target root id", run: func(c *Client) error {
			_, err := c.ScheduleCreate(ctx, "schedule", ScheduleOptions{Target: map[string]any{
				"type": "order", "root_flow_id": v080ReservedScheduleID,
			}})
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: map[string]any{}}
			codec := &countingKVCodec{}
			err := test.run(NewClientWithExecutor(exec, WithCodec(codec)))
			if err == nil {
				t.Fatal("reserved Flow identity was accepted")
			}
			if codec.encodes.Load() != 0 || len(exec.calls) != 0 {
				t.Fatalf("reserved Flow identity performed work: encodes=%d calls=%#v", codec.encodes.Load(), exec.calls)
			}
		})
	}
}
