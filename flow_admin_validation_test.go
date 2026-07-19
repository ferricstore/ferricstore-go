package ferricstore

import (
	"context"
	"math"
	"testing"
)

func TestScheduleCommandsRejectInvalidArgumentsBeforeTransport(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "create empty id", call: func(c *Client) error {
			_, err := c.ScheduleCreate(ctx, "", ScheduleOptions{Target: map[string]any{"type": "email"}})
			return err
		}},
		{name: "create missing target", call: func(c *Client) error {
			_, err := c.ScheduleCreate(ctx, "schedule", ScheduleOptions{})
			return err
		}},
		{name: "create target missing type", call: func(c *Client) error {
			_, err := c.ScheduleCreate(ctx, "schedule", ScheduleOptions{Target: map[string]any{"state": "queued"}})
			return err
		}},
		{name: "create invalid kind", call: func(c *Client) error {
			_, err := c.ScheduleCreate(ctx, "schedule", ScheduleOptions{Kind: "sometimes", Target: map[string]any{"type": "email"}})
			return err
		}},
		{name: "create delay missing delay", call: func(c *Client) error {
			_, err := c.ScheduleCreate(ctx, "schedule", ScheduleOptions{Kind: "delay", Target: map[string]any{"type": "email"}})
			return err
		}},
		{name: "create interval non-positive period", call: func(c *Client) error {
			_, err := c.ScheduleCreate(ctx, "schedule", ScheduleOptions{Kind: "interval", EveryMS: Int64(0), Target: map[string]any{"type": "email"}})
			return err
		}},
		{name: "create cron missing expression", call: func(c *Client) error {
			_, err := c.ScheduleCreate(ctx, "schedule", ScheduleOptions{Kind: "cron", Target: map[string]any{"type": "email"}})
			return err
		}},
		{name: "create timezone on non-cron", call: func(c *Client) error {
			_, err := c.ScheduleCreate(ctx, "schedule", ScheduleOptions{Timezone: "UTC", Target: map[string]any{"type": "email"}})
			return err
		}},
		{name: "create recurring fixed target id", call: func(c *Client) error {
			_, err := c.ScheduleCreate(ctx, "schedule", ScheduleOptions{EveryMS: Int64(1), Target: map[string]any{"type": "email", "id": "fixed"}})
			return err
		}},
		{name: "create invalid overlap policy", call: func(c *Client) error {
			_, err := c.ScheduleCreate(ctx, "schedule", ScheduleOptions{EveryMS: Int64(1), OverlapPolicy: "race", Target: map[string]any{"type": "email"}})
			return err
		}},
		{name: "create one-shot overlap policy", call: func(c *Client) error {
			_, err := c.ScheduleCreate(ctx, "schedule", ScheduleOptions{OverlapPolicy: "skip", Target: map[string]any{"type": "email"}})
			return err
		}},
		{name: "create negative timestamp", call: func(c *Client) error {
			_, err := c.ScheduleCreate(ctx, "schedule", ScheduleOptions{AtMS: Int64(-1), Target: map[string]any{"type": "email"}})
			return err
		}},
		{name: "create invalid target priority", call: func(c *Client) error {
			_, err := c.ScheduleCreate(ctx, "schedule", ScheduleOptions{Target: map[string]any{"type": "email", "priority": int64(3)}})
			return err
		}},
		{name: "create end before first run", call: func(c *Client) error {
			_, err := c.ScheduleCreate(ctx, "schedule", ScheduleOptions{EveryMS: Int64(10), StartAtMS: Int64(20), EndAtMS: Int64(19), Target: map[string]any{"type": "email"}})
			return err
		}},
		{name: "create delay timestamp overflow", call: func(c *Client) error {
			_, err := c.ScheduleCreate(ctx, "schedule", ScheduleOptions{DelayMS: Int64(1), NowMS: Int64(math.MaxInt64), Target: map[string]any{"type": "email"}})
			return err
		}},
		{name: "get empty id", call: func(c *Client) error {
			_, err := c.ScheduleGet(ctx, "", nil)
			return err
		}},
		{name: "status negative now", call: func(c *Client) error {
			_, err := c.SchedulePause(ctx, "schedule", Int64(-1))
			return err
		}},
		{name: "fire due negative block", call: func(c *Client) error {
			_, err := c.ScheduleFireDue(ctx, nil, "worker", Int64(-1), Int(1))
			return err
		}},
		{name: "fire due non-positive limit", call: func(c *Client) error {
			_, err := c.ScheduleFireDue(ctx, nil, "worker", nil, Int(0))
			return err
		}},
		{name: "list invalid kind", call: func(c *Client) error {
			_, err := c.ScheduleList(ctx, ScheduleListOptions{Kind: "sometimes"})
			return err
		}},
		{name: "list negative range", call: func(c *Client) error {
			_, err := c.ScheduleList(ctx, ScheduleListOptions{FromMS: Int64(-1)})
			return err
		}},
	}
	assertAdminCallsRejectedBeforeTransport(t, tests)
}

func TestGovernanceCommandsRejectInvalidArgumentsBeforeTransport(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "effect reserve empty id", call: func(c *Client) error {
			_, err := c.EffectReserve(ctx, "", "key", "email", validEffectReserveOptions())
			return err
		}},
		{name: "effect reserve missing lease", call: func(c *Client) error {
			opt := validEffectReserveOptions()
			opt.LeaseToken = ""
			_, err := c.EffectReserve(ctx, "flow", "key", "email", opt)
			return err
		}},
		{name: "effect reserve missing fencing", call: func(c *Client) error {
			opt := validEffectReserveOptions()
			opt.FencingToken = nil
			_, err := c.EffectReserve(ctx, "flow", "key", "email", opt)
			return err
		}},
		{name: "effect reserve missing digest", call: func(c *Client) error {
			opt := validEffectReserveOptions()
			opt.OperationDigest = ""
			_, err := c.EffectReserve(ctx, "flow", "key", "email", opt)
			return err
		}},
		{name: "effect status negative latency", call: func(c *Client) error {
			opt := validEffectStatusOptions()
			opt.LatencyMS = Int64(-1)
			_, err := c.EffectConfirm(ctx, "flow", "key", opt)
			return err
		}},
		{name: "effect get empty key", call: func(c *Client) error {
			_, err := c.EffectGet(ctx, "flow", "", "")
			return err
		}},
		{name: "approval request missing flow", call: func(c *Client) error {
			_, err := c.ApprovalRequest(ctx, "approval", ApprovalRequestOptions{Scope: "scope"})
			return err
		}},
		{name: "approval request invalid policy version", call: func(c *Client) error {
			_, err := c.ApprovalRequest(ctx, "approval", ApprovalRequestOptions{FlowID: "flow", Scope: "scope", PolicyVersion: -1})
			return err
		}},
		{name: "approval request non-positive timeout", call: func(c *Client) error {
			_, err := c.ApprovalRequest(ctx, "approval", ApprovalRequestOptions{FlowID: "flow", Scope: "scope", TimeoutMS: Int64(0)})
			return err
		}},
		{name: "approval request conflicting expiry", call: func(c *Client) error {
			_, err := c.ApprovalRequest(ctx, "approval", ApprovalRequestOptions{
				FlowID: "flow", Scope: "scope", TimeoutMS: Int64(1), ExpiresAtMS: Int64(2),
			})
			return err
		}},
		{name: "approval decision missing approver", call: func(c *Client) error {
			_, err := c.ApprovalApprove(ctx, "approval", "", "", nil)
			return err
		}},
		{name: "approval list invalid status", call: func(c *Client) error {
			_, err := c.ApprovalList(ctx, ApprovalListOptions{Status: "unknown"})
			return err
		}},
		{name: "approval list blank scope", call: func(c *Client) error {
			_, err := c.ApprovalList(ctx, ApprovalListOptions{Scope: "  "})
			return err
		}},
		{name: "overview non-positive limit", call: func(c *Client) error {
			_, err := c.GovernanceOverview(ctx, ApprovalListOptions{Limit: Int(0)})
			return err
		}},
		{name: "circuit empty scope", call: func(c *Client) error {
			_, err := c.CircuitOpen(ctx, "", Int64(1), Int64(1), nil)
			return err
		}},
		{name: "circuit non-positive duration", call: func(c *Client) error {
			_, err := c.CircuitOpen(ctx, "scope", Int64(0), Int64(1), nil)
			return err
		}},
		{name: "budget non-positive amount", call: func(c *Client) error {
			_, err := c.BudgetReserve(ctx, "scope", 0, nil, nil, "", nil)
			return err
		}},
		{name: "budget invalid window", call: func(c *Client) error {
			_, err := c.BudgetReserve(ctx, "scope", 1, nil, Int64(0), "", nil)
			return err
		}},
		{name: "budget settlement empty reservation", call: func(c *Client) error {
			_, err := c.BudgetCommit(ctx, "scope", "", 0, nil, nil)
			return err
		}},
		{name: "budget settlement negative actual", call: func(c *Client) error {
			_, err := c.BudgetCommit(ctx, "scope", "reservation", -1, nil, nil)
			return err
		}},
		{name: "budget list invalid limit", call: func(c *Client) error {
			_, err := c.BudgetList(ctx, "", "", Int(0))
			return err
		}},
		{name: "budget list blank scope", call: func(c *Client) error {
			_, err := c.BudgetList(ctx, "  ", "", nil)
			return err
		}},
		{name: "limit negative shard", call: func(c *Client) error {
			_, err := c.LimitLease(ctx, "scope", -1, 1, 1, nil, nil)
			return err
		}},
		{name: "limit excessive amount", call: func(c *Client) error {
			_, err := c.LimitSpend(ctx, "scope", 0, 1001, nil)
			return err
		}},
		{name: "limit non-positive ttl", call: func(c *Client) error {
			_, err := c.LimitLease(ctx, "scope", 0, 1, 0, nil, nil)
			return err
		}},
		{name: "limit timestamp overflow", call: func(c *Client) error {
			_, err := c.LimitLease(ctx, "scope", 0, 1, 2, nil, Int64(math.MaxInt64))
			return err
		}},
		{name: "limit release empty reservation ids", call: func(c *Client) error {
			_, err := c.LimitRelease(ctx, "scope", LimitReleaseOptions{ShardID: 0})
			return err
		}},
		{name: "limit get empty scope", call: func(c *Client) error {
			_, err := c.LimitGet(ctx, "", nil)
			return err
		}},
		{name: "limit list negative now", call: func(c *Client) error {
			_, err := c.LimitList(ctx, "", "", nil, Int64(-1))
			return err
		}},
		{name: "limit list blank partition", call: func(c *Client) error {
			_, err := c.LimitList(ctx, "", "  ", nil, nil)
			return err
		}},
	}
	assertAdminCallsRejectedBeforeTransport(t, tests)
}

func TestFlowAdminReadCommandsRejectInvalidArgumentsBeforeTransport(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "stats empty type", call: func(c *Client) error { _, err := c.Stats(ctx, "", ReadOptions{}); return err }},
		{name: "stats invalid count", call: func(c *Client) error { _, err := c.Stats(ctx, "type", ReadOptions{Count: Int(0)}); return err }},
		{name: "count empty state", call: func(c *Client) error { _, err := c.CountByState(ctx, "type", "", ReadOptions{}); return err }},
		{name: "attribute empty name", call: func(c *Client) error { _, err := c.AttributeValues(ctx, "type", "", ReadOptions{}); return err }},
		{name: "ledger empty id", call: func(c *Client) error { _, err := c.GovernanceLedger(ctx, "", GovernanceLedgerOptions{}); return err }},
		{name: "ledger negative time", call: func(c *Client) error {
			_, err := c.GovernanceLedger(ctx, "flow", GovernanceLedgerOptions{FromMS: Int64(-1)})
			return err
		}},
	}
	assertAdminCallsRejectedBeforeTransport(t, tests)
}

func assertAdminCallsRejectedBeforeTransport(t *testing.T, tests []struct {
	name string
	call func(*Client) error
}) {
	t.Helper()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: map[string]any{}}
			if err := test.call(NewClientWithExecutor(exec)); err == nil {
				t.Fatal("invalid admin command succeeded")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid admin command reached transport: %#v", exec.calls)
			}
		})
	}
}

func validEffectReserveOptions() EffectReserveOptions {
	fencing := int64(1)
	return EffectReserveOptions{LeaseToken: "lease", FencingToken: &fencing, OperationDigest: "digest"}
}

func validEffectStatusOptions() EffectStatusOptions {
	fencing := int64(1)
	return EffectStatusOptions{LeaseToken: "lease", FencingToken: &fencing}
}
