//go:build integration

package ferricstore

import (
	"testing"
	"time"
)

func TestIntegrationFlowAttributesSchedulesAndGovernance(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(JSONCodec{})
	defer client.Close()

	runID := integrationSuffix("latest")
	typeName := "go-sdk-latest-" + runID
	partition := "go-sdk:latest:" + runID + ":partition"
	now := time.Now().UnixMilli()

	attrID := "go-sdk:attr:" + runID
	_ = must[*FlowRecord](t)(client.Create(ctx, CreateOptions{
		ID:           attrID,
		Type:         typeName,
		State:        "attr",
		PartitionKey: partition,
		Payload:      map[string]any{"step": "attr"},
		Attributes:   map[string]any{"tenant": "acme", "tier": "gold"},
		RunAtMS:      now,
		NowMS:        now,
		Idempotent:   Bool(true),
	}))
	listed := must[[]FlowRecord](t)(client.List(ctx, typeName, ReadOptions{
		State:                "attr",
		PartitionKey:         partition,
		Attributes:           map[string]any{"tenant": "acme"},
		ConsistentProjection: Bool(true),
		Count:                Int(10),
	}))
	if !hasRecordID(listed, attrID) {
		t.Fatalf("attribute list did not include %s: %#v", attrID, listed)
	}
	requireMap(t, must[map[string]any](t)(client.Stats(ctx, typeName, ReadOptions{State: "attr", PartitionKey: partition, Attributes: map[string]any{"tenant": "acme"}, ConsistentProjection: Bool(true)})))
	requireValue(t, must[[]map[string]any](t)(client.Attributes(ctx, typeName, ReadOptions{State: "attr", PartitionKey: partition, ConsistentProjection: Bool(true), Count: Int(10)})))
	requireValue(t, must[[]map[string]any](t)(client.AttributeValues(ctx, typeName, "tenant", ReadOptions{State: "attr", PartitionKey: partition, ConsistentProjection: Bool(true), Count: Int(10)})))

	scheduleID := "go-sdk:schedule:" + runID
	scheduledFlowID := "go-sdk:scheduled:" + runID
	_ = must[ScheduleResult](t)(client.ScheduleCreate(ctx, scheduleID, ScheduleOptions{
		Kind: "one_shot",
		AtMS: Int64(now + 60_000),
		Target: map[string]any{
			"id":            scheduledFlowID,
			"type":          typeName,
			"state":         "scheduled",
			"partition_key": partition,
			"payload":       map[string]any{"scheduled": true},
		},
		Overwrite: Bool(true),
		NowMS:     Int64(now),
	}))
	if got := must[*ScheduleResult](t)(client.ScheduleGet(ctx, scheduleID, nil)); got == nil || got.ID == "" {
		t.Fatalf("schedule get = %#v", got)
	}
	_ = must[ScheduleResult](t)(client.SchedulePause(ctx, scheduleID, Int64(now+1)))
	_ = must[ScheduleResult](t)(client.ScheduleResume(ctx, scheduleID, Int64(now+2)))
	requireValue(t, must[[]ScheduleResult](t)(client.ScheduleList(ctx, ScheduleListOptions{Count: Int(10)})))
	_ = must[ScheduleResult](t)(client.ScheduleFire(ctx, scheduleID, Int64(now+3)))
	_ = must[ScheduleResult](t)(client.ScheduleFireDue(ctx, Int64(now+4), "go-sdk-scheduler", nil, Int(1)))
	deleteScheduleID := "go-sdk:schedule-delete:" + runID
	_ = must[ScheduleResult](t)(client.ScheduleCreate(ctx, deleteScheduleID, ScheduleOptions{
		Kind:      "one_shot",
		AtMS:      Int64(now + 120_000),
		Target:    map[string]any{"id": scheduledFlowID + ":delete", "type": typeName, "state": "scheduled", "partition_key": partition},
		Overwrite: Bool(true),
		NowMS:     Int64(now),
	}))
	_ = must[ScheduleResult](t)(client.ScheduleDelete(ctx, deleteScheduleID, Int64(now+5)))

	gov := createAndClaim(t, ctx, client, typeName, runID, "governance", "queued", now, 30_000)
	effectKey := "send-email"
	_ = must[EffectResult](t)(client.EffectReserve(ctx, gov.id, effectKey, "email.send", EffectReserveOptions{
		PartitionKey:    gov.partitionKey,
		LeaseToken:      gov.job.LeaseToken,
		FencingToken:    &gov.job.FencingToken,
		OperationDigest: "digest-1",
		IdempotencyKey:  "idem:" + runID,
		NowMS:           Int64(now + 10),
	}))
	_ = must[EffectResult](t)(client.EffectConfirm(ctx, gov.id, effectKey, EffectStatusOptions{PartitionKey: gov.partitionKey, LeaseToken: gov.job.LeaseToken, FencingToken: &gov.job.FencingToken, ExternalID: "mail-1", LatencyMS: Int64(12), NowMS: Int64(now + 11)}))
	if effect := must[*EffectResult](t)(client.EffectGet(ctx, gov.id, effectKey, gov.partitionKey)); effect == nil || effect.Status == "" {
		t.Fatalf("effect get = %#v", effect)
	}
	_ = must[EffectResult](t)(client.EffectReserve(ctx, gov.id, "send-push", "push.send", EffectReserveOptions{PartitionKey: gov.partitionKey, LeaseToken: gov.job.LeaseToken, FencingToken: &gov.job.FencingToken, OperationDigest: "digest-2", IdempotencyKey: "idem:" + runID + ":push", NowMS: Int64(now + 12)}))
	_ = must[EffectResult](t)(client.EffectCompensate(ctx, gov.id, "send-push", EffectStatusOptions{PartitionKey: gov.partitionKey, LeaseToken: gov.job.LeaseToken, FencingToken: &gov.job.FencingToken, Reason: "rollback", NowMS: Int64(now + 13)}))
	_ = must[EffectResult](t)(client.EffectReserve(ctx, gov.id, "send-sms", "sms.send", EffectReserveOptions{PartitionKey: gov.partitionKey, LeaseToken: gov.job.LeaseToken, FencingToken: &gov.job.FencingToken, OperationDigest: "digest-3", IdempotencyKey: "idem:" + runID + ":sms", NowMS: Int64(now + 14)}))
	_ = must[EffectResult](t)(client.EffectFail(ctx, gov.id, "send-sms", EffectStatusOptions{PartitionKey: gov.partitionKey, LeaseToken: gov.job.LeaseToken, FencingToken: &gov.job.FencingToken, Reason: "provider-error", LatencyMS: Int64(20), NowMS: Int64(now + 15)}))
	requireValue(t, must[[]map[string]any](t)(client.GovernanceLedger(ctx, gov.id, GovernanceLedgerOptions{PartitionKey: gov.partitionKey, Limit: Int(10)})))

	approvalScope := "approval:" + runID
	approvalID := "go-sdk:approval:" + runID
	_ = must[ApprovalResult](t)(client.ApprovalRequest(ctx, approvalID, ApprovalRequestOptions{FlowID: gov.id, Scope: approvalScope, Reason: "manual check", RequestedBy: "integration", Assignees: []string{"ops"}, NowMS: Int64(now + 16)}))
	if approval := must[*ApprovalResult](t)(client.ApprovalGet(ctx, approvalID)); approval == nil || approval.Status == "" {
		t.Fatalf("approval get = %#v", approval)
	}
	requireValue(t, must[[]ApprovalResult](t)(client.ApprovalList(ctx, ApprovalListOptions{Scope: approvalScope, Limit: Int(10)})))
	_ = must[ApprovalResult](t)(client.ApprovalApprove(ctx, approvalID, "ops", "ok", Int64(now+17)))
	rejectedID := "go-sdk:approval-reject:" + runID
	_ = must[ApprovalResult](t)(client.ApprovalRequest(ctx, rejectedID, ApprovalRequestOptions{FlowID: gov.id, Scope: approvalScope, Reason: "manual reject", RequestedBy: "integration", NowMS: Int64(now + 18)}))
	_ = must[ApprovalResult](t)(client.ApprovalReject(ctx, rejectedID, "ops", "no", Int64(now+19)))

	circuitScope := "circuit:" + runID
	_ = must[CircuitBreakerStatus](t)(client.CircuitOpen(ctx, circuitScope, Int64(1_000), Int64(3), Int64(now+20)))
	if circuit := must[*CircuitBreakerStatus](t)(client.CircuitGet(ctx, circuitScope)); circuit == nil || circuit.Status == "" {
		t.Fatalf("circuit get = %#v", circuit)
	}
	_ = must[CircuitBreakerStatus](t)(client.CircuitClose(ctx, circuitScope, Int64(now+21)))

	budgetScope := "budget:" + runID
	_ = must[BudgetResult](t)(client.BudgetReserve(ctx, budgetScope, 5, Int64(100), Int64(60_000), "reservation:"+runID+":commit", Int64(now+22)))
	_ = must[BudgetResult](t)(client.BudgetCommit(ctx, budgetScope, "reservation:"+runID+":commit", 4, map[string]any{"tokens": 4}, Int64(now+23)))
	_ = must[BudgetResult](t)(client.BudgetReserve(ctx, budgetScope, 3, Int64(100), Int64(60_000), "reservation:"+runID+":release", Int64(now+24)))
	_ = must[BudgetResult](t)(client.BudgetRelease(ctx, budgetScope, "reservation:"+runID+":release", Int64(now+25)))
	if budget := must[*BudgetResult](t)(client.BudgetGet(ctx, budgetScope)); budget == nil || budget.Scope == "" {
		t.Fatalf("budget get = %#v", budget)
	}
	requireValue(t, must[[]BudgetResult](t)(client.BudgetList(ctx, budgetScope, "", Int(10))))

	limitScope := "limit:" + runID
	requireValue(t, must[LimitResult](t)(client.LimitLease(ctx, limitScope, 0, 5, 30_000, Int64(10), Int64(now+26))))
	spent := must[LimitResult](t)(client.LimitSpend(ctx, limitScope, 0, 2, Int64(now+27)))
	if len(spent.ReservationIDs) != 2 {
		t.Fatalf("limit spend reservation ids = %#v", spent.ReservationIDs)
	}
	requireValue(t, must[LimitResult](t)(client.LimitRelease(ctx, limitScope, LimitReleaseOptions{
		ShardID: 0, ReservationIDs: spent.ReservationIDs[:1], NowMS: Int64(now + 28),
	})))
	requireValue(t, must[*LimitResult](t)(client.LimitGet(ctx, limitScope, Int64(now+29))))
	requireValue(t, must[[]LimitResult](t)(client.LimitList(ctx, limitScope, "", Int(10), Int64(now+30))))
	requireValue(t, must[GovernanceOverview](t)(client.GovernanceOverview(ctx, ApprovalListOptions{Limit: Int(10)})))
}
