//go:build integration

package ferricstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

var errIntegrationWorkerStopped = errors.New("integration worker stopped")

func TestIntegrationDurableStepAdvanceReplayAndStaleClaim(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(JSONCodec{})
	defer client.Close()

	runID := integrationSuffix("durable-step")
	id := "go-sdk:durable-step:" + runID
	partition := id + ":partition"
	typeName := "go-sdk-durable-step-" + runID
	now := time.Now().UnixMilli()

	_ = must[*FlowRecord](t)(client.Create(ctx, CreateOptions{
		ID: id, Type: typeName, State: "charge", PartitionKey: partition,
		Payload: map[string]any{"amount": 150}, RunAtMS: now, NowMS: now,
		Idempotent: Bool(true), ReturnRecord: true,
	}))
	jobs := must[[]ClaimedItem](t)(client.ClaimJobs(ctx, ClaimDueOptions{
		Type: typeName, State: "charge", Worker: "go-sdk-durable-worker-a",
		PartitionKey: partition, LeaseMS: 30_000, Limit: 1, NowMS: now + 1,
		IncludeState: true, Payload: Bool(true),
	}))
	requireLen(t, jobs, 1)
	workerA := jobs[0]

	closureRuns := 0
	advanced, firstResult, err := client.Step(
		ctx,
		workerA,
		"charge-customer:v1",
		func() (any, error) {
			closureRuns++
			return map[string]any{"charge_id": "ch_1", "amount": 150}, nil
		},
		"schedule_warning",
	)
	if err != nil {
		t.Fatal(err)
	}
	if closureRuns != 1 || advanced.LeaseToken == workerA.LeaseToken ||
		advanced.FencingToken <= workerA.FencingToken || advanced.RunState != "schedule_warning" {
		t.Fatalf("advanced = %#v, closure runs = %d", advanced, closureRuns)
	}
	assertDurableStepResult(t, firstResult)

	replayed, replayedResult, err := client.Step(
		ctx,
		advanced,
		"charge-customer:v1",
		func() (any, error) {
			closureRuns++
			return map[string]any{"duplicate": true}, nil
		},
		"schedule_warning",
	)
	if err != nil {
		t.Fatal(err)
	}
	if closureRuns != 1 || replayed.RunState != "schedule_warning" ||
		replayed.LeaseToken != advanced.LeaseToken || replayed.FencingToken != advanced.FencingToken {
		t.Fatalf("replayed = %#v, closure runs = %d", replayed, closureRuns)
	}
	assertDurableStepResult(t, replayedResult)

	staleRuns := 0
	if _, _, err := client.Step(ctx, workerA, "stale-write:v1", func() (any, error) {
		staleRuns++
		return "stale", nil
	}, "must-not-be-entered"); err == nil {
		t.Fatal("stale worker claim unexpectedly passed durable-step preflight")
	}
	if staleRuns != 0 {
		t.Fatalf("stale closure runs = %d; want 0", staleRuns)
	}

	finalClaim := must[ClaimedItem](t)(client.Advance(ctx, replayed, "finalize"))
	if finalClaim.RunState != "finalize" || finalClaim.LeaseToken == replayed.LeaseToken ||
		finalClaim.FencingToken <= replayed.FencingToken {
		t.Fatalf("final claim = %#v", finalClaim)
	}
	_ = must[*FlowRecord](t)(client.Complete(ctx, CompleteOptions{
		ID: finalClaim.ID, LeaseToken: finalClaim.LeaseToken,
		FencingToken: finalClaim.FencingToken, PartitionKey: finalClaim.PartitionKey,
		Result: map[string]any{"done": true},
	}))
}

func TestIntegrationWorkflowContextChainsAdvanceAndStepWithoutSecondMutation(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(JSONCodec{})
	defer client.Close()

	runID := integrationSuffix("workflow-context-durable-step")
	id := "go-sdk:workflow-context-durable-step:" + runID
	partition := id + ":partition"
	typeName := "go-sdk-workflow-context-durable-step-" + runID
	now := time.Now().UnixMilli()

	workflow := NewWorkflowClient(client).Workflow(typeName, "prepare")
	closureRuns := 0
	var final AppliedOutcome
	workflow.State("prepare", func(callCtx context.Context, workflowCtx WorkflowContext) (Outcome, error) {
		if _, err := workflowCtx.Advance(callCtx, "charge"); err != nil {
			return nil, err
		}
		step, err := workflowCtx.Step(callCtx, "charge-customer:v1", func() (any, error) {
			closureRuns++
			return map[string]any{"charge_id": "ch_worker_1"}, nil
		}, "schedule_warning")
		if err != nil {
			return nil, err
		}
		var ok bool
		final, ok = step.AppliedOutcome()
		if !ok {
			t.Fatal("new workflow durable step replayed unexpectedly")
		}
		return final, nil
	})

	created := must[*FlowRecord](t)(workflow.Start(ctx, id, map[string]any{"amount": 150}, CreateOptions{
		PartitionKey: partition, RunAtMS: now, NowMS: now, ReturnRecord: true,
	}))
	worker := workflow.Worker("go-sdk-context-worker", []string{"prepare"}, WorkerOptions{
		BatchSize: 1, LeaseMS: 30_000, NowMS: now + 1, ClaimPayload: true, PartitionKey: partition,
	})
	var result WorkflowWorkerResult
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		var err error
		result, err = worker.RunOnce(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if result.Claimed != 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if result.Claimed != 1 || result.Applied != 1 || closureRuns != 1 {
		t.Fatalf("worker result = %#v, closure runs = %d, created state = %q", result, closureRuns, created.State)
	}
	if final.Claim().RunState != "schedule_warning" || final.Claim().FencingToken < 3 {
		t.Fatalf("final applied claim = %#v", final.Claim())
	}
	stored := must[*FlowRecord](t)(client.Get(ctx, id, partition, []string{durableStepGoldenKey}))
	if stored.State != "schedule_warning" || stored.LeaseToken != "" {
		t.Fatalf("stored workflow state = %q run_state = %q", stored.State, stored.RunState)
	}
	resultValue, ok := stored.Values[durableStepGoldenKey].(map[string]any)
	if !ok || resultValue["charge_id"] != "ch_worker_1" {
		t.Fatalf("stored durable result = %#v", stored.Values[durableStepGoldenKey])
	}

	var nextLease string
	var nextFence int64
	workflow.State("schedule_warning", func(_ context.Context, workflowCtx WorkflowContext) (Outcome, error) {
		nextLease, nextFence = workflowCtx.Job.LeaseToken, workflowCtx.Job.FencingToken
		return CompleteWith(map[string]any{"done": true}), nil
	})
	nextWorker := workflow.Worker("go-sdk-context-next-worker", []string{"schedule_warning"}, WorkerOptions{
		BatchSize: 1, LeaseMS: 30_000, NowMS: time.Now().UnixMilli() + 1_000,
		PartitionKey: partition, ReclaimExpired: Bool(false),
	})
	nextResult, nextErr := runIntegrationWorkerUntilClaim(ctx, nextWorker)
	if nextErr != nil || nextResult.Claimed != 1 || nextResult.Applied != 1 {
		t.Fatalf("next worker result = %#v, error = %v", nextResult, nextErr)
	}
	if nextLease == final.Claim().LeaseToken || nextFence <= final.Claim().FencingToken {
		t.Fatalf("released claim=%q/%d next claim=%q/%d", final.Claim().LeaseToken, final.Claim().FencingToken, nextLease, nextFence)
	}
}

func TestIntegrationWorkflowWorkerRecoversStepStoppedBeforeCommit(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(JSONCodec{})
	defer client.Close()

	runID := integrationSuffix("worker-stop-before-step-commit")
	id := "go-sdk:worker-stop-before-step-commit:" + runID
	partition := id + ":partition"
	typeName := "go-sdk-worker-stop-before-step-commit-" + runID
	now := time.Now().UnixMilli()
	workflow := NewWorkflowClient(client).Workflow(typeName, "charge")
	attempts := 0
	var workerALease, workerBLease string
	var workerAFence, workerBFence int64
	var workerAClaim ClaimedItem
	var applied AppliedOutcome
	workflow.State("charge", func(callCtx context.Context, workflowCtx WorkflowContext) (Outcome, error) {
		attempts++
		if attempts == 1 {
			workerALease, workerAFence = workflowCtx.Job.LeaseToken, workflowCtx.Job.FencingToken
			workerAClaim = workflowCtx.claim()
		} else {
			workerBLease, workerBFence = workflowCtx.Job.LeaseToken, workflowCtx.Job.FencingToken
		}
		step, err := workflowCtx.Step(callCtx, "charge-customer:v1", func() (any, error) {
			if attempts == 1 {
				return nil, errIntegrationWorkerStopped
			}
			return map[string]any{"charge_id": "ch_recovered"}, nil
		}, "schedule_warning")
		if err != nil {
			return nil, err
		}
		var ok bool
		applied, ok = step.AppliedOutcome()
		if !ok {
			t.Fatal("uncommitted step unexpectedly replayed")
		}
		return applied, nil
	})

	_ = must[*FlowRecord](t)(workflow.Start(ctx, id, map[string]any{"amount": 150}, CreateOptions{
		PartitionKey: partition, RunAtMS: now, NowMS: now, ReturnRecord: true,
	}))
	workerA := workflow.Worker("go-sdk-worker-a", []string{"charge"}, WorkerOptions{
		BatchSize: 1, LeaseMS: 30_000, NowMS: now + 1, PartitionKey: partition,
		ErrorPolicy: ErrorPolicyReturn,
	})
	result, err := runIntegrationWorkerUntilClaim(ctx, workerA)
	if !errors.Is(err, errIntegrationWorkerStopped) || result.Claimed != 1 || result.Applied != 0 {
		t.Fatalf("worker A result = %#v, error = %v", result, err)
	}

	workerB := workflow.Worker("go-sdk-worker-b", []string{"charge"}, WorkerOptions{
		BatchSize: 1, LeaseMS: 30_000, NowMS: now + durableStepLeaseMS + 1_000,
		PartitionKey: partition, ReclaimExpired: Bool(true), ErrorPolicy: ErrorPolicyReturn,
	})
	result, err = runIntegrationWorkerUntilClaim(ctx, workerB)
	if err != nil || result.Claimed != 1 || result.Applied != 1 {
		t.Fatalf("worker B result = %#v, error = %v", result, err)
	}
	if attempts != 2 || workerALease == workerBLease || workerBFence <= workerAFence {
		t.Fatalf("attempts=%d worker A=%q/%d worker B=%q/%d", attempts, workerALease, workerAFence, workerBLease, workerBFence)
	}
	if applied.Claim().RunState != "schedule_warning" {
		t.Fatalf("recovered claim = %#v", applied.Claim())
	}
	staleRuns := 0
	if _, _, err := client.Step(ctx, workerAClaim, "stale-after-takeover:v1", func() (any, error) {
		staleRuns++
		return "must-not-commit", nil
	}, "must-not-be-entered"); err == nil {
		t.Fatal("stale worker A unexpectedly passed durable-step preflight after worker B takeover")
	}
	if staleRuns != 0 {
		t.Fatalf("stale worker A closure runs = %d; want 0", staleRuns)
	}
	completeReleasedWorkflow(t, ctx, workflow, partition, now+durableStepLeaseMS+2_000)
}

func TestIntegrationWorkflowWorkerUsesStableExternalIdempotencyAfterStop(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(JSONCodec{})
	defer client.Close()

	runID := integrationSuffix("worker-external-idempotency")
	id := "go-sdk:worker-external-idempotency:" + runID
	partition := id + ":partition"
	typeName := "go-sdk-worker-external-idempotency-" + runID
	now := time.Now().UnixMilli()
	idempotencyKey := id + ":charge-customer:v1"
	effects := map[string]map[string]any{}
	providerAttempts := 0
	stepAttempts := 0
	var workerALease, workerBLease string
	var workerAFence, workerBFence int64
	var applied AppliedOutcome
	workflow := NewWorkflowClient(client).Workflow(typeName, "charge")
	workflow.State("charge", func(callCtx context.Context, workflowCtx WorkflowContext) (Outcome, error) {
		stepAttempts++
		if stepAttempts == 1 {
			workerALease, workerAFence = workflowCtx.Job.LeaseToken, workflowCtx.Job.FencingToken
		} else {
			workerBLease, workerBFence = workflowCtx.Job.LeaseToken, workflowCtx.Job.FencingToken
		}
		step, err := workflowCtx.Step(callCtx, "charge-customer:v1", func() (any, error) {
			providerAttempts++
			effect := effects[idempotencyKey]
			if effect == nil {
				effect = map[string]any{"charge_id": "ch_idempotent"}
				effects[idempotencyKey] = effect
			}
			if stepAttempts == 1 {
				return nil, errIntegrationWorkerStopped
			}
			return effect, nil
		}, "schedule_warning")
		if err != nil {
			return nil, err
		}
		var ok bool
		applied, ok = step.AppliedOutcome()
		if !ok {
			t.Fatal("uncommitted external step unexpectedly replayed")
		}
		return applied, nil
	})

	_ = must[*FlowRecord](t)(workflow.Start(ctx, id, map[string]any{"amount": 150}, CreateOptions{
		PartitionKey: partition, RunAtMS: now, NowMS: now,
	}))
	workerA := workflow.Worker("go-sdk-external-worker-a", []string{"charge"}, WorkerOptions{
		BatchSize: 1, LeaseMS: 30_000, NowMS: now + 1, PartitionKey: partition,
		ErrorPolicy: ErrorPolicyReturn,
	})
	if result, err := runIntegrationWorkerUntilClaim(ctx, workerA); !errors.Is(err, errIntegrationWorkerStopped) || result.Claimed != 1 {
		t.Fatalf("worker A result = %#v, error = %v", result, err)
	}
	workerB := workflow.Worker("go-sdk-external-worker-b", []string{"charge"}, WorkerOptions{
		BatchSize: 1, LeaseMS: 30_000, NowMS: now + durableStepLeaseMS + 1_000,
		PartitionKey: partition, ReclaimExpired: Bool(true), ErrorPolicy: ErrorPolicyReturn,
	})
	if result, err := runIntegrationWorkerUntilClaim(ctx, workerB); err != nil || result.Claimed != 1 || result.Applied != 1 {
		t.Fatalf("worker B result = %#v, error = %v", result, err)
	}
	if providerAttempts != 2 || len(effects) != 1 || workerALease == workerBLease || workerBFence <= workerAFence {
		t.Fatalf("provider attempts=%d effects=%d worker A=%q/%d worker B=%q/%d", providerAttempts, len(effects), workerALease, workerAFence, workerBLease, workerBFence)
	}
	if applied.Claim().RunState != "schedule_warning" {
		t.Fatalf("external recovered claim = %#v", applied.Claim())
	}
	completeReleasedWorkflow(t, ctx, workflow, partition, now+durableStepLeaseMS+2_000)
}

func TestIntegrationWorkflowWorkerRecoversAfterCommittedResponseLoss(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	base := integrationClient(JSONCodec{})
	defer base.Close()
	lostResponse := errors.New("injected response loss after commit")
	lossy := NewClientWithExecutor(&loseStepContinueResponseExecutor{
		base: base.exec, err: lostResponse,
	}, WithCodec(JSONCodec{}))

	runID := integrationSuffix("worker-response-loss")
	id := "go-sdk:worker-response-loss:" + runID
	partition := id + ":partition"
	typeName := "go-sdk-worker-response-loss-" + runID
	now := time.Now().UnixMilli()
	workflowA := NewWorkflowClient(lossy).Workflow(typeName, "charge")
	closureRuns := 0
	var workerALease string
	var workerAFence int64
	workflowA.State("charge", func(callCtx context.Context, workflowCtx WorkflowContext) (Outcome, error) {
		workerALease, workerAFence = workflowCtx.Job.LeaseToken, workflowCtx.Job.FencingToken
		_, err := workflowCtx.Step(callCtx, "charge-customer:v1", func() (any, error) {
			closureRuns++
			return map[string]any{"charge_id": "ch_committed"}, nil
		}, "schedule_warning")
		return nil, err
	})
	_ = must[*FlowRecord](t)(workflowA.Start(ctx, id, map[string]any{"amount": 150}, CreateOptions{
		PartitionKey: partition, RunAtMS: now, NowMS: now,
	}))
	workerA := workflowA.Worker("go-sdk-response-loss-worker-a", []string{"charge"}, WorkerOptions{
		BatchSize: 1, LeaseMS: 30_000, NowMS: now + 1, PartitionKey: partition,
		ErrorPolicy: ErrorPolicyFail,
	})
	result, err := runIntegrationWorkerUntilClaim(ctx, workerA)
	if !errors.Is(err, ErrDurableMutationUncertain) || !errors.Is(err, lostResponse) ||
		result.Claimed != 1 || result.Applied != 0 {
		t.Fatalf("worker A result = %#v, error = %v", result, err)
	}
	committed := must[*FlowRecord](t)(base.Get(ctx, id, partition, []string{durableStepGoldenKey}))
	if committed.State != "running" || committed.RunState != "schedule_warning" || closureRuns != 1 {
		t.Fatalf("committed workflow = %#v, closure runs = %d", committed, closureRuns)
	}

	workflowB := NewWorkflowClient(base).Workflow(typeName, "charge")
	var workerBLease string
	var workerBFence int64
	workflowB.State("schedule_warning", func(_ context.Context, workflowCtx WorkflowContext) (Outcome, error) {
		workerBLease, workerBFence = workflowCtx.Job.LeaseToken, workflowCtx.Job.FencingToken
		return CompleteWith(map[string]any{"done": true}), nil
	})
	workerB := workflowB.Worker("go-sdk-response-loss-worker-b", []string{"schedule_warning"}, WorkerOptions{
		BatchSize: 1, LeaseMS: 30_000, NowMS: now + durableStepLeaseMS + 1_000,
		PartitionKey: partition, ReclaimExpired: Bool(true), ErrorPolicy: ErrorPolicyReturn,
	})
	result, err = runIntegrationWorkerUntilClaim(ctx, workerB)
	if err != nil || result.Claimed != 1 || result.Applied != 1 {
		t.Fatalf("worker B result = %#v, error = %v", result, err)
	}
	if closureRuns != 1 || workerALease == workerBLease || workerBFence <= workerAFence {
		t.Fatalf("closure runs=%d worker A=%q/%d worker B=%q/%d", closureRuns, workerALease, workerAFence, workerBLease, workerBFence)
	}
}

func TestIntegrationWaitingWorkflowReleasesClaimAndContinuesAfterSignal(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(JSONCodec{})
	defer client.Close()
	runID := integrationSuffix("waiting-workflow-recovery")
	id := "go-sdk:waiting-workflow-recovery:" + runID
	partition := id + ":partition"
	typeName := "go-sdk-waiting-workflow-recovery-" + runID
	now := time.Now().UnixMilli()
	prepareRuns := 0
	durableStepRuns := 0
	var workerALease, workerBLease string
	var workerAFence, workerBFence int64
	workflow := NewWorkflowClient(client).Workflow(typeName, "prepare")
	workflow.State("prepare", func(callCtx context.Context, workflowCtx WorkflowContext) (Outcome, error) {
		prepareRuns++
		workerALease, workerAFence = workflowCtx.Job.LeaseToken, workflowCtx.Job.FencingToken
		if _, err := workflowCtx.Step(callCtx, "prepare-approval:v1", func() (any, error) {
			durableStepRuns++
			return map[string]any{"prepared": true}, nil
		}, "waiting"); err != nil {
			return nil, err
		}
		return TransitionResult{ToState: "waiting", RunAtMS: now + 60_000}, nil
	})
	workflow.State("ready", func(_ context.Context, workflowCtx WorkflowContext) (Outcome, error) {
		workerBLease, workerBFence = workflowCtx.Job.LeaseToken, workflowCtx.Job.FencingToken
		return CompleteWith(map[string]any{"done": true}), nil
	})
	_ = must[*FlowRecord](t)(workflow.Start(ctx, id, map[string]any{"approval": true}, CreateOptions{
		PartitionKey: partition, RunAtMS: now, NowMS: now,
	}))
	workerA := workflow.Worker("go-sdk-waiting-worker-a", []string{"prepare"}, WorkerOptions{
		BatchSize: 1, LeaseMS: 30_000, NowMS: now + 1, PartitionKey: partition,
	})
	if result, err := runIntegrationWorkerUntilClaim(ctx, workerA); err != nil || result.Claimed != 1 || result.Applied != 1 {
		t.Fatalf("worker A result = %#v, error = %v", result, err)
	}
	waiting := must[*FlowRecord](t)(client.Get(ctx, id, partition, nil))
	if waiting.State != "waiting" || waiting.LeaseToken != "" {
		t.Fatalf("waiting workflow retained worker claim: %#v", waiting)
	}
	if _, committed := waiting.ValueRefs[durableStepValueName("prepare-approval:v1")]; !committed {
		t.Fatalf("waiting workflow lost its committed durable step: %#v", waiting.ValueRefs)
	}
	_ = must[any](t)(client.Signal(ctx, SignalOptions{
		ID: id, Signal: "approved", PartitionKey: partition,
		IfStates: []string{"waiting"}, TransitionTo: "ready", RunAtMS: now + 2, NowMS: now + 2,
	}))
	workerB := workflow.Worker("go-sdk-waiting-worker-b", []string{"ready"}, WorkerOptions{
		BatchSize: 1, LeaseMS: 30_000, NowMS: now + 3, PartitionKey: partition,
	})
	if result, err := runIntegrationWorkerUntilClaim(ctx, workerB); err != nil || result.Claimed != 1 || result.Applied != 1 {
		t.Fatalf("worker B result = %#v, error = %v", result, err)
	}
	if prepareRuns != 1 || durableStepRuns != 1 || workerALease == workerBLease || workerBFence <= workerAFence {
		t.Fatalf(
			"prepare runs=%d durable step runs=%d worker A=%q/%d worker B=%q/%d",
			prepareRuns, durableStepRuns, workerALease, workerAFence, workerBLease, workerBFence,
		)
	}
}

type loseStepContinueResponseExecutor struct {
	base Executor
	err  error
	mu   sync.Mutex
	lost bool
}

func (e *loseStepContinueResponseExecutor) Do(ctx context.Context, args ...any) (any, error) {
	value, err := e.base.Do(ctx, args...)
	if err != nil || len(args) == 0 || asString(args[0]) != "FLOW.STEP_CONTINUE" {
		return value, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.lost {
		return value, nil
	}
	e.lost = true
	return nil, e.err
}

func runIntegrationWorkerUntilClaim(ctx context.Context, worker *WorkflowWorker) (WorkflowWorkerResult, error) {
	var result WorkflowWorkerResult
	var err error
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		result, err = worker.RunOnce(ctx)
		if err != nil || result.Claimed != 0 {
			return result, err
		}
		time.Sleep(20 * time.Millisecond)
	}
	return result, err
}

func completeReleasedWorkflow(t *testing.T, ctx context.Context, workflow *Workflow, partition string, now int64) {
	t.Helper()
	workflow.State("schedule_warning", func(context.Context, WorkflowContext) (Outcome, error) {
		return CompleteWith(map[string]any{"done": true}), nil
	})
	worker := workflow.Worker("go-sdk-released-state-worker", []string{"schedule_warning"}, WorkerOptions{
		BatchSize: 1, LeaseMS: 30_000, NowMS: now, PartitionKey: partition,
	})
	result, err := runIntegrationWorkerUntilClaim(ctx, worker)
	if err != nil || result.Claimed != 1 || result.Applied != 1 {
		t.Fatalf("released-state worker result = %#v, error = %v", result, err)
	}
}

func assertDurableStepResult(t *testing.T, value any) {
	t.Helper()
	result, ok := value.(map[string]any)
	if !ok || result["charge_id"] != "ch_1" || asInt64(result["amount"]) != 150 {
		t.Fatalf("durable step result = %#v", value)
	}
}
