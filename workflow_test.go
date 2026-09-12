package ferricstore

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestWorkflowWorkerTransitionsState(t *testing.T) {
	exec := &fakeExecutor{value: []any{
		map[string]any{
			"id":            "flow-1",
			"type":          "order",
			"state":         "validate",
			"partition_key": "tenant:1",
			"lease_token":   "lease-1",
			"fencing_token": int64(9),
		},
	}}
	client := NewClientWithExecutor(exec)
	workflow := NewWorkflowClient(client).Workflow("order", "validate")
	workflow.State("validate", func(_ context.Context, ctx WorkflowContext) (Outcome, error) {
		if ctx.ID() != "flow-1" || ctx.State() != "validate" {
			t.Fatalf("unexpected context: %+v", ctx.Job)
		}
		return TransitionTo("charge", []byte("next")), nil
	})

	result, err := workflow.Worker("worker-1", []string{"validate"}, WorkerOptions{BatchSize: 1}).RunOnce(context.Background())

	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 1 || result.Applied != 1 {
		t.Fatalf("unexpected worker result: %+v", result)
	}
	if len(exec.calls) != 2 {
		t.Fatalf("expected claim and transition calls, got %d", len(exec.calls))
	}
	if exec.calls[1][0] != "FLOW.TRANSITION" {
		t.Fatalf("expected FLOW.TRANSITION, got %#v", exec.calls[1])
	}
}

func TestWorkflowWorkerAcceptsPointerOutcomes(t *testing.T) {
	tests := []struct {
		name    string
		outcome Outcome
		command string
	}{
		{name: "transition", outcome: &TransitionResult{ToState: "next"}, command: "FLOW.TRANSITION"},
		{name: "complete", outcome: &CompleteResult{Result: "done"}, command: "FLOW.COMPLETE"},
		{name: "retry", outcome: &RetryResult{Error: "retry"}, command: "FLOW.RETRY"},
		{name: "fail", outcome: &FailResult{Error: "failed"}, command: "FLOW.FAIL"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "ready")
			worker := workflow.Worker("worker-1", []string{"ready"}, WorkerOptions{})
			job := FlowRecord{
				ID: "flow-1", State: "ready", PartitionKey: "tenant:1",
				LeaseToken: "lease-1", FencingToken: 9,
			}
			err := worker.apply(context.Background(), job, "ready", func(context.Context, WorkflowContext) (Outcome, error) {
				return test.outcome, nil
			}, ErrorPolicyRetry)
			if err != nil {
				t.Fatalf("pointer outcome failed: %v", err)
			}
			if len(exec.calls) != 1 || asString(exec.calls[0][0]) != test.command {
				t.Fatalf("pointer outcome calls = %#v; want %s", exec.calls, test.command)
			}
		})
	}
}

func TestWorkflowContextStepReleasesAppliedOutcomeWithRefreshedClaim(t *testing.T) {
	exec := &scriptedExecutor{results: []scriptedExecutorResult{
		{value: durableStepFlowRecord(nil)},
		{value: []any{"flow-1", "tenant:1", "lease-2", int64(8)}},
		{value: []byte("OK")},
	}}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "charge")
	worker := workflow.Worker("worker-1", []string{"charge"}, WorkerOptions{})
	job := FlowRecord{
		ID: "flow-1", Type: "order", State: "running", RunState: "charge",
		PartitionKey: "tenant:1", LeaseToken: "lease-1", FencingToken: 7,
	}
	runs := 0
	var applied AppliedOutcome
	var storedResult any

	err := worker.apply(context.Background(), job, "charge", func(callCtx context.Context, workflowCtx WorkflowContext) (Outcome, error) {
		step, err := workflowCtx.Step(callCtx, "charge-customer:v1", func() (any, error) {
			runs++
			return map[string]any{"charge_id": "ch_1"}, nil
		}, "schedule_warning")
		if err != nil {
			return nil, err
		}
		storedResult = step.Result()
		var ok bool
		applied, ok = step.AppliedOutcome()
		if !ok {
			t.Fatal("new durable step did not produce an applied outcome")
		}
		return applied, nil
	}, ErrorPolicyRetry)

	if err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Fatalf("closure runs = %d; want 1", runs)
	}
	if applied.Claim().LeaseToken != "lease-2" || applied.Claim().FencingToken != 8 ||
		applied.Claim().RunState != "schedule_warning" {
		t.Fatalf("applied claim = %#v", applied.Claim())
	}
	if !reflect.DeepEqual(storedResult, map[string]any{"charge_id": "ch_1"}) {
		t.Fatalf("applied result = %#v", storedResult)
	}
	if len(exec.calls) != 3 {
		t.Fatalf("durable step and refreshed release calls = %#v", exec.calls)
	}
	if exec.calls[0][0] != "FLOW.EXTEND_LEASE" || exec.calls[1][0] != "FLOW.STEP_CONTINUE" {
		t.Fatalf("unexpected durable step calls: %#v", exec.calls)
	}
	assertCommandPrefix(t, exec.calls[2], []any{
		"FLOW.TRANSITION", "flow-1", "running", "schedule_warning", "LEASE_TOKEN", "lease-2",
		"FENCING", int64(8),
	})
}

func TestWorkflowContextAdvanceCanBeReturnedDirectlyAndReleasesRefreshedClaim(t *testing.T) {
	exec := &scriptedExecutor{results: []scriptedExecutorResult{
		{value: []any{"flow-1", "tenant:1", "lease-2", int64(8)}},
		{value: []byte("OK")},
	}}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "prepare")
	worker := workflow.Worker("worker-1", []string{"prepare"}, WorkerOptions{})
	job := FlowRecord{
		ID: "flow-1", Type: "order", State: "running", RunState: "prepare",
		PartitionKey: "tenant:1", LeaseToken: "lease-1", FencingToken: 7,
	}

	err := worker.apply(context.Background(), job, "prepare", func(callCtx context.Context, workflowCtx WorkflowContext) (Outcome, error) {
		return workflowCtx.Advance(callCtx, "charge")
	}, ErrorPolicyRetry)

	if err != nil {
		t.Fatal(err)
	}
	if len(exec.calls) != 2 {
		t.Fatalf("advance and refreshed release calls = %#v", exec.calls)
	}
	assertCommandPrefix(t, exec.calls[0], []any{
		"FLOW.STEP_CONTINUE", "flow-1", "lease-1", "prepare", "charge", "FENCING", int64(7),
	})
	assertCommandPrefix(t, exec.calls[1], []any{
		"FLOW.TRANSITION", "flow-1", "running", "charge", "LEASE_TOKEN", "lease-2", "FENCING", int64(8),
	})
}

func TestWorkflowContextAdvanceAndStepChainRefreshedClaim(t *testing.T) {
	validatedAfterAdvance := durableStepFlowRecord(nil)
	validatedAfterAdvance["run_state"] = "charge"
	validatedAfterAdvance["lease_token"] = "lease-2"
	validatedAfterAdvance["fencing_token"] = int64(8)
	exec := &scriptedExecutor{results: []scriptedExecutorResult{
		{value: []any{"flow-1", "tenant:1", "lease-2", int64(8)}},
		{value: validatedAfterAdvance},
		{value: []any{"flow-1", "tenant:1", "lease-3", int64(9)}},
		{value: []byte("OK")},
	}}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "prepare")
	worker := workflow.Worker("worker-1", []string{"prepare"}, WorkerOptions{})
	job := FlowRecord{
		ID: "flow-1", Type: "order", State: "running", RunState: "prepare",
		PartitionKey: "tenant:1", LeaseToken: "lease-1", FencingToken: 7,
	}

	err := worker.apply(context.Background(), job, "prepare", func(callCtx context.Context, workflowCtx WorkflowContext) (Outcome, error) {
		first, err := workflowCtx.Advance(callCtx, "charge")
		if err != nil {
			return nil, err
		}
		if first.Claim().LeaseToken != "lease-2" || workflowCtx.State() != "charge" {
			t.Fatalf("context did not adopt first refreshed claim: outcome=%#v state=%q", first.Claim(), workflowCtx.State())
		}
		step, err := workflowCtx.Step(callCtx, "charge-customer:v1", func() (any, error) {
			return "ch_1", nil
		}, "schedule_warning")
		if err != nil {
			return nil, err
		}
		return step.OutcomeOr(CompleteWith(step.Result())), nil
	}, ErrorPolicyRetry)

	if err != nil {
		t.Fatal(err)
	}
	if len(exec.calls) != 4 {
		t.Fatalf("calls = %#v", exec.calls)
	}
	assertCommandPrefix(t, exec.calls[0], []any{
		"FLOW.STEP_CONTINUE", "flow-1", "lease-1", "prepare", "charge", "FENCING", int64(7),
	})
	assertCommandPrefix(t, exec.calls[1], []any{
		"FLOW.EXTEND_LEASE", "flow-1", "lease-2", "FENCING", int64(8),
	})
	assertCommandPrefix(t, exec.calls[2], []any{
		"FLOW.STEP_CONTINUE", "flow-1", "lease-2", "charge", "schedule_warning", "FENCING", int64(8),
	})
	assertCommandPrefix(t, exec.calls[3], []any{
		"FLOW.TRANSITION", "flow-1", "running", "schedule_warning", "LEASE_TOKEN", "lease-3",
		"FENCING", int64(9),
	})
}

func TestWorkflowWorkerRejectsEarlierAppliedOutcomeWithoutStaleRelease(t *testing.T) {
	exec := &scriptedExecutor{results: []scriptedExecutorResult{
		{value: []any{"flow-1", "tenant:1", "lease-2", int64(8)}},
		{value: []any{"flow-1", "tenant:1", "lease-3", int64(9)}},
	}}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "prepare")
	worker := workflow.Worker("worker-1", []string{"prepare"}, WorkerOptions{})
	job := FlowRecord{
		ID: "flow-1", Type: "order", State: "running", RunState: "prepare",
		PartitionKey: "tenant:1", LeaseToken: "lease-1", FencingToken: 7,
	}

	err := worker.apply(context.Background(), job, "prepare", func(callCtx context.Context, workflowCtx WorkflowContext) (Outcome, error) {
		first, err := workflowCtx.Advance(callCtx, "charge")
		if err != nil {
			return nil, err
		}
		if _, err := workflowCtx.Advance(callCtx, "schedule_warning"); err != nil {
			return nil, err
		}
		return first, nil
	}, ErrorPolicyRetry)

	if err == nil || !strings.Contains(err.Error(), "stale applied outcome") {
		t.Fatalf("error = %v; want stale applied outcome", err)
	}
	if len(exec.calls) != 2 {
		t.Fatalf("stale applied outcome issued a release: %#v", exec.calls)
	}
}

func TestWorkflowContextStepReplayContinuesToHandlerOutcome(t *testing.T) {
	preflight := durableStepFlowRecord(map[string]any{durableStepGoldenKey: map[string]any{"ref": "ref-1"}})
	preflight["run_state"] = "schedule_warning"
	exec := &scriptedExecutor{results: []scriptedExecutorResult{
		{value: preflight},
		{value: []any{"ch_replayed"}},
		{value: []byte("OK")},
	}}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "schedule_warning")
	worker := workflow.Worker("worker-1", []string{"schedule_warning"}, WorkerOptions{})
	job := FlowRecord{
		ID: "flow-1", Type: "order", State: "running", RunState: "schedule_warning",
		PartitionKey: "tenant:1", LeaseToken: "lease-1", FencingToken: 7,
	}
	runs := 0

	err := worker.apply(context.Background(), job, "schedule_warning", func(callCtx context.Context, workflowCtx WorkflowContext) (Outcome, error) {
		step, err := workflowCtx.Step(callCtx, "charge-customer:v1", func() (any, error) {
			runs++
			return "must-not-run", nil
		}, "schedule_warning")
		if err != nil {
			return nil, err
		}
		if step.Applied() {
			t.Fatal("replayed durable step reported a new mutation")
		}
		return step.OutcomeOr(CompleteWith(step.Result())), nil
	}, ErrorPolicyRetry)

	if err != nil {
		t.Fatal(err)
	}
	if runs != 0 {
		t.Fatalf("closure runs = %d; want 0", runs)
	}
	if len(exec.calls) != 3 || exec.calls[2][0] != "FLOW.COMPLETE" {
		t.Fatalf("replay did not continue to handler outcome: %#v", exec.calls)
	}
	assertCommandPair(t, exec.calls[2], "RESULT", "ch_replayed")
	for _, call := range exec.calls {
		if call[0] == "FLOW.STEP_CONTINUE" {
			t.Fatalf("replay issued a durable-step mutation: %#v", exec.calls)
		}
	}
}

func TestWorkflowWorkerNeverAppliesErrorPolicyAfterUncertainDurableMutation(t *testing.T) {
	tests := []struct {
		name        string
		policy      ErrorPolicy
		swallowStep bool
		advance     bool
		failure     error
	}{
		{name: "step retry policy", policy: ErrorPolicyRetry},
		{name: "step fail policy", policy: ErrorPolicyFail},
		{name: "swallowed step error", policy: ErrorPolicyRetry, swallowStep: true},
		{name: "advance retry policy", policy: ErrorPolicyRetry, advance: true},
		{
			name: "HTTP 408 after dispatch", policy: ErrorPolicyRetry, advance: true,
			failure: &HTTPError{StatusCode: 408, Code: "request_timeout", Message: "timed out"},
		},
		{
			name: "future native status", policy: ErrorPolicyFail, advance: true,
			failure: NativeError{Status: 99, Value: map[string]any{"message": "future"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			commitErr := test.failure
			if commitErr == nil {
				commitErr = errors.New("connection lost after server commit")
			}
			results := []scriptedExecutorResult{{err: commitErr}}
			if !test.advance {
				results = append([]scriptedExecutorResult{{value: durableStepFlowRecord(nil)}}, results...)
			}
			exec := &scriptedExecutor{results: results}
			workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "charge")
			worker := workflow.Worker("worker-1", []string{"charge"}, WorkerOptions{})
			job := FlowRecord{
				ID: "flow-1", Type: "order", State: "running", RunState: "charge",
				PartitionKey: "tenant:1", LeaseToken: "lease-1", FencingToken: 7,
			}

			err := worker.apply(context.Background(), job, "charge", func(callCtx context.Context, workflowCtx WorkflowContext) (Outcome, error) {
				if test.advance {
					_, err := workflowCtx.Advance(callCtx, "schedule_warning")
					return nil, err
				}
				_, err := workflowCtx.Step(callCtx, "charge-customer:v1", func() (any, error) {
					return "charged", nil
				}, "schedule_warning")
				if test.swallowStep {
					return RetryWith("handler tried to continue"), nil
				}
				return nil, err
			}, test.policy)

			if !errors.Is(err, ErrDurableMutationUncertain) {
				t.Fatalf("error = %v; want uncertain commit cause", err)
			}
			if nativeFailure, ok := commitErr.(NativeError); ok {
				var got NativeError
				if !errors.As(err, &got) || got.Status != nativeFailure.Status {
					t.Fatalf("error = %v; want native status %d", err, nativeFailure.Status)
				}
			} else if !errors.Is(err, commitErr) {
				t.Fatalf("error = %v; want cause %v", err, commitErr)
			}
			wantCalls := 1
			if !test.advance {
				wantCalls = 2
			}
			if len(exec.calls) != wantCalls {
				t.Fatalf("uncertain mutation triggered retry/fail/second write: %#v", exec.calls)
			}
			if got := exec.calls[len(exec.calls)-1][0]; got != "FLOW.STEP_CONTINUE" {
				t.Fatalf("last command = %v; want uncertain STEP_CONTINUE", got)
			}
		})
	}
}

func TestWorkflowWorkerBatchAppliesUnrelatedOutcomeBeforeReturningUncertainError(t *testing.T) {
	commitErr := errors.New("connection lost after server commit")
	exec := &scriptedExecutor{results: []scriptedExecutorResult{
		{value: []any{
			map[string]any{"id": "flow-1", "type": "order", "state": "running", "run_state": "charge", "partition_key": "tenant:1", "lease_token": "lease-1", "fencing_token": int64(7)},
			map[string]any{"id": "flow-2", "type": "order", "state": "running", "run_state": "charge", "partition_key": "tenant:1", "lease_token": "lease-2", "fencing_token": int64(11)},
		}},
		{err: commitErr},
		{value: []byte("OK")},
	}}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "charge")
	workflow.State("charge", func(callCtx context.Context, workflowCtx WorkflowContext) (Outcome, error) {
		if workflowCtx.ID() == "flow-1" {
			return workflowCtx.Advance(callCtx, "next")
		}
		return CompleteWith("unrelated-complete"), nil
	})

	result, err := workflow.Worker("worker-1", []string{"charge"}, WorkerOptions{
		BatchSize: 2, Concurrency: 1,
	}).RunOnce(context.Background())

	if !errors.Is(err, ErrDurableMutationUncertain) || !errors.Is(err, commitErr) {
		t.Fatalf("error = %v; want uncertain commit cause", err)
	}
	if result.Claimed != 2 || result.Applied != 1 {
		t.Fatalf("worker result = %#v; unrelated outcome was not applied", result)
	}
	if len(exec.calls) != 3 || exec.calls[2][0] != "FLOW.COMPLETE" {
		t.Fatalf("worker stopped before unrelated completion: %#v", exec.calls)
	}
}

func TestWorkflowWorkerRejectsUninitializedAppliedOutcome(t *testing.T) {
	exec := &fakeExecutor{}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "ready")
	worker := workflow.Worker("worker-1", []string{"ready"}, WorkerOptions{})
	job := FlowRecord{
		ID: "flow-1", State: "running", RunState: "ready", PartitionKey: "tenant:1",
		LeaseToken: "lease-1", FencingToken: 7,
	}

	err := worker.apply(context.Background(), job, "ready", func(context.Context, WorkflowContext) (Outcome, error) {
		return AppliedOutcome{}, nil
	}, ErrorPolicyRetry)

	if err == nil || !strings.Contains(err.Error(), "applied outcome") {
		t.Fatalf("error = %v; want invalid applied outcome", err)
	}
	if len(exec.calls) != 0 {
		t.Fatalf("invalid applied outcome performed I/O: %#v", exec.calls)
	}
}

func TestWorkflowContextUsesRefreshedClaimForErrorPolicy(t *testing.T) {
	handlerErr := errors.New("handler failed after advance")
	exec := &scriptedExecutor{results: []scriptedExecutorResult{
		{value: []any{"flow-1", "tenant:1", "lease-2", int64(8)}},
		{value: []byte("OK")},
	}}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "prepare")
	worker := workflow.Worker("worker-1", []string{"prepare"}, WorkerOptions{})
	job := FlowRecord{
		ID: "flow-1", State: "running", RunState: "prepare", PartitionKey: "tenant:1",
		LeaseToken: "lease-1", FencingToken: 7,
	}

	err := worker.apply(context.Background(), job, "prepare", func(callCtx context.Context, workflowCtx WorkflowContext) (Outcome, error) {
		if _, err := workflowCtx.Advance(callCtx, "charge"); err != nil {
			return nil, err
		}
		return nil, handlerErr
	}, ErrorPolicyRetry)

	if err != nil {
		t.Fatal(err)
	}
	if len(exec.calls) != 2 || exec.calls[1][0] != "FLOW.RETRY" {
		t.Fatalf("calls = %#v", exec.calls)
	}
	assertCommandPrefix(t, exec.calls[1], []any{
		"FLOW.RETRY", "flow-1", "lease-2", "FENCING", int64(8),
	})
}

func TestWorkflowWorkerFailPolicyFailsJob(t *testing.T) {
	exec := &fakeExecutor{value: []any{
		map[string]any{
			"id":            "flow-1",
			"type":          "order",
			"state":         "validate",
			"partition_key": "tenant:1",
			"lease_token":   "lease-1",
			"fencing_token": int64(9),
		},
	}}
	client := NewClientWithExecutor(exec)
	workflow := NewWorkflowClient(client).Workflow("order", "validate")
	workflow.State("validate", func(context.Context, WorkflowContext) (Outcome, error) {
		return nil, errors.New("bad input")
	})

	result, err := workflow.Worker("worker-1", []string{"validate"}, WorkerOptions{
		BatchSize:   1,
		ErrorPolicy: ErrorPolicyFail,
	}).RunOnce(context.Background())

	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 1 || result.Applied != 1 {
		t.Fatalf("unexpected worker result: %+v", result)
	}
	if len(exec.calls) != 2 {
		t.Fatalf("expected claim and fail calls, got %d", len(exec.calls))
	}
	if exec.calls[1][0] != "FLOW.FAIL" {
		t.Fatalf("expected FLOW.FAIL, got %#v", exec.calls[1])
	}
}

func TestWorkflowWorkerAppliesFailPolicyToHandlerPanic(t *testing.T) {
	claimed := []any{
		map[string]any{
			"id":            "flow-1",
			"type":          "order",
			"state":         "validate",
			"partition_key": "tenant:1",
			"lease_token":   "lease-1",
			"fencing_token": int64(9),
		},
	}
	exec := &fakeExecutor{values: []any{claimed, []byte("OK")}}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "validate")
	workflow.State("validate", func(context.Context, WorkflowContext) (Outcome, error) {
		panic(errWorkerHandlerPanic)
	})

	result, err := workflow.Worker("worker-1", []string{"validate"}, WorkerOptions{
		BatchSize:   1,
		ErrorPolicy: ErrorPolicyFail,
	}).RunOnce(context.Background())

	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 1 || result.Applied != 1 {
		t.Fatalf("unexpected worker result: %+v", result)
	}
	if len(exec.calls) != 2 || exec.calls[1][0] != "FLOW.FAIL" {
		t.Fatalf("panic did not follow fail policy: %#v", exec.calls)
	}
	if got := asString(exec.calls[1][indexOf(exec.calls[1], "ERROR")+1]); !strings.Contains(got, errWorkerHandlerPanic.Error()) {
		t.Fatalf("fail error = %q; want panic cause", got)
	}
}

func TestWorkflowInstallsFIFOStatePolicyAndRejectsPriorityTransition(t *testing.T) {
	exec := &fakeExecutor{values: []any{
		policySnapshotResponse("order", 1, map[string]any{"states": map[string]any{
			"ready": map[string]any{"mode": "fifo"},
		}}),
		[]any{map[string]any{
			"id":            "flow-1",
			"type":          "order",
			"state":         "created",
			"partition_key": "tenant:1",
			"lease_token":   "lease-1",
			"fencing_token": int64(9),
		}},
	}}
	client := NewClientWithExecutor(exec)
	workflow := NewWorkflowClient(client).Workflow("order", "created")
	workflow.State("created", func(context.Context, WorkflowContext) (Outcome, error) {
		return TransitionResult{ToState: "ready", Priority: Int64(1)}, nil
	})
	workflow.State("ready", func(context.Context, WorkflowContext) (Outcome, error) {
		return CompleteWith(map[string]any{"ok": true}), nil
	}, FlowStatePolicy{Mode: FlowStateMode("fifo")})

	if _, err := workflow.InstallPolicy(context.Background(), PolicyOptions{}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(exec.calls[0], []any{"FLOW.POLICY.SET", "order", "REPLACE", "true", "STATE", "ready", "MODE", "FIFO"}) {
		t.Fatalf("unexpected workflow policy call: %#v", exec.calls[0])
	}

	_, err := workflow.Worker("worker-1", []string{"created"}, WorkerOptions{BatchSize: 1}).RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "priority is not supported for fifo state") {
		t.Fatalf("expected fifo priority error, got %v", err)
	}
	if len(exec.calls) != 2 {
		t.Fatalf("expected policy and claim calls only, got %#v", exec.calls)
	}
}

func TestWorkflowWorkerValidatesOptionsBeforeClaiming(t *testing.T) {
	tests := []struct {
		name string
		opts WorkerOptions
	}{
		{name: "negative batch", opts: WorkerOptions{BatchSize: -1}},
		{name: "negative concurrency", opts: WorkerOptions{Concurrency: -1}},
		{name: "negative lease", opts: WorkerOptions{LeaseMS: -1}},
		{name: "invalid error policy", opts: WorkerOptions{ErrorPolicy: ErrorPolicy(99)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []any{}}
			workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "ready")
			workflow.State("ready", func(context.Context, WorkflowContext) (Outcome, error) {
				return CompleteWith(nil), nil
			})

			if _, err := workflow.Worker("worker-1", []string{"ready"}, test.opts).RunOnce(context.Background()); err == nil {
				t.Fatal("invalid workflow worker options were accepted")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid options reached executor: %#v", exec.calls)
			}
		})
	}
}
