package ferricstore

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func waitForChildrenTestJob() FlowRecord {
	return FlowRecord{
		ID: "report-1", Type: "report", State: "running", RunState: "split",
		PartitionKey: "tenant:1", LeaseToken: "lease-1", FencingToken: 9,
	}
}

func waitForChildrenTestChildren() []ChildSpec {
	return []ChildSpec{
		{ID: "report-1:part-1", Type: "report-part", Payload: map[string]any{"part": 1}},
		{ID: "report-1:part-2", Type: "report-part", Payload: map[string]any{"part": 2}},
	}
}

func TestWorkflowWorkerWaitForChildrenSpawnsWithTheClaim(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec, WithCodec(JSONCodec{}))).Workflow("report", "split")
	worker := workflow.Worker("worker-1", []string{"split"}, WorkerOptions{})

	err := worker.apply(context.Background(), waitForChildrenTestJob(), "split", func(context.Context, WorkflowContext) (Outcome, error) {
		return WaitForChildren(waitForChildrenTestChildren(), WaitForChildrenOptions{
			GroupID:        "parts",
			WaitState:      "waiting_for_parts",
			Success:        "join",
			Failure:        "failed",
			OnChildFailed:  "ignore",
			OnParentClosed: "abandon_children",
		}), nil
	}, ErrorPolicyRetry)
	if err != nil {
		t.Fatalf("apply wait for children: %v", err)
	}

	if len(exec.calls) != 1 {
		t.Fatalf("calls = %#v; want one FLOW.SPAWN_CHILDREN", exec.calls)
	}
	call := exec.calls[0]
	assertCommandPrefix(t, call, []any{"FLOW.SPAWN_CHILDREN", "report-1", "GROUP", "parts", "WAIT", "all"})
	assertCommandPair(t, call, "PARTITION", "tenant:1")
	assertCommandPair(t, call, "LEASE_TOKEN", "lease-1")
	assertCommandPair(t, call, "FENCING", int64(9))
	assertCommandPair(t, call, "WAIT_STATE", "waiting_for_parts")
	assertCommandPair(t, call, "SUCCESS", "join")
	assertCommandPair(t, call, "FAILURE", "failed")
	assertCommandPair(t, call, "ON_CHILD_FAILED", "ignore")
	assertCommandPair(t, call, "ON_PARENT_CLOSED", "abandon_children")
	if indexOf(call, "report-1:part-1") < 0 || indexOf(call, "report-1:part-2") < 0 {
		t.Fatalf("command %#v does not carry both children", call)
	}
}

func TestWorkflowWorkerWaitForChildrenUsesTheRefreshedClaimAfterAStep(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("report", "split")
	worker := workflow.Worker("worker-1", []string{"split"}, WorkerOptions{})
	refreshed := waitForChildrenTestJob()
	refreshed.LeaseToken = "lease-2"
	refreshed.FencingToken = 10

	err := worker.apply(context.Background(), waitForChildrenTestJob(), "split", func(_ context.Context, ctx WorkflowContext) (Outcome, error) {
		ctx.state.store(refreshed)
		return &WaitForChildrenResult{
			Children:               waitForChildrenTestChildren(),
			WaitForChildrenOptions: WaitForChildrenOptions{WaitState: "waiting", Success: "join", Failure: "failed"},
		}, nil
	}, ErrorPolicyRetry)
	if err != nil {
		t.Fatalf("apply wait for children: %v", err)
	}
	assertCommandPair(t, exec.calls[0], "LEASE_TOKEN", "lease-2")
	assertCommandPair(t, exec.calls[0], "FENCING", int64(10))
	assertCommandPair(t, exec.calls[0], "GROUP", "default")
}

func TestWaitForChildrenRejectsIncompleteOptionsBeforeSpawning(t *testing.T) {
	children := waitForChildrenTestChildren()
	states := WaitForChildrenOptions{WaitState: "waiting", Success: "join", Failure: "failed"}
	tests := []struct {
		name     string
		children []ChildSpec
		options  WaitForChildrenOptions
		message  string
	}{
		{name: "no children", children: nil, options: states, message: "at least one child"},
		{name: "no wait state", children: children, options: WaitForChildrenOptions{Success: "join", Failure: "failed"}, message: "WaitState"},
		{name: "no success state", children: children, options: WaitForChildrenOptions{WaitState: "waiting", Failure: "failed"}, message: "Success"},
		{name: "no failure state", children: children, options: WaitForChildrenOptions{WaitState: "waiting", Success: "join"}, message: "Failure"},
		{name: "wait none", children: children, options: WaitForChildrenOptions{WaitState: "waiting", Success: "join", Failure: "failed", Wait: "none"}, message: "all or any"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("report", "split")
			worker := workflow.Worker("worker-1", []string{"split"}, WorkerOptions{})
			err := worker.apply(context.Background(), waitForChildrenTestJob(), "split", func(context.Context, WorkflowContext) (Outcome, error) {
				return WaitForChildren(test.children, test.options), nil
			}, ErrorPolicyReturn)
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("err = %v; want it to mention %q", err, test.message)
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid wait sent commands: %#v", exec.calls)
			}
		})
	}
}

func childGroupTestRecord() map[string]any {
	return map[string]any{
		"id": "report-1", "type": "report", "state": "join", "partition_key": "tenant:1",
		"child_groups": map[string]any{
			"parts": map[string]any{
				"wait":     "all",
				"resolved": "success",
				"children": map[string]any{
					"report-1:part-2": "failed",
					"report-1:part-1": "completed",
					"report-1:part-3": "running",
				},
				"child_partitions": map[string]any{"report-1:part-1": "tenant:1"},
				"results": map[string]any{
					"report-1:part-1": map[string]any{"result_ref": "ref-result-1"},
					"report-1:part-2": map[string]any{"error_ref": "ref-error-2"},
				},
				"summary": map[string]any{"total": int64(3), "completed": int64(1), "failed": int64(1), "cancelled": int64(0)},
			},
		},
	}
}

func TestClientChildResultsReadsTheGroupAndItsValuesInOneRead(t *testing.T) {
	exec := &fakeExecutor{values: []any{
		childGroupTestRecord(),
		[]any{[]byte(`{"rows":3}`), []byte(`{"reason":"timeout"}`)},
	}}
	client := NewClientWithExecutor(exec, WithCodec(JSONCodec{}))

	group, err := client.ChildResults(context.Background(), "report-1", ChildResultsOptions{PartitionKey: "tenant:1", GroupID: "parts"})
	if err != nil {
		t.Fatalf("child results: %v", err)
	}

	assertCommandPrefix(t, exec.calls[0], []any{"FLOW.GET", "report-1"})
	assertCommandPair(t, exec.calls[0], "PARTITION", "tenant:1")
	if !reflect.DeepEqual(exec.calls[1], []any{"FLOW.VALUE.MGET", "ref-result-1", "ref-error-2"}) {
		t.Fatalf("value read = %#v; want refs in child id order", exec.calls[1])
	}
	want := &ChildGroupResults{
		GroupID:  "parts",
		Wait:     "all",
		Resolved: "success",
		Summary:  ChildGroupSummary{Total: 3, Completed: 1, Failed: 1},
		Children: []ChildResult{
			{ID: "report-1:part-1", PartitionKey: "tenant:1", Status: "completed", Result: map[string]any{"rows": float64(3)}},
			{ID: "report-1:part-2", Status: "failed", Error: map[string]any{"reason": "timeout"}},
			{ID: "report-1:part-3", Status: "running"},
		},
	}
	if !reflect.DeepEqual(group, want) {
		t.Fatalf("group = %#v; want %#v", group, want)
	}
}

func TestClientChildResultsReturnsNilForAMissingGroup(t *testing.T) {
	exec := &fakeExecutor{value: childGroupTestRecord()}
	client := NewClientWithExecutor(exec)

	group, err := client.ChildResults(context.Background(), "report-1", ChildResultsOptions{GroupID: "other"})
	if err != nil || group != nil {
		t.Fatalf("missing group = %#v, %v; want nil, nil", group, err)
	}
	if len(exec.calls) != 1 {
		t.Fatalf("missing group read values: %#v", exec.calls)
	}
}

func TestClientChildResultsRejectsAnUnknownChildStatus(t *testing.T) {
	record := childGroupTestRecord()
	record["child_groups"].(map[string]any)["parts"].(map[string]any)["children"] = map[string]any{"report-1:part-1": "paused"}
	client := NewClientWithExecutor(&fakeExecutor{value: record})

	_, err := client.ChildResults(context.Background(), "report-1", ChildResultsOptions{GroupID: "parts"})
	if err == nil || !strings.Contains(err.Error(), "paused") {
		t.Fatalf("err = %v; want unknown child status error", err)
	}
}

func TestWorkflowContextChildResultsReadsTheCurrentFlowAndRequiresTheGroup(t *testing.T) {
	exec := &fakeExecutor{values: []any{
		childGroupTestRecord(),
		[]any{[]byte(`1`), []byte(`2`)},
		childGroupTestRecord(),
	}}
	ctx := WorkflowContext{Client: NewClientWithExecutor(exec, WithCodec(JSONCodec{}))}
	ctx.state = newWorkflowContextState(waitForChildrenTestJob())

	group, err := ctx.ChildResults(context.Background(), "parts")
	if err != nil || group == nil || len(group.Children) != 3 {
		t.Fatalf("child results = %#v, %v", group, err)
	}
	assertCommandPair(t, exec.calls[0], "PARTITION", "tenant:1")

	_, err = ctx.ChildResults(context.Background(), "missing")
	if err == nil || !strings.Contains(err.Error(), `flow "report-1" has no child group "missing"`) {
		t.Fatalf("err = %v; want missing group error", err)
	}
}
