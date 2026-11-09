//go:build integration && !compatibility_floor

package ferricstore

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// Fan-in needs server 0.11.19 or newer: older servers never make a parent
// claimable again once its child group resolves.

func runChildrenWorkflowsUntilTerminal(ctx context.Context, t *testing.T, client *Client, id, partition string, workers ...*WorkflowWorker) *FlowRecord {
	t.Helper()
	for round := 0; round < 100; round++ {
		record, err := client.Get(ctx, id, partition, nil)
		if err != nil {
			t.Fatalf("read %s: %v", id, err)
		}
		if record != nil && (record.State == "completed" || record.State == "failed" || record.State == "cancelled") {
			return record
		}
		for _, worker := range workers {
			if _, err := worker.RunOnce(ctx); err != nil {
				t.Fatalf("worker round %d: %v", round, err)
			}
		}
	}
	t.Fatalf("Flow %s did not finish", id)
	return nil
}

func TestIntegrationWaitForChildrenJoinsAndReadsChildResults(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()
	client := integrationClient(JSONCodec{})
	defer client.Close()

	runID := integrationSuffix("children")
	partition := "go-sdk:children:" + runID
	parentType := "go-sdk-children-parent-" + runID
	childType := "go-sdk-children-child-" + runID
	workflows := NewWorkflowClient(client)
	parent := workflows.Workflow(parentType, "dispatch")
	child := workflows.Workflow(childType, "queued")
	var joined *ChildGroupResults

	parent.State("dispatch", func(_ context.Context, flow WorkflowContext) (Outcome, error) {
		return WaitForChildren([]ChildSpec{
			{ID: flow.ID() + ":ok", Type: childType, Payload: map[string]any{"fail": false}, PartitionKey: partition},
			{ID: flow.ID() + ":bad", Type: childType, Payload: map[string]any{"fail": true}, PartitionKey: partition},
		}, WaitForChildrenOptions{
			GroupID: "lines", WaitState: "waiting", Success: "joined", Failure: "child_failed",
			OnChildFailed: "ignore",
		}), nil
	})
	child.State("queued", func(_ context.Context, flow WorkflowContext) (Outcome, error) {
		payload, _ := flow.Payload().(map[string]any)
		if payload["fail"] == true {
			return FailWith(map[string]any{"reason": "bad input"}), nil
		}
		return CompleteWith(map[string]any{"rows": 7}), nil
	})
	parent.State("joined", func(ctx context.Context, flow WorkflowContext) (Outcome, error) {
		group, err := flow.ChildResults(ctx, "lines")
		if err != nil {
			return nil, err
		}
		joined = group
		return CompleteWith(map[string]any{"children": len(group.Children)}), nil
	})
	parent.State("child_failed", func(context.Context, WorkflowContext) (Outcome, error) {
		return FailWith("the group must ignore failed children"), nil
	})

	id := "go-sdk:children:" + runID
	if _, err := parent.Start(ctx, id, nil, CreateOptions{PartitionKey: partition}); err != nil {
		t.Fatalf("start parent: %v", err)
	}
	record := runChildrenWorkflowsUntilTerminal(ctx, t, client, id, partition,
		parent.Worker("go-sdk-children-parent", []string{"dispatch", "joined", "child_failed"}, WorkerOptions{PartitionKey: partition}),
		child.Worker("go-sdk-children-child", []string{"queued"}, WorkerOptions{PartitionKey: partition}),
	)

	if record.State != "completed" || joined == nil {
		t.Fatalf("parent = %s, joined = %#v; want completed through joined", record.State, joined)
	}
	if joined.Wait != "all" || joined.Resolved != "success" || joined.Summary != (ChildGroupSummary{Total: 2, Completed: 1, Failed: 1}) {
		t.Fatalf("group = %#v", joined)
	}
	bad, good := joined.Children[0], joined.Children[1]
	badError, _ := bad.Error.(map[string]any)
	if bad.ID != id+":bad" || bad.Status != "failed" || badError["reason"] != "bad input" {
		t.Fatalf("failed child = %#v", bad)
	}
	goodResult, _ := good.Result.(map[string]any)
	if good.ID != id+":ok" || good.Status != "completed" || goodResult["rows"] != float64(7) || good.PartitionKey != partition {
		t.Fatalf("completed child = %#v", good)
	}

	missing, err := client.ChildResults(ctx, id, ChildResultsOptions{PartitionKey: partition, GroupID: "absent"})
	if err != nil || missing != nil {
		t.Fatalf("absent group = %#v, %v; want nil", missing, err)
	}
}

func TestIntegrationFanoutExampleRuns(t *testing.T) {
	if integrationUsesHTTP() {
		t.Skip("the fan-out example uses the native protocol")
	}
	ctx, cancel := integrationContext(t)
	defer cancel()

	command := exec.CommandContext(ctx, "go", "run", "./examples/fanout", "report:"+integrationSuffix("example"))
	command.Env = append(os.Environ(), "FERRICSTORE_ADDR="+integrationAddress())
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("fan-out example failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "report completed: 2 parts, 20 rows") {
		t.Fatalf("fan-out example output = %q", output)
	}
}
