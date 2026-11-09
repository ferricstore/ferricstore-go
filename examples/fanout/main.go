// Fan out one child Flow per report part, wait for all of them, then read
// their results.
//
// The report Flow waits in "waiting_for_parts" without holding a lease or a
// worker. FerricStore moves it to "join" once every part completes, or to
// "failed" at the first failed part.
package main

import (
	"context"
	"fmt"
	"os"

	ferricstore "github.com/ferricstore/ferricstore-go"
)

var terminal = map[string]bool{"completed": true, "failed": true, "cancelled": true}

func main() {
	ctx := context.Background()
	addr := os.Getenv("FERRICSTORE_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6388"
	}
	reportID := "report:1"
	if len(os.Args) > 1 {
		reportID = os.Args[1]
	}
	client := ferricstore.NewClient(addr, ferricstore.WithCodec(ferricstore.JSONCodec{}))
	defer func() { _ = client.Close() }()

	workflows := ferricstore.NewWorkflowClient(client)
	report := workflows.Workflow("report", "split")
	part := workflows.Workflow("report-part", "queued")
	var summary string

	// Creating the parts and parking the report is one fenced command.
	report.State("split", func(_ context.Context, flow ferricstore.WorkflowContext) (ferricstore.Outcome, error) {
		return ferricstore.WaitForChildren([]ferricstore.ChildSpec{
			{ID: flow.ID() + ":part:1", Type: "report-part", Payload: map[string]any{"part": 1}},
			{ID: flow.ID() + ":part:2", Type: "report-part", Payload: map[string]any{"part": 2}},
		}, ferricstore.WaitForChildrenOptions{
			GroupID:   "parts",
			WaitState: "waiting_for_parts",
			Success:   "join",
			Failure:   "failed",
		}), nil
	})

	// Each part is an ordinary Flow with its own lease, retries, and history.
	part.State("queued", func(_ context.Context, flow ferricstore.WorkflowContext) (ferricstore.Outcome, error) {
		payload, _ := flow.Payload().(map[string]any)
		return ferricstore.CompleteWith(map[string]any{"part": payload["part"], "rows": 10}), nil
	})

	report.State("join", func(ctx context.Context, flow ferricstore.WorkflowContext) (ferricstore.Outcome, error) {
		parts, err := flow.ChildResults(ctx, "parts")
		if err != nil {
			return nil, err
		}
		rows := 0.0
		for _, child := range parts.Children {
			result, _ := child.Result.(map[string]any)
			count, _ := result["rows"].(float64)
			rows += count
		}
		summary = fmt.Sprintf("%d parts, %.0f rows", len(parts.Children), rows)
		return ferricstore.CompleteWith(map[string]any{"parts": len(parts.Children), "rows": rows}), nil
	})

	report.State("failed", func(ctx context.Context, flow ferricstore.WorkflowContext) (ferricstore.Outcome, error) {
		parts, err := flow.ChildResults(ctx, "parts")
		if err != nil {
			return nil, err
		}
		var failed []string
		for _, child := range parts.Children {
			if child.Status == "failed" {
				failed = append(failed, child.ID)
			}
		}
		summary = fmt.Sprintf("failed parts: %v", failed)
		return ferricstore.FailWith(map[string]any{"failed": failed}), nil
	})

	_, err := report.Start(ctx, reportID, nil, ferricstore.CreateOptions{Idempotent: ferricstore.Bool(true)})
	if err != nil {
		fmt.Fprintln(os.Stderr, "report start failed:", err)
		os.Exit(1)
	}

	// Production workers are long-running loops, usually in separate services.
	// This example drives both in one process until the report is terminal.
	reportWorker := report.Worker("report-worker-1", []string{"split", "join", "failed"}, ferricstore.WorkerOptions{})
	partWorker := part.Worker("part-worker-1", []string{"queued"}, ferricstore.WorkerOptions{})
	for round := 0; round < 100; round++ {
		record, err := client.Get(ctx, reportID, "", nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, "report read failed:", err)
			os.Exit(1)
		}
		if record != nil && terminal[record.State] {
			fmt.Printf("report %s: %s\n", record.State, summary)
			return
		}
		for _, worker := range []*ferricstore.WorkflowWorker{reportWorker, partWorker} {
			if _, err := worker.RunOnce(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "worker failed:", err)
				os.Exit(1)
			}
		}
	}
	fmt.Fprintln(os.Stderr, "report did not finish")
	os.Exit(1)
}
