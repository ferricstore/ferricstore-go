package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// WaitForChildrenOptions configures a WaitForChildren outcome.
type WaitForChildrenOptions struct {
	// GroupID names the child group; it defaults to "default".
	GroupID string
	// Wait is "all" (the default) or "any".
	Wait string
	// WaitState is the state the parent waits in, without a lease.
	WaitState string
	// Success is the state the parent moves to once the group resolves.
	Success string
	// Failure is the state the parent moves to when a child fails, unless
	// OnChildFailed is "ignore".
	Failure        string
	OnChildFailed  string
	OnParentClosed string
	MaxActiveMS    any
	Values         map[string]any
	ValueRefs      map[string]string
}

// WaitForChildrenResult creates child Flows and parks the parent in WaitState
// in one fenced command. FerricStore moves the parent to Success or Failure
// when the group resolves; read the children with WorkflowContext.ChildResults.
type WaitForChildrenResult struct {
	Children []ChildSpec
	WaitForChildrenOptions
}

func (WaitForChildrenResult) kind() string { return "wait_for_children" }

// WaitForChildren returns the outcome that fans out to children and waits for
// them. A handler returns it instead of calling SpawnChildren itself, so a
// fast child cannot finish before the parent is parked.
func WaitForChildren(children []ChildSpec, opt WaitForChildrenOptions) WaitForChildrenResult {
	return WaitForChildrenResult{Children: slices.Clone(children), WaitForChildrenOptions: opt}
}

func (r WaitForChildrenResult) validate() error {
	if len(r.Children) == 0 {
		return errors.New("wait for children requires at least one child")
	}
	for _, field := range []struct{ name, value string }{
		{name: "WaitState", value: r.WaitState},
		{name: "Success", value: r.Success},
		{name: "Failure", value: r.Failure},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("wait for children requires %s", field.name)
		}
	}
	switch canonicalAdminEnum(r.Wait) {
	case "", "all", "any":
		return nil
	default:
		return errors.New("wait for children Wait must be all or any")
	}
}

func (w *WorkflowWorker) applyWaitForChildren(ctx context.Context, job FlowRecord, value WaitForChildrenResult) error {
	if err := value.validate(); err != nil {
		return err
	}
	_, err := w.workflow.client.SpawnChildren(ctx, SpawnChildrenOptions{
		ID:             job.ID,
		Children:       value.Children,
		PartitionKey:   job.PartitionKey,
		LeaseToken:     job.LeaseToken,
		FencingToken:   Int64(job.FencingToken),
		GroupID:        value.GroupID,
		Wait:           value.Wait,
		WaitState:      value.WaitState,
		Success:        value.Success,
		Failure:        value.Failure,
		OnChildFailed:  value.OnChildFailed,
		OnParentClosed: value.OnParentClosed,
		MaxActiveMS:    value.MaxActiveMS,
		Values:         value.Values,
		ValueRefs:      value.ValueRefs,
	})
	return err
}

// ChildResults reads one of this Flow's child groups. It returns an error when
// the Flow has no group with that id; an empty groupID means "default".
func (c WorkflowContext) ChildResults(ctx context.Context, groupID string) (*ChildGroupResults, error) {
	job := c.currentJob()
	if groupID == "" {
		groupID = "default"
	}
	group, err := c.Client.ChildResults(ctx, job.ID, ChildResultsOptions{PartitionKey: job.PartitionKey, GroupID: groupID})
	if err != nil {
		return nil, err
	}
	if group == nil {
		return nil, fmt.Errorf("flow %q has no child group %q", job.ID, groupID)
	}
	return group, nil
}
