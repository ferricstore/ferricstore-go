package ferricstore

import (
	"context"
	"fmt"
	"slices"
)

// ChildResultsOptions selects the parent's partition and child group.
type ChildResultsOptions struct {
	PartitionKey string
	// GroupID defaults to "default".
	GroupID string
	// MaxBytes bounds each returned result or error value.
	MaxBytes *int64
}

// ChildResult is one child of a group: its status and, once terminal, its
// decoded result or error.
type ChildResult struct {
	ID           string
	PartitionKey string
	// Status is "running", "completed", "failed", or "cancelled".
	Status string
	Result any
	Error  any
}

// ChildGroupSummary counts a group's children by outcome.
type ChildGroupSummary struct {
	Total     int64
	Completed int64
	Failed    int64
	Cancelled int64
}

// ChildGroupResults is a parent's child group, with children ordered by id.
type ChildGroupResults struct {
	GroupID string
	// Wait is "all", "any", or "none".
	Wait string
	// Resolved is "success" or "failure" once the group resolved, and empty
	// while it waits.
	Resolved string
	Summary  ChildGroupSummary
	Children []ChildResult
}

var childResultStatuses = []string{"running", "completed", "failed", "cancelled"}

// ChildResults reads one child group from the parent record, then every
// child's stored result or error in one FLOW.VALUE.MGET. It returns nil when
// the parent or the group does not exist.
func (c *Client) ChildResults(ctx context.Context, parentID string, opt ChildResultsOptions) (*ChildGroupResults, error) {
	if err := validateFlowGet(parentID, nil); err != nil {
		return nil, err
	}
	groupID := opt.GroupID
	if groupID == "" {
		groupID = "default"
	}
	args := []any{"FLOW.GET", parentID}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	value, err := c.typedReply(ctx, args...)
	if err != nil || value == nil || isOK(value) {
		return nil, err
	}
	record, err := nativeMap(value)
	if err != nil {
		return nil, err
	}
	group, err := childGroupField(record["child_groups"], groupID)
	if err != nil || group == nil {
		return nil, err
	}
	results, err := readChildGroup(groupID, group)
	if err != nil {
		return nil, err
	}
	return results, c.readChildValues(ctx, group, results, opt.MaxBytes)
}

func readChildGroup(groupID string, group map[string]any) (*ChildGroupResults, error) {
	wait := asString(group["wait"])
	if !slices.Contains([]string{"all", "any", "none"}, wait) {
		return nil, fmt.Errorf("FLOW.GET child group returned unknown wait %q", wait)
	}
	resolved := asString(group["resolved"])
	if resolved != "" && resolved != "success" && resolved != "failure" {
		return nil, fmt.Errorf("FLOW.GET child group returned unknown resolution %q", resolved)
	}
	summary, err := childGroupSummary(group["summary"])
	if err != nil {
		return nil, err
	}
	statuses, err := childGroupMap(group["children"], "children")
	if err != nil {
		return nil, err
	}
	partitions, err := childGroupMap(group["child_partitions"], "child partitions")
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(statuses))
	for id := range statuses {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	children := make([]ChildResult, 0, len(ids))
	for _, id := range ids {
		status := asString(statuses[id])
		if !slices.Contains(childResultStatuses, status) {
			return nil, fmt.Errorf("FLOW.GET child group returned unknown child status %q", status)
		}
		children = append(children, ChildResult{ID: id, PartitionKey: asString(partitions[id]), Status: status})
	}
	return &ChildGroupResults{GroupID: groupID, Wait: wait, Resolved: resolved, Summary: summary, Children: children}, nil
}

// readChildValues fills each terminal child's Result or Error from its stored
// value reference.
func (c *Client) readChildValues(ctx context.Context, group map[string]any, results *ChildGroupResults, maxBytes *int64) error {
	terminal, err := childGroupMap(group["results"], "results")
	if err != nil {
		return err
	}
	var refs []string
	var targets []*any
	for index := range results.Children {
		child := &results.Children[index]
		if terminal[child.ID] == nil {
			continue
		}
		entry, err := childGroupMap(terminal[child.ID], "result")
		if err != nil {
			return err
		}
		for _, target := range []struct {
			field string
			value *any
		}{{field: "result_ref", value: &child.Result}, {field: "error_ref", value: &child.Error}} {
			if ref := asString(entry[target.field]); ref != "" {
				refs = append(refs, ref)
				targets = append(targets, target.value)
			}
		}
	}
	values, err := c.ValueMGet(ctx, refs, maxBytes)
	if err != nil {
		return err
	}
	for index, value := range values {
		*targets[index] = value
	}
	return nil
}

func childGroupField(groups any, groupID string) (map[string]any, error) {
	if groups == nil {
		return nil, nil
	}
	byID, err := childGroupMap(groups, "child groups")
	if err != nil || byID[groupID] == nil {
		return nil, err
	}
	return childGroupMap(byID[groupID], "child group")
}

func childGroupMap(value any, what string) (map[string]any, error) {
	if value == nil {
		return map[string]any{}, nil
	}
	mapped, err := nativeMap(value)
	if err != nil {
		return nil, fmt.Errorf("FLOW.GET %s returned an unexpected response: %w", what, err)
	}
	return mapped, nil
}

func childGroupSummary(value any) (ChildGroupSummary, error) {
	counts, err := childGroupMap(value, "child group summary")
	if err != nil {
		return ChildGroupSummary{}, err
	}
	summary := ChildGroupSummary{
		Total:     asInt64(counts["total"]),
		Completed: asInt64(counts["completed"]),
		Failed:    asInt64(counts["failed"]),
		Cancelled: asInt64(counts["cancelled"]),
	}
	if summary.Total < 0 || summary.Completed < 0 || summary.Failed < 0 || summary.Cancelled < 0 {
		return ChildGroupSummary{}, fmt.Errorf("FLOW.GET child group returned a negative count: %+v", summary)
	}
	return summary, nil
}
