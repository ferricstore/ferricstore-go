package ferricstore

import (
	"errors"
	"strings"
)

func normalizeSpawnChildrenOptions(opt SpawnChildrenOptions) SpawnChildrenOptions {
	if opt.GroupID == "" {
		opt.GroupID = "default"
	}
	if opt.Wait == "" {
		opt.Wait = "all"
	} else {
		opt.Wait = canonicalAdminEnum(opt.Wait)
	}
	if opt.OnChildFailed != "" {
		opt.OnChildFailed = canonicalAdminEnum(opt.OnChildFailed)
	}
	if opt.OnParentClosed != "" {
		opt.OnParentClosed = canonicalAdminEnum(opt.OnParentClosed)
	}
	return opt
}

func validateSpawnChildrenOptions(opt SpawnChildrenOptions) error {
	for _, field := range []struct{ name, value string }{
		{name: "flow parent id", value: opt.ParentID},
		{name: "flow partition key", value: opt.PartitionKey},
		{name: "flow group id", value: opt.GroupID},
		{name: "flow child success state", value: opt.Success},
		{name: "flow child failure state", value: opt.Failure},
	} {
		if err := validateFlowMutationText(field.name, field.value); err != nil {
			return err
		}
	}
	if strings.HasPrefix(opt.GroupID, "__") {
		return errors.New("flow group id is reserved")
	}
	if opt.FencingToken == nil || *opt.FencingToken < 0 {
		return errors.New("flow fencing token must be a non-negative integer")
	}
	if opt.NowMS < 0 {
		return errors.New("flow now milliseconds must be non-negative")
	}
	if len(opt.Children) == 0 {
		return errors.New("flow children must be a non-empty list")
	}
	if err := validateFlowBatchSize(len(opt.Children)); err != nil {
		return err
	}
	switch opt.Wait {
	case "all", "any", "none":
	default:
		return errors.New("flow wait must be all, any, or none")
	}
	if opt.OnChildFailed != "" && opt.OnChildFailed != "fail_parent" && opt.OnChildFailed != "ignore" {
		return errors.New("flow on_child_failed must be fail_parent or ignore")
	}
	if opt.OnParentClosed != "" && opt.OnParentClosed != "cancel_children" && opt.OnParentClosed != "abandon_children" {
		return errors.New("flow on_parent_closed must be cancel_children or abandon_children")
	}
	if err := validateNamedValues(NamedValues{Values: opt.Values, ValueRefs: opt.ValueRefs}); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(opt.Children))
	for _, child := range opt.Children {
		if err := validateFlowMutationText("flow child id", child.ID); err != nil {
			return err
		}
		if err := validateFlowMutationText("flow child type", child.Type); err != nil {
			return err
		}
		if child.ID == opt.ParentID {
			return errors.New("flow child id must differ from parent id")
		}
		if _, exists := seen[child.ID]; exists {
			return errors.New("flow duplicate id in batch")
		}
		seen[child.ID] = struct{}{}
		if len(child.Attributes) != 0 {
			return errors.New("SpawnChildren does not support per-child attributes")
		}
		if err := validateNamedValues(NamedValues{Values: child.Values, ValueRefs: child.ValueRefs}); err != nil {
			return err
		}
	}
	return nil
}
