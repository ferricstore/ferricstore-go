package ferricstore

import "errors"

func validateFlowWakeSubscriptionOptions(opt FlowWakeSubscriptionOptions) error {
	if opt.Type == "" {
		return errors.New("FLOW_WAKE type must be a non-empty string")
	}
	if opt.State != "" && len(opt.States) > 0 {
		return errors.New("state and states are mutually exclusive")
	}
	if opt.States != nil && len(opt.States) == 0 {
		return errors.New("FLOW_WAKE states must be a non-empty list")
	}
	if err := validateFlowStateFilters(opt.States); err != nil {
		return err
	}
	if opt.PartitionKey != "" && len(opt.PartitionKeys) > 0 {
		return errors.New("partition_key and partition_keys are mutually exclusive")
	}
	if opt.PartitionKeys != nil && len(opt.PartitionKeys) == 0 {
		return errors.New("FLOW_WAKE partition keys must be a non-empty list")
	}
	if err := validateClaimStrings("partition keys", opt.PartitionKeys); err != nil {
		return err
	}
	if opt.Priority != nil && (*opt.Priority < 0 || *opt.Priority > maxFlowClaimPriority) {
		return errors.New("FLOW_WAKE priority must be between 0 and 2")
	}
	if opt.Limit != nil && *opt.Limit <= 0 {
		return errors.New("FLOW_WAKE limit must be positive")
	}
	return nil
}
