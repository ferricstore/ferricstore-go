package ferricstore

import (
	"errors"
	"fmt"
	"strings"
)

const maxFlowClaimPriority int64 = 2

func validateClaimDueOptions(opt ClaimDueOptions) error {
	if opt.State != "" && len(opt.States) > 0 {
		return errors.New("state and states are mutually exclusive")
	}
	if opt.PartitionKey != "" && len(opt.PartitionKeys) > 0 {
		return errors.New("partition key and partition keys are mutually exclusive")
	}
	if opt.IncludeState && !opt.JobOnly {
		return errors.New("include state requires job only")
	}
	if err := validateClaimCommon(
		opt.Type, opt.Worker, opt.LeaseMS, opt.Limit, opt.NowMS, opt.Priority,
		opt.PartitionKeys, opt.Values, opt.PayloadMaxBytes, opt.ValueMaxBytes,
	); err != nil {
		return err
	}
	if opt.BlockMS != nil && *opt.BlockMS < 0 {
		return errors.New("flow block milliseconds must be non-negative")
	}
	if opt.ReclaimRatio != nil && (*opt.ReclaimRatio < 0 || *opt.ReclaimRatio > 100) {
		return errors.New("flow reclaim ratio must be between 0 and 100")
	}
	return validateFlowStateFilters(opt.States)
}

func validateReclaimOptions(opt ReclaimOptions) error {
	if opt.State != "" && opt.State != "running" {
		return errors.New("FLOW.RECLAIM only supports running state")
	}
	if opt.PartitionKey != "" && len(opt.PartitionKeys) > 0 {
		return errors.New("partition key and partition keys are mutually exclusive")
	}
	return validateClaimCommon(
		opt.Type, opt.Worker, opt.LeaseMS, opt.Limit, opt.NowMS, opt.Priority,
		opt.PartitionKeys, opt.Values, opt.PayloadMaxBytes, opt.ValueMaxBytes,
	)
}

func validateClaimCommon(
	flowType, worker string,
	leaseMS int64,
	limit int,
	nowMS int64,
	priority *int64,
	partitionKeys, valueNames []string,
	payloadMaxBytes, valueMaxBytes *int64,
) error {
	if flowType == "" {
		return errors.New("flow type must be a non-empty string")
	}
	if worker == "" {
		return errors.New("flow worker must be a non-empty string")
	}
	if leaseMS < 0 {
		return errors.New("flow lease milliseconds must be positive")
	}
	if limit < 0 {
		return errors.New("flow claim limit must be positive")
	}
	if nowMS < 0 {
		return errors.New("flow now milliseconds must be non-negative")
	}
	if priority != nil && (*priority < 0 || *priority > maxFlowClaimPriority) {
		return fmt.Errorf("flow claim priority must be between 0 and %d", maxFlowClaimPriority)
	}
	if err := validateClaimStrings("partition keys", partitionKeys); err != nil {
		return err
	}
	if err := validateClaimStrings("value names", valueNames); err != nil {
		return err
	}
	if payloadMaxBytes != nil && *payloadMaxBytes < 0 {
		return errors.New("flow payload maximum bytes must be non-negative")
	}
	if valueMaxBytes != nil && *valueMaxBytes < 0 {
		return errors.New("flow value maximum bytes must be non-negative")
	}
	return nil
}

func validateClaimStrings(field string, values []string) error {
	for _, value := range values {
		if value == "" {
			return fmt.Errorf("flow %s must be non-empty strings", field)
		}
	}
	return nil
}

func validateFlowStateFilters(states []string) error {
	hasAnyState := false
	hasExplicitState := false
	for _, state := range states {
		if state == "" {
			return errors.New("flow states must be non-empty strings")
		}
		if strings.EqualFold(state, "ANY") {
			hasAnyState = true
		} else {
			hasExplicitState = true
		}
	}
	if hasAnyState && hasExplicitState {
		return errors.New("flow state ANY cannot be mixed with explicit states")
	}
	return nil
}
