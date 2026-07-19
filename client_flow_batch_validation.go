package ferricstore

import (
	"errors"
	"fmt"
)

func validateRunStepsManyOptions(opt RunStepsManyOptions) error {
	if err := validateFlowBatchSize(len(opt.Items)); err != nil {
		return err
	}
	if (len(opt.States) == 0) == (opt.Steps == 0) {
		return errors.New("run_steps_many requires exactly one of states or steps")
	}
	if opt.Steps < 0 {
		return errors.New("run_steps_many steps must be positive")
	}
	if err := validatePublicFlowType("run_steps_many type", opt.Type); err != nil {
		return err
	}
	if opt.Worker == "" {
		return errors.New("run_steps_many worker must be a non-empty string")
	}
	leaseMS := opt.LeaseMS
	if leaseMS == 0 {
		leaseMS = 30_000
	}
	if err := validateFlowDeadline("run_steps_many lease", opt.NowMS, leaseMS); err != nil {
		return err
	}
	if opt.RetentionTTLMS != nil && *opt.RetentionTTLMS <= 0 {
		return errors.New("run_steps_many retention ttl must be positive")
	}
	for _, state := range opt.States {
		if state == "" {
			return errors.New("run_steps_many states must be non-empty strings")
		}
	}
	stepCount := opt.Steps
	if len(opt.States) != 0 {
		stepCount = len(opt.States)
	}
	if len(opt.Items) > 0 && stepCount > maxFlowMutationBatchItemsV080/len(opt.Items) {
		return fmt.Errorf("flow run step operation count exceeds maximum %d", maxFlowMutationBatchItemsV080)
	}
	seen := make(map[string]struct{}, len(opt.Items))
	for _, item := range opt.Items {
		if err := validatePublicFlowID("run_steps_many item id", item.ID); err != nil {
			return err
		}
		if _, exists := seen[item.ID]; exists {
			return errors.New("flow duplicate id in batch")
		}
		seen[item.ID] = struct{}{}
	}
	return nil
}
