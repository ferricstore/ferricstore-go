package ferricstore

import "errors"

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
	if opt.Type == "" {
		return errors.New("run_steps_many type must be a non-empty string")
	}
	if opt.Worker == "" {
		return errors.New("run_steps_many worker must be a non-empty string")
	}
	if opt.LeaseMS < 0 {
		return errors.New("run_steps_many lease must be positive")
	}
	if opt.NowMS < 0 {
		return errors.New("run_steps_many now must be non-negative")
	}
	if opt.RetentionTTLMS != nil && *opt.RetentionTTLMS <= 0 {
		return errors.New("run_steps_many retention ttl must be positive")
	}
	for _, state := range opt.States {
		if state == "" {
			return errors.New("run_steps_many states must be non-empty strings")
		}
	}
	seen := make(map[string]struct{}, len(opt.Items))
	for _, item := range opt.Items {
		if item.ID == "" {
			return errors.New("run_steps_many item ids must be non-empty strings")
		}
		if _, exists := seen[item.ID]; exists {
			return errors.New("flow duplicate id in batch")
		}
		seen[item.ID] = struct{}{}
	}
	return nil
}
