package ferricstore

import "errors"

const maxFlowBatchItems = 1_000

func validateCreateManyOptions(opt CreateManyOptions) error {
	if err := validateFlowMutationText("flow type", opt.Type); err != nil {
		return err
	}
	if opt.State != "" {
		if err := validateFlowMutationText("flow state", opt.State); err != nil {
			return err
		}
	}
	if err := validateCreateMutationFields(opt.NowMS, opt.RunAtMS, opt.Priority, opt.RetentionTTLMS); err != nil {
		return err
	}
	if err := validateFlowBatchSize(len(opt.Items)); err != nil {
		return err
	}
	if err := validateCreateItemIDs(opt.Items, !boolDefault(opt.Independent, false)); err != nil {
		return err
	}
	if err := validateNamedValues(NamedValues{Values: opt.Values, ValueRefs: opt.ValueRefs}); err != nil {
		return err
	}
	if err := validateFlowMetadata(opt.Attributes, nil, nil, opt.StateMeta); err != nil {
		return err
	}
	for _, item := range opt.Items {
		if err := validateNamedValues(NamedValues{Values: item.Values, ValueRefs: item.ValueRefs}); err != nil {
			return err
		}
		if err := validateFlowMetadata(item.Attributes, nil, nil, item.StateMeta); err != nil {
			return err
		}
	}
	return nil
}

func validateCompleteManyOptions(opt CompleteManyOptions) error {
	if err := validateClaimedItems(opt.Items, boolDefault(opt.Independent, false)); err != nil {
		return err
	}
	if opt.NowMS < 0 {
		return errors.New("flow now milliseconds must be non-negative")
	}
	if err := validateOptionalPositiveInt64("flow ttl milliseconds", opt.TTLMS); err != nil {
		return err
	}
	if err := validateNamedValues(opt.NamedValues); err != nil {
		return err
	}
	return validateFlowMetadata(nil, opt.AttributesMerge, opt.AttributesDelete, opt.StateMeta)
}

func validateTransitionManyOptions(opt TransitionManyOptions) error {
	if err := validateFlowMutationText("flow from state", opt.FromState); err != nil {
		return err
	}
	if err := validateFlowMutationText("flow to state", opt.ToState); err != nil {
		return err
	}
	if opt.ToState == "running" {
		return errors.New("flow running state is only entered by ClaimDue")
	}
	if err := validateFencedItems(opt.Items, boolDefault(opt.Independent, false)); err != nil {
		return err
	}
	if err := validateFlowMutationTimes(opt.NowMS, opt.RunAtMS); err != nil {
		return err
	}
	if err := validateFlowMutationPriority(opt.Priority); err != nil {
		return err
	}
	if err := validateNamedValues(opt.NamedValues); err != nil {
		return err
	}
	return validateFlowMetadata(nil, opt.AttributesMerge, opt.AttributesDelete, opt.StateMeta)
}

func validateRetryManyOptions(opt RetryManyOptions) error {
	if err := validateClaimedItems(opt.Items, boolDefault(opt.Independent, false)); err != nil {
		return err
	}
	if err := validateFlowMutationTimes(opt.NowMS, opt.RunAtMS); err != nil {
		return err
	}
	if hasNamedValueMutation(opt.NamedValues) {
		return errors.New("FLOW.RETRY_MANY does not support named value mutations")
	}
	return validateFlowMetadata(nil, opt.AttributesMerge, opt.AttributesDelete, opt.StateMeta)
}

func validateFailManyOptions(opt FailManyOptions) error {
	if err := validateClaimedItems(opt.Items, boolDefault(opt.Independent, false)); err != nil {
		return err
	}
	if opt.NowMS < 0 {
		return errors.New("flow now milliseconds must be non-negative")
	}
	if err := validateOptionalPositiveInt64("flow ttl milliseconds", opt.TTLMS); err != nil {
		return err
	}
	if err := validateNamedValues(opt.NamedValues); err != nil {
		return err
	}
	return validateFlowMetadata(nil, opt.AttributesMerge, opt.AttributesDelete, opt.StateMeta)
}

func validateCancelManyOptions(opt CancelManyOptions) error {
	if err := validateFencedItems(opt.Items, boolDefault(opt.Independent, false)); err != nil {
		return err
	}
	if opt.NowMS < 0 {
		return errors.New("flow now milliseconds must be non-negative")
	}
	if err := validateOptionalPositiveInt64("flow ttl milliseconds", opt.TTLMS); err != nil {
		return err
	}
	if err := validateNamedValues(opt.NamedValues); err != nil {
		return err
	}
	return validateFlowMetadata(nil, opt.AttributesMerge, opt.AttributesDelete, opt.StateMeta)
}

func validateCreateItemIDs(items []CreateItem, unique bool) error {
	var seen map[string]struct{}
	if unique {
		seen = make(map[string]struct{}, len(items))
	}
	for _, item := range items {
		if err := validateFlowMutationText("flow item id", item.ID); err != nil {
			return err
		}
		if unique {
			if _, exists := seen[item.ID]; exists {
				return errors.New("flow duplicate id in batch")
			}
			seen[item.ID] = struct{}{}
		}
	}
	return nil
}

func validateClaimedItems(items []ClaimedItem, allowDuplicates bool) error {
	if err := validateFlowBatchSize(len(items)); err != nil {
		return err
	}
	var seen map[string]struct{}
	if !allowDuplicates {
		seen = make(map[string]struct{}, len(items))
	}
	for _, item := range items {
		if err := validateFlowMutationText("flow item id", item.ID); err != nil {
			return err
		}
		if err := validateFlowMutationText("flow item lease token", item.LeaseToken); err != nil {
			return err
		}
		if item.FencingToken < 0 {
			return errors.New("flow item fencing token must be non-negative")
		}
		if !allowDuplicates {
			if _, exists := seen[item.ID]; exists {
				return errors.New("flow duplicate id in batch")
			}
			seen[item.ID] = struct{}{}
		}
	}
	return nil
}

func validateFencedItems(items []FencedItem, allowDuplicates bool) error {
	if err := validateFlowBatchSize(len(items)); err != nil {
		return err
	}
	var seen map[string]struct{}
	if !allowDuplicates {
		seen = make(map[string]struct{}, len(items))
	}
	for _, item := range items {
		if err := validateFlowMutationText("flow item id", item.ID); err != nil {
			return err
		}
		if item.FencingToken < 0 {
			return errors.New("flow item fencing token must be non-negative")
		}
		if !allowDuplicates {
			if _, exists := seen[item.ID]; exists {
				return errors.New("flow duplicate id in batch")
			}
			seen[item.ID] = struct{}{}
		}
	}
	return nil
}

func validateFlowBatchSize(count int) error {
	if count > maxFlowBatchItems {
		return errors.New("flow batch item count exceeds maximum 1000")
	}
	return nil
}
