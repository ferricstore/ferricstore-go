package ferricstore

import "errors"

// FerricStore 0.8 defaults flow_max_batch_items to 1,000, but that setting is
// runtime-configurable up to this server-enforced hard ceiling. The SDK must
// not turn the default into a protocol limit.
const maxFlowMutationBatchItemsV080 = 100_000

func validateCreateManyOptions(opt CreateManyOptions) error {
	if err := validatePublicFlowType("flow type", opt.Type); err != nil {
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
	if _, err := canonicalFlowMaxActiveMS(opt.MaxActiveMS); err != nil {
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
		if _, err := canonicalFlowMaxActiveMS(item.MaxActiveMS); err != nil {
			return err
		}
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
	if err := validateFlowExactNonNegative("flow now milliseconds", opt.NowMS); err != nil {
		return err
	}
	if err := validateOptionalFlowDeadline("flow ttl milliseconds", opt.NowMS, opt.TTLMS); err != nil {
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
	if err := validateFencedItems(opt.Items, boolDefault(opt.Independent, false), true); err != nil {
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
	return validateFlowMetadata(nil, opt.AttributesMerge, opt.AttributesDelete, opt.StateMeta)
}

func validateFailManyOptions(opt FailManyOptions) error {
	if err := validateClaimedItems(opt.Items, boolDefault(opt.Independent, false)); err != nil {
		return err
	}
	if err := validateFlowExactNonNegative("flow now milliseconds", opt.NowMS); err != nil {
		return err
	}
	if err := validateOptionalFlowDeadline("flow ttl milliseconds", opt.NowMS, opt.TTLMS); err != nil {
		return err
	}
	if err := validateNamedValues(opt.NamedValues); err != nil {
		return err
	}
	return validateFlowMetadata(nil, opt.AttributesMerge, opt.AttributesDelete, opt.StateMeta)
}

func validateCancelManyOptions(opt CancelManyOptions) error {
	if err := validateFencedItems(opt.Items, boolDefault(opt.Independent, false), false); err != nil {
		return err
	}
	if err := validateFlowExactNonNegative("flow now milliseconds", opt.NowMS); err != nil {
		return err
	}
	if err := validateOptionalFlowDeadline("flow ttl milliseconds", opt.NowMS, opt.TTLMS); err != nil {
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
		if err := validatePublicFlowID("flow item id", item.ID); err != nil {
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
		if err := validatePublicFlowID("flow item id", item.ID); err != nil {
			return err
		}
		if err := validateFlowMutationText("flow item lease token", item.LeaseToken); err != nil {
			return err
		}
		if err := validateFlowExactNonNegative("flow item fencing token", item.FencingToken); err != nil {
			return err
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

func validateFencedItems(items []FencedItem, allowDuplicates, requireLease bool) error {
	if err := validateFlowBatchSize(len(items)); err != nil {
		return err
	}
	var seen map[string]struct{}
	if !allowDuplicates {
		seen = make(map[string]struct{}, len(items))
	}
	for _, item := range items {
		if err := validatePublicFlowID("flow item id", item.ID); err != nil {
			return err
		}
		if err := validateFlowExactNonNegative("flow item fencing token", item.FencingToken); err != nil {
			return err
		}
		if requireLease {
			if err := validateFlowMutationText("flow item lease token", item.LeaseToken); err != nil {
				return err
			}
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
	if count > maxFlowMutationBatchItemsV080 {
		return errors.New("flow batch item count exceeds maximum 100000")
	}
	return nil
}
