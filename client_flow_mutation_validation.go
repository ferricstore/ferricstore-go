package ferricstore

import (
	"errors"
	"fmt"
)

const maxFlowMutationPriority int64 = 2

func validateCreateOptions(opt CreateOptions) error {
	if err := validatePublicFlowID("flow id", opt.ID); err != nil {
		return err
	}
	if err := validatePublicFlowType("flow type", opt.Type); err != nil {
		return err
	}
	if opt.State != "" {
		if err := validateFlowMutationText("flow state", opt.State); err != nil {
			return err
		}
	}
	for _, field := range []struct{ name, value string }{
		{name: "flow parent_flow_id", value: opt.ParentFlowID},
		{name: "flow root_flow_id", value: opt.RootFlowID},
	} {
		if err := validateOptionalPublicFlowIDReference(field.name, field.value); err != nil {
			return err
		}
	}
	if err := validateFlowReference("flow correlation_id", opt.CorrelationID); err != nil {
		return err
	}
	if err := validateCreateMutationFields(opt.NowMS, opt.RunAtMS, opt.Priority, opt.RetentionTTLMS); err != nil {
		return err
	}
	if _, err := canonicalFlowMaxActiveMS(opt.MaxActiveMS); err != nil {
		return err
	}
	if err := validateNamedValues(NamedValues{Values: opt.Values, ValueRefs: opt.ValueRefs}); err != nil {
		return err
	}
	return validateFlowMetadata(opt.Attributes, nil, nil, opt.StateMeta)
}

func validateCreateMutationFields(nowMS, runAtMS int64, priority, retentionTTLMS *int64) error {
	if err := validateFlowMutationTimes(nowMS, runAtMS); err != nil {
		return err
	}
	if err := validateFlowMutationPriority(priority); err != nil {
		return err
	}
	return validateOptionalPositiveInt64("flow retention ttl milliseconds", retentionTTLMS)
}

func validateValuePutOptions(opt ValuePutOptions) error {
	if err := validateFlowExactNonNegative("flow now milliseconds", opt.NowMS); err != nil {
		return err
	}
	if err := validateOptionalPublicFlowIDReference("flow owner_flow_id", opt.OwnerFlowID); err != nil {
		return err
	}
	if err := validateFlowReference("flow value name", opt.Name); err != nil {
		return err
	}
	if opt.Name != "" && opt.OwnerFlowID == "" {
		return errors.New("flow named value put requires owner_flow_id")
	}
	if opt.Override != nil && (opt.OwnerFlowID == "" || opt.Name == "") {
		return errors.New("flow value override requires owner_flow_id and name")
	}
	if opt.OwnerFlowID != "" && opt.Name != "" && opt.TTLMS != nil {
		return errors.New("flow named values inherit retention and do not support ttl")
	}
	return validateOptionalFlowDeadline("flow value ttl milliseconds", opt.NowMS, opt.TTLMS)
}

func validateValueMGet(refs []string, maxBytes *int64) error {
	for _, ref := range refs {
		if err := validateFlowMutationText("flow value ref", ref); err != nil {
			return err
		}
	}
	return validateOptionalNonNegativeInt64("flow value maximum bytes", maxBytes)
}

func validateSignalOptions(opt SignalOptions) error {
	if err := validatePublicFlowID("flow id", opt.ID); err != nil {
		return err
	}
	if err := validateFlowMutationText("flow signal", opt.Signal); err != nil {
		return err
	}
	if err := validateFlowReference("flow idempotency_key", opt.IdempotencyKey); err != nil {
		return err
	}
	for _, state := range opt.IfStates {
		if err := validateFlowMutationText("flow if_state", state); err != nil {
			return err
		}
	}
	if opt.TransitionTo == "running" {
		return errors.New("flow running state is only entered by ClaimDue")
	}
	if err := validateFlowMutationTimes(opt.NowMS, opt.RunAtMS); err != nil {
		return err
	}
	return validateNamedValues(opt.NamedValues)
}

func validateStartAndClaimOptions(opt StartAndClaimOptions) error {
	if err := validatePublicFlowID("flow id", opt.ID); err != nil {
		return err
	}
	if err := validatePublicFlowType("flow type", opt.Type); err != nil {
		return err
	}
	for _, field := range []struct{ name, value string }{
		{name: "flow initial state", value: opt.InitialState},
		{name: "flow worker", value: opt.Worker},
	} {
		if err := validateFlowMutationText(field.name, field.value); err != nil {
			return err
		}
	}
	if opt.InitialState == "running" {
		return errors.New("flow running state is only entered by ClaimDue")
	}
	for _, field := range []struct{ name, value string }{
		{name: "flow parent_flow_id", value: opt.ParentFlowID},
		{name: "flow root_flow_id", value: opt.RootFlowID},
	} {
		if err := validateOptionalPublicFlowIDReference(field.name, field.value); err != nil {
			return err
		}
	}
	if err := validateFlowReference("flow correlation_id", opt.CorrelationID); err != nil {
		return err
	}
	if err := validateCreateMutationFields(opt.NowMS, 0, opt.Priority, opt.RetentionTTLMS); err != nil {
		return err
	}
	leaseMS := opt.LeaseMS
	if leaseMS == 0 {
		leaseMS = 30_000
	}
	if err := validateFlowDeadline("flow lease milliseconds", opt.NowMS, leaseMS); err != nil {
		return err
	}
	if _, err := canonicalFlowMaxActiveMS(opt.MaxActiveMS); err != nil {
		return err
	}
	if err := validateNamedValues(NamedValues{Values: opt.Values, ValueRefs: opt.ValueRefs}); err != nil {
		return err
	}
	return validateFlowMetadata(opt.Attributes, nil, nil, opt.StateMeta)
}

func validateExtendLease(id, leaseToken string, fencingToken, leaseMS int64) error {
	if err := validatePublicFlowID("flow id", id); err != nil {
		return err
	}
	if err := validateFlowMutationText("flow lease token", leaseToken); err != nil {
		return err
	}
	if err := validateFlowExactNonNegative("flow fencing token", fencingToken); err != nil {
		return err
	}
	return validateFlowDeadline("flow lease milliseconds", 0, leaseMS)
}

func validateTransitionOptions(opt TransitionOptions) error {
	if err := validateFlowTransitionFields(opt.ID, opt.FromState, opt.ToState, opt.FencingToken); err != nil {
		return err
	}
	if err := validateFlowMutationText("flow lease token", opt.LeaseToken); err != nil {
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

func validateStepContinueOptions(opt StepContinueOptions) error {
	if err := validateFlowTransitionFields(opt.ID, opt.FromState, opt.ToState, opt.FencingToken); err != nil {
		return err
	}
	if err := validateFlowMutationText("flow lease token", opt.LeaseToken); err != nil {
		return err
	}
	leaseMS := opt.LeaseMS
	if leaseMS == 0 {
		leaseMS = 30_000
	}
	if err := validateFlowDeadline("flow lease milliseconds", opt.NowMS, leaseMS); err != nil {
		return err
	}
	if err := validateNamedValues(opt.NamedValues); err != nil {
		return err
	}
	return validateFlowMetadata(nil, opt.AttributesMerge, opt.AttributesDelete, opt.StateMeta)
}

func validateCompleteOptions(opt CompleteOptions) error {
	if err := validateClaimedMutation(opt.ID, opt.LeaseToken, opt.FencingToken, opt.NowMS); err != nil {
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

func validateRetryOptions(opt RetryOptions) error {
	if err := validateClaimedMutation(opt.ID, opt.LeaseToken, opt.FencingToken, opt.NowMS); err != nil {
		return err
	}
	if err := validateFlowExactNonNegative("flow run_at milliseconds", opt.RunAtMS); err != nil {
		return err
	}
	return validateFlowMetadata(nil, opt.AttributesMerge, opt.AttributesDelete, opt.StateMeta)
}

func validateFailOptions(opt FailOptions) error {
	if err := validateClaimedMutation(opt.ID, opt.LeaseToken, opt.FencingToken, opt.NowMS); err != nil {
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

func validateCancelOptions(opt CancelOptions) error {
	if err := validatePublicFlowID("flow id", opt.ID); err != nil {
		return err
	}
	if err := validateFlowExactNonNegative("flow fencing token", opt.FencingToken); err != nil {
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

func validateRewindOptions(opt RewindOptions) error {
	if err := validatePublicFlowID("flow id", opt.ID); err != nil {
		return err
	}
	if err := validateFlowMutationText("flow to_event", opt.ToEvent); err != nil {
		return err
	}
	if err := validateFlowMutationTimes(opt.NowMS, opt.RunAtMS); err != nil {
		return err
	}
	return nil
}

func validateFlowTransitionFields(id, fromState, toState string, fencingToken int64) error {
	if err := validatePublicFlowID("flow id", id); err != nil {
		return err
	}
	for _, field := range []struct{ name, value string }{
		{name: "flow from state", value: fromState},
		{name: "flow to state", value: toState},
	} {
		if err := validateFlowMutationText(field.name, field.value); err != nil {
			return err
		}
	}
	if toState == "running" {
		return errors.New("flow running state is only entered by ClaimDue")
	}
	return validateFlowExactNonNegative("flow fencing token", fencingToken)
}

func validateClaimedMutation(id, leaseToken string, fencingToken, nowMS int64) error {
	if err := validatePublicFlowID("flow id", id); err != nil {
		return err
	}
	if err := validateFlowMutationText("flow lease token", leaseToken); err != nil {
		return err
	}
	if err := validateFlowExactNonNegative("flow fencing token", fencingToken); err != nil {
		return err
	}
	return validateFlowExactNonNegative("flow now milliseconds", nowMS)
}

func validateFlowMutationTimes(nowMS, runAtMS int64) error {
	if err := validateFlowExactNonNegative("flow now milliseconds", nowMS); err != nil {
		return err
	}
	return validateFlowExactNonNegative("flow run_at milliseconds", runAtMS)
}

func validateFlowMutationPriority(priority *int64) error {
	if priority != nil && (*priority < 0 || *priority > maxFlowMutationPriority) {
		return fmt.Errorf("flow priority must be between 0 and %d", maxFlowMutationPriority)
	}
	return nil
}

func validateFlowMutationText(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s must be a non-empty string", name)
	}
	return nil
}

func validateNamedValues(named NamedValues) error {
	for name := range named.Values {
		if err := validateFlowMutationText("flow value name", name); err != nil {
			return err
		}
	}
	for name, ref := range named.ValueRefs {
		if err := validateFlowMutationText("flow value name", name); err != nil {
			return err
		}
		if err := validateFlowMutationText("flow value ref", ref); err != nil {
			return err
		}
	}
	for _, name := range named.DropValues {
		if err := validateFlowMutationText("flow value name", name); err != nil {
			return err
		}
	}
	for _, name := range named.OverrideValues {
		if err := validateFlowMutationText("flow value name", name); err != nil {
			return err
		}
	}
	return nil
}
