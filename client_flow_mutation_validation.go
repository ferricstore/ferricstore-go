package ferricstore

import (
	"errors"
	"fmt"
)

const maxFlowMutationPriority int64 = 2

func validateCreateOptions(opt CreateOptions) error {
	if err := validateFlowMutationText("flow id", opt.ID); err != nil {
		return err
	}
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
	if opt.NowMS < 0 {
		return errors.New("flow now milliseconds must be non-negative")
	}
	return validateOptionalPositiveInt64("flow value ttl milliseconds", opt.TTLMS)
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
	if err := validateFlowMutationText("flow id", opt.ID); err != nil {
		return err
	}
	if err := validateFlowMutationText("flow signal", opt.Signal); err != nil {
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
	if opt.RunAtMS < 0 || opt.NowMS < 0 {
		return errors.New("flow timestamps must be non-negative")
	}
	if opt.Priority != nil {
		return errors.New("FLOW.SIGNAL does not support priority")
	}
	return validateNamedValues(opt.NamedValues)
}

func validateStartAndClaimOptions(opt StartAndClaimOptions) error {
	for _, field := range []struct{ name, value string }{
		{name: "flow id", value: opt.ID},
		{name: "flow type", value: opt.Type},
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
	if opt.LeaseMS < 0 {
		return errors.New("flow lease milliseconds must be positive")
	}
	if err := validateCreateMutationFields(opt.NowMS, 0, opt.Priority, opt.RetentionTTLMS); err != nil {
		return err
	}
	return validateFlowMetadata(opt.Attributes, nil, nil, opt.StateMeta)
}

func validateExtendLease(id, leaseToken string, fencingToken, leaseMS int64) error {
	if err := validateFlowMutationText("flow id", id); err != nil {
		return err
	}
	if err := validateFlowMutationText("flow lease token", leaseToken); err != nil {
		return err
	}
	if fencingToken < 0 {
		return errors.New("flow fencing token must be non-negative")
	}
	if leaseMS <= 0 {
		return errors.New("flow lease milliseconds must be positive")
	}
	return nil
}

func validateTransitionOptions(opt TransitionOptions) error {
	if err := validateFlowTransitionFields(opt.ID, opt.FromState, opt.ToState, opt.FencingToken); err != nil {
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
	if opt.LeaseMS < 0 {
		return errors.New("flow lease milliseconds must be positive")
	}
	if opt.NowMS < 0 {
		return errors.New("flow now milliseconds must be non-negative")
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
	if err := validateOptionalPositiveInt64("flow ttl milliseconds", opt.TTLMS); err != nil {
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
	if opt.RunAtMS < 0 {
		return errors.New("flow run_at milliseconds must be non-negative")
	}
	if hasNamedValueMutation(opt.NamedValues) {
		return errors.New("FLOW.RETRY does not support named value mutations")
	}
	return validateFlowMetadata(nil, opt.AttributesMerge, opt.AttributesDelete, opt.StateMeta)
}

func validateFailOptions(opt FailOptions) error {
	if err := validateClaimedMutation(opt.ID, opt.LeaseToken, opt.FencingToken, opt.NowMS); err != nil {
		return err
	}
	if err := validateOptionalPositiveInt64("flow ttl milliseconds", opt.TTLMS); err != nil {
		return err
	}
	if err := validateNamedValues(opt.NamedValues); err != nil {
		return err
	}
	return validateFlowMetadata(nil, opt.AttributesMerge, opt.AttributesDelete, opt.StateMeta)
}

func validateCancelOptions(opt CancelOptions) error {
	if err := validateFlowMutationText("flow id", opt.ID); err != nil {
		return err
	}
	if opt.FencingToken < 0 {
		return errors.New("flow fencing token must be non-negative")
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

func validateRewindOptions(opt RewindOptions) error {
	if err := validateFlowMutationText("flow id", opt.ID); err != nil {
		return err
	}
	if err := validateFlowMutationText("flow to_event", opt.ToEvent); err != nil {
		return err
	}
	if opt.RunAtMS < 0 || opt.NowMS < 0 {
		return errors.New("flow timestamps must be non-negative")
	}
	if opt.ReasonRef != "" {
		return errors.New("FLOW.REWIND does not support reason_ref")
	}
	return nil
}

func validateFlowTransitionFields(id, fromState, toState string, fencingToken int64) error {
	for _, field := range []struct{ name, value string }{
		{name: "flow id", value: id},
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
	if fencingToken < 0 {
		return errors.New("flow fencing token must be non-negative")
	}
	return nil
}

func validateClaimedMutation(id, leaseToken string, fencingToken, nowMS int64) error {
	if err := validateFlowMutationText("flow id", id); err != nil {
		return err
	}
	if err := validateFlowMutationText("flow lease token", leaseToken); err != nil {
		return err
	}
	if fencingToken < 0 {
		return errors.New("flow fencing token must be non-negative")
	}
	if nowMS < 0 {
		return errors.New("flow now milliseconds must be non-negative")
	}
	return nil
}

func validateFlowMutationTimes(nowMS, runAtMS int64) error {
	if nowMS < 0 || runAtMS < 0 {
		return errors.New("flow timestamps must be non-negative")
	}
	return nil
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

func hasNamedValueMutation(named NamedValues) bool {
	return len(named.Values) != 0 || len(named.ValueRefs) != 0 ||
		len(named.DropValues) != 0 || len(named.OverrideValues) != 0
}
