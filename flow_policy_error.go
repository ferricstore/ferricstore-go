package ferricstore

import (
	"errors"
	"fmt"
	"strings"
)

// ErrStalePolicyGeneration identifies a failed Flow policy generation CAS.
var ErrStalePolicyGeneration = errors.New("ferricstore: stale flow policy generation")

// StalePolicyGenerationError reports a policy update rejected because its
// expected generation no longer matches the stored snapshot.
type StalePolicyGenerationError struct {
	FlowType           string
	ExpectedGeneration int64
	Err                error
}

func (e *StalePolicyGenerationError) Error() string {
	if e == nil {
		return ErrStalePolicyGeneration.Error()
	}
	return fmt.Sprintf(
		"%s for flow type %q at expected generation %d",
		ErrStalePolicyGeneration, e.FlowType, e.ExpectedGeneration,
	)
}

func (e *StalePolicyGenerationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *StalePolicyGenerationError) Is(target error) bool {
	return e != nil && target == ErrStalePolicyGeneration
}

func policySetError(flowType string, expectedGeneration *int64, err error) error {
	if err == nil || expectedGeneration == nil || !isStalePolicyGenerationError(err) {
		return err
	}
	return &StalePolicyGenerationError{
		FlowType:           flowType,
		ExpectedGeneration: *expectedGeneration,
		Err:                err,
	}
}

func isStalePolicyGenerationError(err error) bool {
	const staleMessage = "ERR stale flow policy generation"
	if nativeErr, ok := nativeErrorValue(err); ok {
		return strings.EqualFold(strings.TrimSpace(nativeErrorMessage(nativeErr.Value)), staleMessage)
	}
	message := strings.TrimSpace(err.Error())
	return strings.EqualFold(message, staleMessage) ||
		strings.HasSuffix(strings.ToLower(message), strings.ToLower(": "+staleMessage))
}
