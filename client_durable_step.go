package ferricstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"unicode/utf8"
)

const (
	durableStepValuePrefix = "__ferricstore_step__:sha256:"
	durableStepLeaseMS     = int64(30_000)
)

// ErrDurableMutationUncertain marks a durable workflow mutation whose request
// may have committed even though its response was not safely observed. The
// caller must recover by reading or reclaiming the workflow; blindly issuing a
// retry, failure, or second transition with the old claim is unsafe.
var ErrDurableMutationUncertain = errors.New("ferricstore: durable workflow mutation outcome is uncertain")

type durableMutationUncertainError struct {
	operation string
	cause     error
}

func (e *durableMutationUncertainError) Error() string {
	return fmt.Sprintf("%s: %s: %v", ErrDurableMutationUncertain, e.operation, e.cause)
}

func (e *durableMutationUncertainError) Unwrap() error { return e.cause }

func (e *durableMutationUncertainError) Is(target error) bool {
	return target == ErrDurableMutationUncertain
}

type durableStepDisposition uint8

const (
	durableStepReplayed durableStepDisposition = iota
	durableStepApplied
)

type durableStepCommitValue struct {
	name    string
	encoded any
}

// Advance atomically moves a claimed workflow to toState and returns its
// refreshed claim. Identity, partition, run state, lease, and fencing data are
// taken from job; the server retains the current worker identity.
func (c *Client) Advance(ctx context.Context, job ClaimedItem, toState string) (ClaimedItem, error) {
	return c.advanceClaim(ctx, job, toState, nil)
}

// Step durably executes run under the stable step name, stores its result in
// the workflow, atomically advances to toState, and returns the refreshed claim
// and result. A committed result is replayed without executing run again.
// External operations should still use their own stable idempotency key.
func (c *Client) Step(
	ctx context.Context,
	job ClaimedItem,
	name string,
	run func() (any, error),
	toState string,
) (ClaimedItem, any, error) {
	claim, result, _, err := c.stepWithDisposition(ctx, job, name, run, toState)
	return claim, result, err
}

func (c *Client) stepWithDisposition(
	ctx context.Context,
	job ClaimedItem,
	name string,
	run func() (any, error),
	toState string,
) (ClaimedItem, any, durableStepDisposition, error) {
	if err := validateDurableStep(job, name, run, toState); err != nil {
		return ClaimedItem{}, nil, durableStepReplayed, err
	}

	validated, err := c.ExtendLease(
		ctx,
		job.ID,
		job.LeaseToken,
		job.FencingToken,
		durableStepLeaseMS,
		job.PartitionKey,
	)
	if err != nil {
		return ClaimedItem{}, nil, durableStepReplayed, err
	}
	if err := validateDurableStepPreflight(job, validated); err != nil {
		return ClaimedItem{}, nil, durableStepReplayed, err
	}

	valueName := durableStepValueName(name)
	if refValue, committed := validated.ValueRefs[valueName]; committed {
		if validated.RunState != toState {
			return ClaimedItem{}, nil, durableStepReplayed, errors.New(
				"committed durable step result does not match the requested target state",
			)
		}
		ref, err := durableStepResultRef(refValue)
		if err != nil {
			return ClaimedItem{}, nil, durableStepReplayed, fmt.Errorf("decode durable step result reference: %w", err)
		}
		result, err := c.durableStepStoredResult(ctx, ref)
		if err != nil {
			return ClaimedItem{}, nil, durableStepReplayed, err
		}
		return claimedItemFromRecord(*validated, job), result, durableStepReplayed, nil
	}

	if err := ctx.Err(); err != nil {
		return ClaimedItem{}, nil, durableStepReplayed, err
	}
	result, err := run()
	if err != nil {
		return ClaimedItem{}, nil, durableStepReplayed, err
	}
	encoded, err := c.codec.Encode(result)
	if err != nil {
		return ClaimedItem{}, nil, durableStepReplayed, fmt.Errorf("encode durable step result: %w", err)
	}
	storedResult, err := decodeValue(c.codec, encoded)
	if err != nil {
		return ClaimedItem{}, nil, durableStepReplayed, fmt.Errorf("decode durable step result: %w", err)
	}
	refreshed, err := c.advanceClaim(ctx, job, toState, &durableStepCommitValue{
		name: valueName, encoded: encoded,
	})
	if err != nil {
		return ClaimedItem{}, nil, durableStepApplied, err
	}
	return refreshed, storedResult, durableStepApplied, nil
}

func (c *Client) advanceClaim(
	ctx context.Context,
	job ClaimedItem,
	toState string,
	commit *durableStepCommitValue,
) (ClaimedItem, error) {
	if err := validateDurableClaim(job); err != nil {
		return ClaimedItem{}, err
	}
	opt := StepContinueOptions{
		ID:           job.ID,
		LeaseToken:   job.LeaseToken,
		FromState:    job.RunState,
		ToState:      toState,
		FencingToken: job.FencingToken,
		LeaseMS:      durableStepLeaseMS,
		PartitionKey: job.PartitionKey,
	}
	if err := validateStepContinueOptions(opt); err != nil {
		return ClaimedItem{}, err
	}

	args := []any{
		"FLOW.STEP_CONTINUE", opt.ID, opt.LeaseToken, opt.FromState, opt.ToState,
		"FENCING", opt.FencingToken,
		"LEASE_MS", opt.LeaseMS,
		"NOW", nowMS(),
	}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	if commit != nil {
		args = append(args, "VALUE", commit.name, commit.encoded)
	}
	appendOpt(&args, "RETURN", "JOBS_COMPACT")
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return ClaimedItem{}, durableMutationCommandError("FLOW.STEP_CONTINUE", err)
	}
	if _, compact := value.([]any); !compact {
		mapping, mapErr := nativeMap(value)
		if mapErr == nil {
			state, stateErr := optionalResponseStringField(mapping, "state", "claimed item")
			if stateErr != nil {
				return ClaimedItem{}, newDurableMutationUncertain("decode FLOW.STEP_CONTINUE response", stateErr)
			}
			if state == "" {
				return ClaimedItem{}, newDurableMutationUncertain(
					"decode FLOW.STEP_CONTINUE response",
					errors.New("full workflow response is missing state"),
				)
			}
			runState, runStateErr := optionalResponseStringField(mapping, "run_state", "claimed item")
			if runStateErr != nil {
				return ClaimedItem{}, newDurableMutationUncertain("decode FLOW.STEP_CONTINUE response", runStateErr)
			}
			if runState == "" {
				return ClaimedItem{}, newDurableMutationUncertain(
					"decode FLOW.STEP_CONTINUE response",
					errors.New("full workflow response is missing run state"),
				)
			}
		}
	}
	refreshed, err := claimedItemFromNative(value, c.codec)
	if err != nil {
		return ClaimedItem{}, newDurableMutationUncertain("decode FLOW.STEP_CONTINUE response", err)
	}
	// JOBS_COMPACT intentionally carries only claim identity. The new run state
	// is nevertheless authoritative because it was part of this same atomic
	// STEP_CONTINUE operation.
	if refreshed.RunState == "" {
		if _, compact := value.([]any); !compact {
			return ClaimedItem{}, newDurableMutationUncertain(
				"decode FLOW.STEP_CONTINUE response",
				errors.New("full workflow response is missing run state"),
			)
		}
		refreshed.RunState = toState
	}
	if err := validateAdvancedClaim(job, toState, refreshed); err != nil {
		return ClaimedItem{}, newDurableMutationUncertain("validate FLOW.STEP_CONTINUE response", err)
	}
	return mergeClaimProjection(refreshed, job), nil
}

func (c *Client) durableStepStoredResult(ctx context.Context, ref string) (any, error) {
	value, err := c.typedReply(ctx, "FLOW.VALUE.MGET", ref)
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("durable step result expected value array, got %T", value)
	}
	if len(items) != 1 {
		return nil, fmt.Errorf("durable step result returned %d values, expected 1", len(items))
	}
	if items[0] == nil {
		return nil, errors.New("durable step committed result is missing")
	}
	result, err := decodeValue(c.codec, items[0])
	if err != nil {
		return nil, fmt.Errorf("decode durable step committed result: %w", err)
	}
	return result, nil
}

func durableMutationCommandError(operation string, err error) error {
	if err == nil || durableMutationDefinitelyRejected(err) {
		return err
	}
	return newDurableMutationUncertain(operation, err)
}

func newDurableMutationUncertain(operation string, cause error) error {
	if errors.Is(cause, ErrDurableMutationUncertain) {
		return cause
	}
	return &durableMutationUncertainError{operation: operation, cause: cause}
}

func durableMutationDefinitelyRejected(err error) bool {
	var notSent *commandNotSentError
	if errors.As(err, &notSent) {
		return true
	}
	var nativeErr NativeError
	if errors.As(err, &nativeErr) {
		return true
	}
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		return false
	}
	if httpErr.StatusCode >= 500 {
		return false
	}
	if httpErr.StatusCode >= 400 && httpErr.StatusCode < 500 {
		return true
	}
	if httpErr.StatusCode == 200 {
		return httpErr.Code != "invalid_response" &&
			httpErr.Code != "response_too_large" &&
			httpErr.Code != "transport_error" &&
			httpErr.Code != "transport_timeout" &&
			httpErr.Code != "transport_canceled"
	}
	if httpErr.StatusCode != 0 {
		return false
	}
	if httpErr.SafeToRetry {
		return true
	}
	switch httpErr.Code {
	case "closed", "request_too_large", "too_many_commands":
		return true
	default:
		return false
	}
}

func durableStepValueName(name string) string {
	digest := sha256.Sum256([]byte(name))
	return durableStepValuePrefix + hex.EncodeToString(digest[:])
}

func durableStepResultRef(value any) (string, error) {
	if ref, err := responseString(value, nil); err == nil {
		if ref == "" {
			return "", errors.New("empty value reference")
		}
		return ref, nil
	}
	mapping, err := nativeMap(value)
	if err != nil {
		return "", err
	}
	ref, err := responseString(mapping["ref"], nil)
	if err != nil {
		return "", err
	}
	if ref == "" {
		return "", errors.New("empty value reference")
	}
	return ref, nil
}

func validateDurableStep(
	job ClaimedItem,
	name string,
	run func() (any, error),
	toState string,
) error {
	if err := validateDurableClaim(job); err != nil {
		return err
	}
	if err := validateStepContinueOptions(StepContinueOptions{
		ID:           job.ID,
		LeaseToken:   job.LeaseToken,
		FromState:    job.RunState,
		ToState:      toState,
		FencingToken: job.FencingToken,
		LeaseMS:      durableStepLeaseMS,
		PartitionKey: job.PartitionKey,
	}); err != nil {
		return err
	}
	if err := validateFlowMutationText("durable step name", name); err != nil {
		return err
	}
	if !utf8.ValidString(name) {
		return errors.New("durable step name must be valid UTF-8")
	}
	if run == nil {
		return errors.New("durable step run closure is required")
	}
	return nil
}

func validateDurableClaim(job ClaimedItem) error {
	if job.FencingToken <= 0 {
		return errors.New("claimed workflow fencing token must be positive")
	}
	if job.State != "" && job.State != "running" {
		return errors.New("claimed workflow state must be running")
	}
	return nil
}

func validateDurableStepPreflight(job ClaimedItem, record *FlowRecord) error {
	if record == nil {
		return errors.New("FLOW.EXTEND_LEASE did not return a workflow record")
	}
	if record.ID != job.ID || record.PartitionKey != job.PartitionKey {
		return errors.New("FLOW.EXTEND_LEASE returned a different workflow identity")
	}
	if record.LeaseToken != job.LeaseToken || record.FencingToken != job.FencingToken {
		return errors.New("FLOW.EXTEND_LEASE returned a different workflow claim")
	}
	if record.State != "running" || record.RunState != job.RunState {
		return errors.New("FLOW.EXTEND_LEASE returned a different workflow state")
	}
	return nil
}

func validateAdvancedClaim(job ClaimedItem, toState string, refreshed ClaimedItem) error {
	if refreshed.ID != job.ID || refreshed.PartitionKey != job.PartitionKey {
		return errors.New("FLOW.STEP_CONTINUE returned a different workflow identity")
	}
	if refreshed.LeaseToken == job.LeaseToken {
		return errors.New("FLOW.STEP_CONTINUE did not refresh the workflow lease")
	}
	if refreshed.FencingToken <= job.FencingToken {
		return errors.New("FLOW.STEP_CONTINUE did not increase the workflow fencing token")
	}
	if refreshed.State != "running" || refreshed.RunState != toState {
		return errors.New("FLOW.STEP_CONTINUE returned an unexpected workflow state")
	}
	return nil
}

func claimedItemFromRecord(record FlowRecord, fallback ClaimedItem) ClaimedItem {
	return mergeClaimProjection(ClaimedItem{
		ID:           record.ID,
		LeaseToken:   record.LeaseToken,
		FencingToken: record.FencingToken,
		PartitionKey: record.PartitionKey,
		Type:         record.Type,
		State:        record.State,
		RunState:     record.RunState,
		Payload:      record.Payload,
		Attributes:   record.Attributes,
	}, fallback)
}

func mergeClaimProjection(claim, fallback ClaimedItem) ClaimedItem {
	if claim.Type == "" {
		claim.Type = fallback.Type
	}
	if claim.Payload == nil {
		claim.Payload = fallback.Payload
	}
	if claim.Attributes == nil {
		claim.Attributes = fallback.Attributes
	}
	return claim
}
