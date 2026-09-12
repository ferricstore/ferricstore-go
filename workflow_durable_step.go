package ferricstore

import (
	"context"
	"errors"
	"sync"
)

// AppliedOutcome reports that a WorkflowContext durable operation already
// performed the workflow mutation. Returning it from a workflow handler tells
// the worker not to apply a second transition, completion, retry, or failure.
//
// AppliedOutcome values are created by WorkflowContext.Advance and returned by
// WorkflowStepResult.AppliedOutcome for a newly committed Step. The zero value
// is invalid. When returned from a handler, the worker releases the refreshed
// claim into its logical run state.
type AppliedOutcome struct {
	claim   ClaimedItem
	applied bool
}

func (AppliedOutcome) kind() string { return "applied" }

// Claim returns the refreshed claim produced by the durable operation. If this
// outcome is returned from a handler, the worker consumes that claim to release
// the workflow, so callers must not use it after the handler returns.
func (o AppliedOutcome) Claim() ClaimedItem { return o.claim }

// WorkflowStepResult contains the refreshed claim and codec-decoded durable
// result returned by WorkflowContext.Step. A new commit has an applied outcome
// that the handler should return; a replay has no applied outcome, allowing the
// handler to continue with its intended next outcome.
type WorkflowStepResult struct {
	claim   ClaimedItem
	result  any
	outcome AppliedOutcome
	applied bool
}

// Claim returns the current validated or refreshed workflow claim.
func (r WorkflowStepResult) Claim() ClaimedItem { return r.claim }

// Result returns the stored, codec-decoded durable-step result.
func (r WorkflowStepResult) Result() any { return r.result }

// Applied reports whether this invocation committed the step and state change.
// It is false when an existing committed result was replayed.
func (r WorkflowStepResult) Applied() bool { return r.applied }

// AppliedOutcome returns the already-applied worker outcome for a new commit.
// The second result is false for replay, when the handler must continue and
// return its intended next outcome.
func (r WorkflowStepResult) AppliedOutcome() (AppliedOutcome, bool) {
	return r.outcome, r.applied
}

// OutcomeOr returns the already-applied outcome for a new commit, or replayed
// when Step recovered an existing result. This makes the safe handler branch
// explicit without risking a replayed step pinning the worker lease.
func (r WorkflowStepResult) OutcomeOr(replayed Outcome) Outcome {
	if r.applied {
		return r.outcome
	}
	return replayed
}

// Advance atomically moves the currently claimed workflow to toState, adopts
// the refreshed claim for subsequent context operations, and returns an
// outcome that can be returned directly from the workflow handler.
func (c *WorkflowContext) Advance(ctx context.Context, toState string) (AppliedOutcome, error) {
	if c == nil {
		return AppliedOutcome{}, errors.New("workflow context is required")
	}
	if c.Client == nil {
		return AppliedOutcome{}, errors.New("workflow context client is required")
	}
	claim := c.claim()
	refreshed, err := c.Client.Advance(ctx, claim, toState)
	if err != nil {
		c.markUncertain(err)
		return AppliedOutcome{}, err
	}
	c.adopt(refreshed)
	return newAppliedOutcome(refreshed), nil
}

// Step validates the current lease and fencing token, durably executes run
// under name, atomically stores its result and advances to toState, adopts the
// refreshed claim, and returns a result that distinguishes a new application
// from replay. Replayed committed steps return their stored result without
// executing run again so the handler can continue to its intended outcome.
func (c *WorkflowContext) Step(
	ctx context.Context,
	name string,
	run func() (any, error),
	toState string,
) (WorkflowStepResult, error) {
	if c == nil {
		return WorkflowStepResult{}, errors.New("workflow context is required")
	}
	if c.Client == nil {
		return WorkflowStepResult{}, errors.New("workflow context client is required")
	}
	claim := c.claim()
	refreshed, result, disposition, err := c.Client.stepWithDisposition(ctx, claim, name, run, toState)
	if err != nil {
		c.markUncertain(err)
		return WorkflowStepResult{}, err
	}
	c.adopt(refreshed)
	step := WorkflowStepResult{claim: refreshed, result: result}
	if disposition == durableStepApplied {
		step.outcome = newAppliedOutcome(refreshed)
		step.applied = true
	}
	return step, nil
}

func newAppliedOutcome(claim ClaimedItem) AppliedOutcome {
	return AppliedOutcome{claim: claim, applied: true}
}

func (o AppliedOutcome) validate(job FlowRecord) error {
	if !o.applied {
		return errors.New("workflow handler returned an uninitialized applied outcome")
	}
	if o.claim.ID == "" || o.claim.LeaseToken == "" || o.claim.FencingToken <= 0 ||
		o.claim.State != "running" || o.claim.RunState == "" {
		return errors.New("workflow handler returned an invalid applied outcome")
	}
	if o.claim.ID != job.ID || o.claim.PartitionKey != job.PartitionKey ||
		o.claim.LeaseToken != job.LeaseToken || o.claim.FencingToken != job.FencingToken ||
		o.claim.State != job.State || o.claim.RunState != job.RunState {
		return errors.New("workflow handler returned a stale applied outcome")
	}
	return nil
}

type workflowContextState struct {
	mu        sync.RWMutex
	job       FlowRecord
	uncertain error
}

func newWorkflowContextState(job FlowRecord) *workflowContextState {
	return &workflowContextState{job: job}
}

func (s *workflowContextState) snapshot() FlowRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.job
}

func (s *workflowContextState) store(job FlowRecord) {
	s.mu.Lock()
	s.job = job
	s.mu.Unlock()
}

func (s *workflowContextState) markUncertain(err error) {
	if !errors.Is(err, ErrDurableMutationUncertain) {
		return
	}
	s.mu.Lock()
	if s.uncertain == nil {
		s.uncertain = err
	}
	s.mu.Unlock()
}

func (s *workflowContextState) uncertainty() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.uncertain
}

func (c WorkflowContext) currentJob() FlowRecord {
	if c.state != nil {
		return c.state.snapshot()
	}
	return c.Job
}

func (c WorkflowContext) claim() ClaimedItem {
	job := c.currentJob()
	runState := job.RunState
	if runState == "" {
		runState = c.StateName
	}
	if runState == "" && job.State != "running" {
		runState = job.State
	}
	return ClaimedItem{
		ID:           job.ID,
		LeaseToken:   job.LeaseToken,
		FencingToken: job.FencingToken,
		PartitionKey: job.PartitionKey,
		Type:         job.Type,
		State:        job.State,
		RunState:     runState,
		Payload:      job.Payload,
		Attributes:   job.Attributes,
	}
}

func (c *WorkflowContext) markUncertain(err error) {
	if c != nil && c.state != nil {
		c.state.markUncertain(err)
	}
}

func (c WorkflowContext) uncertainty() error {
	if c.state == nil {
		return nil
	}
	return c.state.uncertainty()
}

func (c *WorkflowContext) adopt(claim ClaimedItem) {
	job := c.currentJob()
	job.ID = claim.ID
	job.LeaseToken = claim.LeaseToken
	job.FencingToken = claim.FencingToken
	job.PartitionKey = claim.PartitionKey
	job.Type = claim.Type
	job.State = claim.State
	job.RunState = claim.RunState
	job.Payload = claim.Payload
	job.Attributes = claim.Attributes
	c.Job = job
	c.StateName = claim.RunState
	if c.state != nil {
		c.state.store(job)
	}
}
