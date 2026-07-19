package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
)

type Outcome interface {
	kind() string
}

type TransitionResult struct {
	ToState   string
	Payload   any
	RunAtMS   int64
	Priority  *int64
	StateMeta map[string]any
	NamedValues
}

func (TransitionResult) kind() string { return "transition" }

type CompleteResult struct {
	Result    any
	Payload   any
	TTLMS     *int64
	StateMeta map[string]any
	NamedValues
}

func (CompleteResult) kind() string { return "complete" }

type RetryResult struct {
	Error            any
	Payload          any
	RunAtMS          int64
	StateMeta        map[string]any
	AttributesMerge  map[string]any
	AttributesDelete []string
}

func (RetryResult) kind() string { return "retry" }

type FailResult struct {
	Error     any
	Payload   any
	TTLMS     *int64
	StateMeta map[string]any
	NamedValues
}

func (FailResult) kind() string { return "fail" }

func TransitionTo(state string, payload any) TransitionResult {
	return TransitionResult{ToState: state, Payload: payload}
}

func CompleteWith(result any) CompleteResult {
	return CompleteResult{Result: result}
}

func RetryWith(err any) RetryResult {
	return RetryResult{Error: err}
}

func FailWith(err any) FailResult {
	return FailResult{Error: err}
}

type WorkflowHandler func(context.Context, WorkflowContext) (Outcome, error)

type WorkflowContext struct {
	Client    *Client
	Job       FlowRecord
	StateName string
}

func (c WorkflowContext) ID() string   { return c.Job.ID }
func (c WorkflowContext) Type() string { return c.Job.Type }
func (c WorkflowContext) State() string {
	if c.StateName != "" {
		return c.StateName
	}
	return c.Job.State
}
func (c WorkflowContext) PartitionKey() string  { return c.Job.PartitionKey }
func (c WorkflowContext) Payload() any          { return c.Job.Payload }
func (c WorkflowContext) Value(name string) any { return c.Job.Values[name] }

type WorkflowClient struct {
	client *Client
}

func NewWorkflowClient(client *Client) *WorkflowClient {
	return &WorkflowClient{client: client}
}

func (c *WorkflowClient) Workflow(flowType, initialState string) *Workflow {
	return &Workflow{
		client:        c.client,
		Type:          flowType,
		InitialState:  initialState,
		handlers:      map[string]WorkflowHandler{},
		statePolicies: map[string]FlowStatePolicy{},
	}
}

type Workflow struct {
	client           *Client
	Type             string
	InitialState     string
	handlers         map[string]WorkflowHandler
	statePolicies    map[string]FlowStatePolicy
	configurationErr error
}

func (w *Workflow) State(name string, handler WorkflowHandler, policy ...FlowStatePolicy) *Workflow {
	if strings.TrimSpace(name) == "" {
		w.recordConfigurationError(errors.New("workflow state name must be a non-empty string"))
		return w
	}
	if handler == nil {
		w.recordConfigurationError(fmt.Errorf("workflow state %q requires a handler", name))
		return w
	}
	if len(policy) > 1 {
		w.recordConfigurationError(fmt.Errorf("workflow state %q accepts at most one state policy", name))
		return w
	}
	if _, exists := w.handlers[name]; exists {
		w.recordConfigurationError(fmt.Errorf("duplicate workflow state %q", name))
		return w
	}
	w.handlers[name] = handler
	if len(policy) > 0 {
		w.statePolicies[name] = snapshotFlowStatePolicy(policy[0])
	}
	return w
}

func (w *Workflow) recordConfigurationError(err error) {
	if w.configurationErr == nil {
		w.configurationErr = err
	}
}

func (w *Workflow) InstallPolicy(ctx context.Context, opt PolicyOptions) (any, error) {
	if w.configurationErr != nil {
		return nil, w.configurationErr
	}
	statePolicies := make(map[string]FlowStatePolicy, len(opt.StatePolicies)+len(w.statePolicies))
	for state, policy := range opt.StatePolicies {
		statePolicies[state] = snapshotFlowStatePolicy(policy)
	}
	for state, policy := range w.statePolicies {
		if _, exists := opt.States[state]; exists {
			return nil, fmt.Errorf("flow state %q appears in both States and workflow state policies", state)
		}
		if existing, exists := statePolicies[state]; exists && !equivalentFlowStatePolicy(existing, policy) {
			return nil, fmt.Errorf("flow state %q has conflicting PolicyOptions and workflow state policies", state)
		}
		statePolicies[state] = policy
	}
	opt.StatePolicies = statePolicies
	value, err := w.client.SetPolicy(ctx, w.Type, opt)
	if err != nil {
		return nil, err
	}
	// Keep the successfully installed snapshot so worker-side checks use the
	// same FIFO/PARALLEL policy that was sent to the server.
	w.statePolicies = statePolicies
	return value, nil
}

func snapshotFlowStatePolicy(policy FlowStatePolicy) FlowStatePolicy {
	if policy.Retry != nil {
		retry := *policy.Retry
		policy.Retry = &retry
	}
	return policy
}

func equivalentFlowStatePolicy(left, right FlowStatePolicy) bool {
	leftMode, leftErr := flowStateModeCommandToken(left.Mode)
	rightMode, rightErr := flowStateModeCommandToken(right.Mode)
	if leftErr != nil || rightErr != nil {
		if left.Mode != right.Mode {
			return false
		}
	} else if leftMode != rightMode {
		return false
	}
	if left.Retry == nil || right.Retry == nil {
		return left.Retry == nil && right.Retry == nil
	}
	return *left.Retry == *right.Retry
}

func (w *Workflow) Start(ctx context.Context, id string, payload any, opt CreateOptions) (*FlowRecord, error) {
	if w.configurationErr != nil {
		return nil, w.configurationErr
	}
	opt.ID = id
	opt.Type = w.Type
	opt.State = w.InitialState
	opt.Payload = payload
	return w.client.Create(ctx, opt)
}

func (w *Workflow) Worker(worker string, states []string, opts WorkerOptions) *WorkflowWorker {
	if len(states) == 0 {
		for state := range w.handlers {
			states = append(states, state)
		}
		slices.Sort(states)
	}
	opts.States = states
	opts = snapshotWorkerOptions(opts)
	return &WorkflowWorker{workflow: w, Worker: worker, Options: opts}
}

type WorkflowWorkerResult struct {
	Claimed    int
	Applied    int
	ClaimCalls int
}

type WorkflowWorker struct {
	workflow *Workflow
	Worker   string
	Options  WorkerOptions
}

func (w *WorkflowWorker) RunOnce(ctx context.Context) (WorkflowWorkerResult, error) {
	if w.workflow.configurationErr != nil {
		return WorkflowWorkerResult{}, w.workflow.configurationErr
	}
	opts := w.Options
	if len(opts.States) == 0 {
		return WorkflowWorkerResult{}, errors.New("workflow worker requires at least one state")
	}
	if err := validateWorkerOptions(opts); err != nil {
		return WorkflowWorkerResult{}, err
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 10
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}
	for _, stateName := range opts.States {
		if w.workflow.handlers[stateName] == nil {
			return WorkflowWorkerResult{}, errors.New("no workflow handler for state " + stateName)
		}
	}
	var payload *bool
	if opts.ClaimPayload {
		payload = Bool(true)
	}
	result := WorkflowWorkerResult{}
	for _, stateName := range opts.States {
		handler := w.workflow.handlers[stateName]

		jobs, err := w.workflow.client.ClaimDue(ctx, ClaimDueOptions{
			Type:           w.workflow.Type,
			State:          stateName,
			Worker:         w.Worker,
			PartitionKey:   opts.PartitionKey,
			PartitionKeys:  opts.PartitionKeys,
			LeaseMS:        opts.LeaseMS,
			Limit:          opts.BatchSize,
			NowMS:          opts.NowMS,
			ReclaimExpired: opts.ReclaimExpired,
			ReclaimRatio:   opts.ReclaimRatio,
			Payload:        payload,
		})
		result.ClaimCalls++
		if err != nil {
			return result, err
		}
		result.Claimed += len(jobs)
		if len(jobs) == 0 {
			continue
		}

		var mu sync.Mutex
		var firstErr error
		recordErr := func(err error) {
			if err == nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if firstErr == nil {
				firstErr = err
			}
		}
		run := func(job FlowRecord) {
			if err := w.apply(ctx, job, stateName, handler, opts.ErrorPolicy); err != nil {
				recordErr(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			result.Applied++
		}
		runConcurrent(jobs, opts.Concurrency, run)
		if firstErr != nil {
			return result, firstErr
		}
	}
	return result, nil
}

func (w *WorkflowWorker) apply(
	ctx context.Context,
	job FlowRecord,
	stateName string,
	handler WorkflowHandler,
	errorPolicy ErrorPolicy,
) error {
	outcome, err := invokeWorkflowHandler(
		handler,
		ctx,
		WorkflowContext{Client: w.workflow.client, Job: job, StateName: stateName},
	)
	if err != nil {
		if errorPolicy == ErrorPolicyReturn {
			return err
		}
		if errorPolicy == ErrorPolicyFail {
			_, failErr := w.workflow.client.Fail(ctx, FailOptions{
				ID:           job.ID,
				LeaseToken:   job.LeaseToken,
				FencingToken: job.FencingToken,
				PartitionKey: job.PartitionKey,
				Error:        errorPayload(err),
			})
			return failErr
		}
		_, retryErr := w.workflow.client.Retry(ctx, RetryOptions{
			ID:           job.ID,
			LeaseToken:   job.LeaseToken,
			FencingToken: job.FencingToken,
			PartitionKey: job.PartitionKey,
			Error:        errorPayload(err),
		})
		return retryErr
	}
	outcome, err = workflowOutcomeValue(outcome)
	if err != nil {
		return err
	}
	switch value := outcome.(type) {
	case TransitionResult:
		if value.Priority != nil && isFIFOStatePolicy(w.workflow.statePolicies[value.ToState]) {
			return errors.New("priority is not supported for fifo state")
		}
		_, err = w.workflow.client.Transition(ctx, TransitionOptions{
			ID:           job.ID,
			FromState:    job.State,
			ToState:      value.ToState,
			LeaseToken:   job.LeaseToken,
			FencingToken: job.FencingToken,
			PartitionKey: job.PartitionKey,
			Payload:      value.Payload,
			RunAtMS:      value.RunAtMS,
			Priority:     value.Priority,
			StateMeta:    value.StateMeta,
			NamedValues:  value.NamedValues,
		})
	case CompleteResult:
		_, err = w.workflow.client.Complete(ctx, CompleteOptions{
			ID:           job.ID,
			LeaseToken:   job.LeaseToken,
			FencingToken: job.FencingToken,
			PartitionKey: job.PartitionKey,
			Result:       value.Result,
			Payload:      value.Payload,
			TTLMS:        value.TTLMS,
			StateMeta:    value.StateMeta,
			NamedValues:  value.NamedValues,
		})
	case RetryResult:
		_, err = w.workflow.client.Retry(ctx, RetryOptions{
			ID:               job.ID,
			LeaseToken:       job.LeaseToken,
			FencingToken:     job.FencingToken,
			PartitionKey:     job.PartitionKey,
			Error:            value.Error,
			Payload:          value.Payload,
			RunAtMS:          value.RunAtMS,
			StateMeta:        value.StateMeta,
			AttributesMerge:  value.AttributesMerge,
			AttributesDelete: value.AttributesDelete,
		})
	case FailResult:
		_, err = w.workflow.client.Fail(ctx, FailOptions{
			ID:           job.ID,
			LeaseToken:   job.LeaseToken,
			FencingToken: job.FencingToken,
			PartitionKey: job.PartitionKey,
			Error:        value.Error,
			Payload:      value.Payload,
			TTLMS:        value.TTLMS,
			StateMeta:    value.StateMeta,
			NamedValues:  value.NamedValues,
		})
	default:
		err = errors.New("workflow handler returned nil or unknown outcome")
	}
	return err
}

func workflowOutcomeValue(outcome Outcome) (Outcome, error) {
	switch value := outcome.(type) {
	case *TransitionResult:
		if value != nil {
			return *value, nil
		}
	case *CompleteResult:
		if value != nil {
			return *value, nil
		}
	case *RetryResult:
		if value != nil {
			return *value, nil
		}
	case *FailResult:
		if value != nil {
			return *value, nil
		}
	default:
		if outcome != nil {
			return outcome, nil
		}
	}
	return nil, errors.New("workflow handler returned nil or unknown outcome")
}

func isFIFOStatePolicy(policy FlowStatePolicy) bool {
	mode, err := flowStateModeCommandToken(policy.Mode)
	return err == nil && mode == string(FlowStateModeFIFO)
}
