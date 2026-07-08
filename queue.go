package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type QueueClient struct {
	client *Client
}

func NewQueueClient(client *Client) *QueueClient {
	return &QueueClient{client: client}
}

func (c *QueueClient) Queue(flowType string) *Queue {
	return &Queue{client: c.client, Type: flowType, State: "queued"}
}

type Queue struct {
	client *Client
	Type   string
	State  string
}

func (q *Queue) Enqueue(ctx context.Context, id string, payload any, opt CreateOptions) (*FlowRecord, error) {
	opt.ID = id
	opt.Type = q.Type
	opt.Payload = payload
	if opt.State == "" {
		opt.State = q.State
	}
	return q.client.Enqueue(ctx, opt)
}

func (q *Queue) EnqueueMany(ctx context.Context, items []CreateItem, opt CreateManyOptions) ([]FlowRecord, error) {
	opt.Items = items
	opt.Type = q.Type
	if opt.State == "" {
		opt.State = q.State
	}
	return q.client.EnqueueMany(ctx, opt)
}

func (q *Queue) InstallPolicy(ctx context.Context, opt PolicyOptions) (any, error) {
	return q.client.SetPolicy(ctx, q.Type, opt)
}

func (c *QueueClient) InstallPolicy(ctx context.Context, flowType string, opt PolicyOptions) (any, error) {
	return c.client.SetPolicy(ctx, flowType, opt)
}

func (q *Queue) Worker(worker string, handler QueueHandler, opts WorkerOptions) *QueueWorker {
	if opts.State == "" && len(opts.States) == 0 {
		opts.State = q.State
	}
	return &QueueWorker{client: q.client, Type: q.Type, Worker: worker, Handler: handler, Options: opts}
}

type QueueHandler func(context.Context, FlowRecord) error

type ErrorPolicy int

const (
	ErrorPolicyRetry ErrorPolicy = iota
	ErrorPolicyFail
	ErrorPolicyReturn
)

type WorkerOptions struct {
	State          string
	States         []string
	PartitionKey   string
	PartitionKeys  []string
	BatchSize      int
	LeaseMS        int64
	NowMS          int64
	Concurrency    int
	ReclaimExpired *bool
	ReclaimRatio   *int64
	ClaimPayload   bool
	ErrorPolicy    ErrorPolicy
}

type QueueWorkerResult struct {
	Claimed    int
	Completed  int
	Retried    int
	Failed     int
	ClaimCalls int
}

type QueueWorker struct {
	client  *Client
	Type    string
	Worker  string
	Handler QueueHandler
	Options WorkerOptions
}

func (w *QueueWorker) RunOnce(ctx context.Context) (QueueWorkerResult, error) {
	if w.Handler == nil {
		return QueueWorkerResult{}, errors.New("queue worker handler is nil")
	}
	opts := w.Options
	if opts.BatchSize == 0 {
		opts.BatchSize = 10
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}
	var payload *bool
	if opts.ClaimPayload {
		payload = Bool(true)
	}
	jobs, err := w.client.ClaimDue(ctx, ClaimDueOptions{
		Type:           w.Type,
		State:          opts.State,
		States:         opts.States,
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
	if err != nil {
		return QueueWorkerResult{ClaimCalls: 1}, err
	}
	result := QueueWorkerResult{Claimed: len(jobs), ClaimCalls: 1}
	if len(jobs) == 0 {
		return result, nil
	}
	var mu sync.Mutex
	var firstErr error
	completedJobs := make([]ClaimedItem, 0, len(jobs))
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
		handlerErr := w.Handler(ctx, job)
		if handlerErr == nil {
			mu.Lock()
			defer mu.Unlock()
			completedJobs = append(completedJobs, ClaimedItem{
				ID:           job.ID,
				PartitionKey: job.PartitionKey,
				LeaseToken:   job.LeaseToken,
				FencingToken: job.FencingToken,
			})
			return
		}
		if opts.ErrorPolicy == ErrorPolicyReturn {
			recordErr(handlerErr)
			return
		}
		if opts.ErrorPolicy == ErrorPolicyFail {
			_, err := w.client.Fail(ctx, FailOptions{
				ID:           job.ID,
				LeaseToken:   job.LeaseToken,
				FencingToken: job.FencingToken,
				PartitionKey: job.PartitionKey,
				Error:        errorPayload(handlerErr),
			})
			if err != nil {
				recordErr(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			result.Failed++
			return
		}
		_, err := w.client.Retry(ctx, RetryOptions{
			ID:           job.ID,
			LeaseToken:   job.LeaseToken,
			FencingToken: job.FencingToken,
			PartitionKey: job.PartitionKey,
			Error:        errorPayload(handlerErr),
		})
		if err != nil {
			recordErr(err)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		result.Retried++
	}
	runConcurrent(jobs, opts.Concurrency, run)
	if firstErr != nil {
		return result, firstErr
	}
	if err := w.completeSuccessfulJobs(ctx, completedJobs); err != nil {
		return result, err
	}
	result.Completed = len(completedJobs)
	return result, nil
}

func (w *QueueWorker) completeSuccessfulJobs(ctx context.Context, jobs []ClaimedItem) error {
	if len(jobs) == 0 {
		return nil
	}
	if len(jobs) == 1 {
		job := jobs[0]
		_, err := w.client.Complete(ctx, CompleteOptions{
			ID:           job.ID,
			LeaseToken:   job.LeaseToken,
			FencingToken: job.FencingToken,
			PartitionKey: job.PartitionKey,
		})
		return err
	}
	_, err := w.client.CompleteMany(ctx, CompleteManyOptions{
		Items: jobs,
	})
	return err
}

func runConcurrent(jobs []FlowRecord, concurrency int, fn func(FlowRecord)) {
	if concurrency <= 1 || len(jobs) <= 1 {
		for _, job := range jobs {
			fn(job)
		}
		return
	}
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for _, job := range jobs {
		sem <- struct{}{}
		wg.Add(1)
		go func(job FlowRecord) {
			defer wg.Done()
			defer func() { <-sem }()
			fn(job)
		}(job)
	}
	wg.Wait()
}

func errorPayload(err error) map[string]string {
	return map[string]string{"message": err.Error(), "type": fmt.Sprintf("%T", err)}
}
