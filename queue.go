package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const queueCompletionBatchWindow = time.Millisecond

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
	opts = snapshotWorkerOptions(opts)
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

func snapshotWorkerOptions(opts WorkerOptions) WorkerOptions {
	opts.States = append([]string(nil), opts.States...)
	opts.PartitionKeys = append([]string(nil), opts.PartitionKeys...)
	if opts.ReclaimExpired != nil {
		value := *opts.ReclaimExpired
		opts.ReclaimExpired = &value
	}
	if opts.ReclaimRatio != nil {
		value := *opts.ReclaimRatio
		opts.ReclaimRatio = &value
	}
	return opts
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
	if err := validateWorkerOptions(opts); err != nil {
		return QueueWorkerResult{}, err
	}
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
	successfulJobs := make(chan ClaimedItem, len(jobs))
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
		handlerErr := invokeQueueHandler(w.Handler, ctx, job)
		if handlerErr == nil {
			successfulJobs <- ClaimedItem{
				ID:           job.ID,
				PartitionKey: job.PartitionKey,
				LeaseToken:   job.LeaseToken,
				FencingToken: job.FencingToken,
			}
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
	go func() {
		runConcurrent(jobs, opts.Concurrency, run)
		close(successfulJobs)
	}()
	var completionErrors []error
	for {
		batch, open := nextQueueCompletionBatch(successfulJobs, opts.BatchSize)
		if len(batch) > 0 {
			if err := w.completeSuccessfulJobs(ctx, batch); err != nil {
				completionErrors = append(completionErrors, err)
			} else {
				result.Completed += len(batch)
			}
		}
		if !open {
			break
		}
	}
	mu.Lock()
	workerErr := firstErr
	mu.Unlock()
	return result, errors.Join(workerErr, errors.Join(completionErrors...))
}

func validateWorkerOptions(opts WorkerOptions) error {
	if opts.BatchSize < 0 {
		return errors.New("queue worker batch size must be non-negative")
	}
	if opts.Concurrency < 0 {
		return errors.New("queue worker concurrency must be non-negative")
	}
	if opts.LeaseMS < 0 {
		return errors.New("queue worker lease must be non-negative")
	}
	if opts.ErrorPolicy < ErrorPolicyRetry || opts.ErrorPolicy > ErrorPolicyReturn {
		return fmt.Errorf("queue worker error policy %d is invalid", opts.ErrorPolicy)
	}
	if duplicate, ok := firstDuplicateString(opts.States); ok {
		return fmt.Errorf("queue worker state %q is duplicated", duplicate)
	}
	if duplicate, ok := firstDuplicateString(opts.PartitionKeys); ok {
		return fmt.Errorf("queue worker partition key %q is duplicated", duplicate)
	}
	return nil
}

func firstDuplicateString(values []string) (string, bool) {
	const linearScanLimit = 16
	if len(values) > linearScanLimit {
		seen := make(map[string]struct{}, len(values))
		for _, value := range values {
			if _, exists := seen[value]; exists {
				return value, true
			}
			seen[value] = struct{}{}
		}
		return "", false
	}
	for index, value := range values {
		for previous := 0; previous < index; previous++ {
			if values[previous] == value {
				return value, true
			}
		}
	}
	return "", false
}

func nextQueueCompletionBatch(successes <-chan ClaimedItem, maxSize int) ([]ClaimedItem, bool) {
	first, open := <-successes
	if !open {
		return nil, false
	}
	capacity := 1
	if maxSize > 1 {
		capacity = min(maxSize, max(1, cap(successes)))
	}
	batch := make([]ClaimedItem, 1, capacity)
	batch[0] = first
	if maxSize <= 1 {
		return batch, true
	}
	timer := time.NewTimer(queueCompletionBatchWindow)
	defer timer.Stop()
	for len(batch) < maxSize {
		select {
		case job, open := <-successes:
			if !open {
				return batch, false
			}
			batch = append(batch, job)
		case <-timer.C:
			return batch, true
		}
	}
	return batch, true
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
