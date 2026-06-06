package ferricstore

import (
	"context"
	"sync"
	"time"
)

const defaultWorkerPollInterval = time.Second

func (w *QueueWorker) RunForever(ctx context.Context, interval time.Duration) (QueueWorkerResult, error) {
	if interval <= 0 {
		interval = defaultWorkerPollInterval
	}
	var total QueueWorkerResult
	for {
		select {
		case <-ctx.Done():
			return total, nil
		default:
		}
		result, err := w.RunOnce(ctx)
		total.add(result)
		if err != nil {
			return total, err
		}
		if result.Claimed > 0 {
			continue
		}
		if !sleepOrDone(ctx, interval) {
			return total, nil
		}
	}
}

func (w *QueueWorker) Start(ctx context.Context, interval time.Duration) *QueueWorkerHandle {
	ctx, cancel := context.WithCancel(ctx)
	handle := &QueueWorkerHandle{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(handle.done)
		if interval <= 0 {
			interval = defaultWorkerPollInterval
		}
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			result, err := w.RunOnce(ctx)
			handle.add(result)
			if err != nil {
				handle.setError(err)
				return
			}
			if result.Claimed > 0 {
				continue
			}
			if !sleepOrDone(ctx, interval) {
				return
			}
		}
	}()
	return handle
}

type QueueWorkerHandle struct {
	cancel func()
	done   chan struct{}
	mu     sync.Mutex
	result QueueWorkerResult
	err    error
}

func (h *QueueWorkerHandle) Stop() {
	h.cancel()
}

func (h *QueueWorkerHandle) Join() (QueueWorkerResult, error) {
	<-h.done
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.result, h.err
}

func (h *QueueWorkerHandle) Stats() QueueWorkerResult {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.result
}

func (h *QueueWorkerHandle) add(result QueueWorkerResult) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.result.add(result)
}

func (h *QueueWorkerHandle) setError(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.err == nil {
		h.err = err
	}
}

func (r *QueueWorkerResult) add(other QueueWorkerResult) {
	r.Claimed += other.Claimed
	r.Completed += other.Completed
	r.Retried += other.Retried
	r.Failed += other.Failed
	r.ClaimCalls += other.ClaimCalls
}

func (w *WorkflowWorker) RunForever(ctx context.Context, interval time.Duration) (WorkflowWorkerResult, error) {
	if interval <= 0 {
		interval = defaultWorkerPollInterval
	}
	var total WorkflowWorkerResult
	for {
		select {
		case <-ctx.Done():
			return total, nil
		default:
		}
		result, err := w.RunOnce(ctx)
		total.add(result)
		if err != nil {
			return total, err
		}
		if result.Claimed > 0 {
			continue
		}
		if !sleepOrDone(ctx, interval) {
			return total, nil
		}
	}
}

func (w *WorkflowWorker) Start(ctx context.Context, interval time.Duration) *WorkflowWorkerHandle {
	ctx, cancel := context.WithCancel(ctx)
	handle := &WorkflowWorkerHandle{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(handle.done)
		if interval <= 0 {
			interval = defaultWorkerPollInterval
		}
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			result, err := w.RunOnce(ctx)
			handle.add(result)
			if err != nil {
				handle.setError(err)
				return
			}
			if result.Claimed > 0 {
				continue
			}
			if !sleepOrDone(ctx, interval) {
				return
			}
		}
	}()
	return handle
}

type WorkflowWorkerHandle struct {
	cancel func()
	done   chan struct{}
	mu     sync.Mutex
	result WorkflowWorkerResult
	err    error
}

func (h *WorkflowWorkerHandle) Stop() {
	h.cancel()
}

func (h *WorkflowWorkerHandle) Join() (WorkflowWorkerResult, error) {
	<-h.done
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.result, h.err
}

func (h *WorkflowWorkerHandle) Stats() WorkflowWorkerResult {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.result
}

func (h *WorkflowWorkerHandle) add(result WorkflowWorkerResult) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.result.add(result)
}

func (h *WorkflowWorkerHandle) setError(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.err == nil {
		h.err = err
	}
}

func (r *WorkflowWorkerResult) add(other WorkflowWorkerResult) {
	r.Claimed += other.Claimed
	r.Applied += other.Applied
	r.ClaimCalls += other.ClaimCalls
}

func sleepOrDone(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
