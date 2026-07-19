package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultAutoBatchMaxSize       = 100
	defaultAutoBatchFlushInterval = time.Millisecond
	defaultAutoBatchQueueSize     = 4096
	defaultAutoBatchFlushTimeout  = 30 * time.Second
	defaultAutoBatchCloseTimeout  = 30 * time.Second
)

var errAutoBatchClosed = errors.New("ferricstore autobatch executor is closed")

type AutoBatchOptions struct {
	MaxSize         int
	FlushInterval   time.Duration
	QueueSize       int
	FlushTimeout    time.Duration
	ShutdownTimeout time.Duration
}

type AutoBatchExecutor struct {
	client          *Client
	requests        chan autoBatchRequest
	queueSlots      chan struct{}
	pipelineSlot    chan struct{}
	closed          chan struct{}
	done            chan struct{}
	submitMu        sync.RWMutex
	closeOnce       sync.Once
	isClosed        atomic.Bool
	closeErrMu      sync.Mutex
	closeErr        error
	codecMu         sync.Mutex
	shutdownTimeout time.Duration
}

type autoBatchRequest struct {
	ctx     context.Context
	args    []any
	result  chan autoBatchResult
	control *autoBatchRequestControl
}

type autoBatchResult struct {
	value any
	err   error
}

func NewAutoBatchExecutor(client *Client, opt AutoBatchOptions) *AutoBatchExecutor {
	if opt.MaxSize <= 0 {
		opt.MaxSize = defaultAutoBatchMaxSize
	}
	if opt.FlushInterval <= 0 {
		opt.FlushInterval = defaultAutoBatchFlushInterval
	}
	if opt.QueueSize <= 0 {
		opt.QueueSize = defaultAutoBatchQueueSize
	}
	if opt.FlushTimeout <= 0 {
		opt.FlushTimeout = defaultAutoBatchFlushTimeout
	}
	if opt.ShutdownTimeout <= 0 {
		opt.ShutdownTimeout = defaultAutoBatchCloseTimeout
	}
	exec := &AutoBatchExecutor{
		client:          client,
		requests:        make(chan autoBatchRequest, opt.QueueSize),
		queueSlots:      make(chan struct{}, opt.QueueSize),
		pipelineSlot:    make(chan struct{}, 1),
		closed:          make(chan struct{}),
		done:            make(chan struct{}),
		shutdownTimeout: opt.ShutdownTimeout,
	}
	for range opt.QueueSize {
		exec.queueSlots <- struct{}{}
	}
	exec.pipelineSlot <- struct{}{}
	go exec.loop(opt)
	return exec
}

func NewAutoBatchClient(addr string, batch AutoBatchOptions, opts ...ClientOption) *Client {
	base := NewClient(addr, opts...)
	return newAutoBatchClient(base, batch)
}

func NewAutoBatchClientFromURL(rawurl string, batch AutoBatchOptions, opts ...ClientOption) (*Client, error) {
	base, err := NewClientFromURL(rawurl, opts...)
	if err != nil {
		return nil, err
	}
	return newAutoBatchClient(base, batch), nil
}

func newAutoBatchClient(base *Client, batch AutoBatchOptions) *Client {
	exec := NewAutoBatchExecutor(base, batch)
	client := NewClientWithExecutor(exec)
	client.codec = base.codec
	client.closer = func() error {
		autoErr := exec.Close()
		baseErr := base.Close()
		return errors.Join(autoErr, baseErr)
	}
	return client
}

func (e *AutoBatchExecutor) Do(ctx context.Context, args ...any) (any, error) {
	value, _, err := e.doWithState(ctx, args...)
	return value, err
}

func (e *AutoBatchExecutor) doWithState(ctx context.Context, args ...any) (any, bool, error) {
	return e.doTypedWithState(ctx, true, args...)
}

func (e *AutoBatchExecutor) doTypedWithState(
	ctx context.Context,
	allowQueued bool,
	args ...any,
) (any, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateCommandArgs(args); err != nil {
		return nil, false, err
	}
	if isBlockingCommand(args) {
		return e.executeBlockingCommandDirect(ctx, allowQueued, args)
	}
	result, err := e.submitWithQueuePolicy(ctx, args, allowQueued)
	if err != nil {
		return nil, false, err
	}
	select {
	case result := <-result:
		value, queued := unwrapTypedCommandState(result.value)
		return value, queued, result.err
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}

func (e *AutoBatchExecutor) Close() error {
	e.closeOnce.Do(func() {
		e.submitMu.Lock()
		e.isClosed.Store(true)
		close(e.closed)
		e.submitMu.Unlock()
	})
	<-e.done
	e.closeErrMu.Lock()
	defer e.closeErrMu.Unlock()
	return e.closeErr
}

func (e *AutoBatchExecutor) loop(opt AutoBatchOptions) {
	defer close(e.done)
	maxSize, flushInterval := opt.MaxSize, opt.FlushInterval
	timer := time.NewTimer(flushInterval)
	if !timer.Stop() {
		<-timer.C
	}
	timerActive := false
	batch := make([]autoBatchRequest, 0, maxSize)

	startTimer := func() {
		if !timerActive {
			timer.Reset(flushInterval)
			timerActive = true
		}
	}
	stopTimer := func() {
		if timerActive {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timerActive = false
		}
	}
	flush := func(ctx context.Context) error {
		if len(batch) == 0 {
			return nil
		}
		stopTimer()
		err := e.flush(ctx, batch)
		batch = batch[:0]
		if err != nil && e.isClosed.Load() {
			e.setCloseError(err)
		}
		return err
	}
	drainAndFlush := func() {
		ctx, cancel := context.WithTimeout(context.Background(), opt.ShutdownTimeout)
		defer cancel()
		defer func() {
			if err := e.waitForPipeline(ctx); err != nil {
				e.setCloseError(err)
			}
		}()
		var terminalErr error
		for {
			select {
			case request := <-e.requests:
				e.queueSlots <- struct{}{}
				if request.isFlushRequest() {
					if terminalErr != nil {
						request.result <- autoBatchResult{err: terminalErr}
						continue
					}
					err := flush(ctx)
					request.result <- autoBatchResult{err: err}
					if err != nil && ctx.Err() != nil {
						terminalErr = ctx.Err()
					}
					continue
				}
				if terminalErr != nil {
					request.result <- autoBatchResult{err: terminalErr}
					continue
				}
				batch = append(batch, request)
				if len(batch) >= maxSize {
					if err := flush(ctx); err != nil && ctx.Err() != nil {
						terminalErr = ctx.Err()
					}
				}
			default:
				if terminalErr != nil {
					e.failRemaining(batch, terminalErr)
					batch = batch[:0]
					return
				}
				_ = flush(ctx)
				return
			}
		}
	}
	flushRegular := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), opt.FlushTimeout)
		defer cancel()
		return flush(ctx)
	}

	for {
		if len(batch) == 0 {
			select {
			case request := <-e.requests:
				e.queueSlots <- struct{}{}
				if request.isFlushRequest() {
					request.result <- autoBatchResult{err: flushRegular()}
					continue
				}
				batch = append(batch, request)
				startTimer()
				if len(batch) >= maxSize {
					_ = flushRegular()
				}
			case <-e.closed:
				drainAndFlush()
				return
			}
			continue
		}

		select {
		case request := <-e.requests:
			e.queueSlots <- struct{}{}
			if request.isFlushRequest() {
				request.result <- autoBatchResult{err: flushRegular()}
				continue
			}
			batch = append(batch, request)
			if len(batch) >= maxSize {
				_ = flushRegular()
			}
		case <-timer.C:
			timerActive = false
			_ = flushRegular()
		case <-e.closed:
			drainAndFlush()
			return
		}
	}
}

func (e *AutoBatchExecutor) flush(ctx context.Context, batch []autoBatchRequest) error {
	active := make([]autoBatchRequest, 0, len(batch))
	for _, request := range batch {
		select {
		case <-request.ctx.Done():
			request.result <- autoBatchResult{err: request.ctx.Err()}
			continue
		default:
		}
		active = append(active, request)
	}
	if len(active) == 0 {
		return nil
	}
	if err := e.acquirePipelineSlot(ctx); err != nil {
		for _, request := range active {
			request.result <- autoBatchResult{err: err}
		}
		return err
	}
	type pipelineResult struct {
		results []pipelineItemResult
		err     error
	}
	resultCh := make(chan pipelineResult, 1)
	go func() {
		defer func() { e.pipelineSlot <- struct{}{} }()
		results, err := e.executeAutoBatchRequests(ctx, active)
		resultCh <- pipelineResult{results: results, err: err}
	}()
	var results []pipelineItemResult
	var err error
	select {
	case result := <-resultCh:
		results, err = result.results, result.err
	case <-ctx.Done():
		err = ctx.Err()
	case <-e.closed:
		timer := time.NewTimer(e.shutdownTimeout)
		defer timer.Stop()
		select {
		case result := <-resultCh:
			results, err = result.results, result.err
		case <-ctx.Done():
			err = ctx.Err()
		case <-timer.C:
			err = context.DeadlineExceeded
		}
	}
	if err != nil {
		for _, request := range active {
			request.result <- autoBatchResult{err: err}
		}
		return err
	}
	if len(results) != len(active) {
		err = fmt.Errorf("ferricstore autobatch returned %d results for %d commands", len(results), len(active))
		for _, request := range active {
			request.result <- autoBatchResult{err: err}
		}
		return err
	}
	for i, request := range active {
		request.result <- autoBatchResult{value: results[i].value, err: results[i].err}
	}
	return nil
}

func (e *AutoBatchExecutor) acquirePipelineSlot(ctx context.Context) error {
	select {
	case <-e.pipelineSlot:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-e.closed:
		timer := time.NewTimer(e.shutdownTimeout)
		defer timer.Stop()
		select {
		case <-e.pipelineSlot:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return context.DeadlineExceeded
		}
	}
}

func (e *AutoBatchExecutor) waitForPipeline(ctx context.Context) error {
	select {
	case <-e.pipelineSlot:
		e.pipelineSlot <- struct{}{}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *AutoBatchExecutor) failRemaining(batch []autoBatchRequest, err error) {
	for _, request := range batch {
		request.result <- autoBatchResult{err: err}
	}
}

func (e *AutoBatchExecutor) setCloseError(err error) {
	if err == nil {
		return
	}
	e.closeErrMu.Lock()
	if e.closeErr == nil {
		e.closeErr = err
	}
	e.closeErrMu.Unlock()
}
