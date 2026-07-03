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
)

var errAutoBatchClosed = errors.New("ferricstore autobatch executor is closed")

type AutoBatchOptions struct {
	MaxSize       int
	FlushInterval time.Duration
	QueueSize     int
}

type AutoBatchExecutor struct {
	client    *Client
	requests  chan autoBatchRequest
	closed    chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	isClosed  atomic.Bool
}

type autoBatchRequest struct {
	ctx    context.Context
	args   []any
	result chan autoBatchResult
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
	exec := &AutoBatchExecutor{
		client:   client,
		requests: make(chan autoBatchRequest, opt.QueueSize),
		closed:   make(chan struct{}),
		done:     make(chan struct{}),
	}
	go exec.loop(opt.MaxSize, opt.FlushInterval)
	return exec
}

func NewAutoBatchClient(addr string, batch AutoBatchOptions, opts ...ClientOption) *Client {
	base := NewClient(addr, opts...)
	exec := NewAutoBatchExecutor(base, batch)
	client := NewClientWithExecutor(exec, opts...)
	client.closer = func() error {
		autoErr := exec.Close()
		baseErr := base.Close()
		if autoErr != nil {
			return autoErr
		}
		return baseErr
	}
	return client
}

func NewAutoBatchClientFromURL(rawurl string, batch AutoBatchOptions, opts ...ClientOption) (*Client, error) {
	base, err := NewClientFromURL(rawurl, opts...)
	if err != nil {
		return nil, err
	}
	exec := NewAutoBatchExecutor(base, batch)
	client := NewClientWithExecutor(exec, opts...)
	client.closer = func() error {
		autoErr := exec.Close()
		baseErr := base.Close()
		if autoErr != nil {
			return autoErr
		}
		return baseErr
	}
	return client, nil
}

func (e *AutoBatchExecutor) Do(ctx context.Context, args ...any) (any, error) {
	if e == nil || e.client == nil {
		return nil, errors.New("ferricstore autobatch executor requires a client")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if e.isClosed.Load() {
		return nil, errAutoBatchClosed
	}
	request := autoBatchRequest{
		ctx:    ctx,
		args:   append([]any(nil), args...),
		result: make(chan autoBatchResult, 1),
	}
	select {
	case e.requests <- request:
		if e.isClosed.Load() {
			return nil, errAutoBatchClosed
		}
	case <-e.closed:
		return nil, errAutoBatchClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case result := <-request.result:
		return result.value, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (e *AutoBatchExecutor) Close() error {
	e.closeOnce.Do(func() {
		e.isClosed.Store(true)
		close(e.closed)
	})
	<-e.done
	return nil
}

func (e *AutoBatchExecutor) loop(maxSize int, flushInterval time.Duration) {
	defer close(e.done)
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
	flush := func() {
		if len(batch) == 0 {
			return
		}
		stopTimer()
		e.flush(batch)
		batch = batch[:0]
	}

	for {
		if len(batch) == 0 {
			select {
			case request := <-e.requests:
				batch = append(batch, request)
				startTimer()
				if len(batch) >= maxSize {
					flush()
				}
			case <-e.closed:
				e.drain(&batch, maxSize)
				flush()
				return
			}
			continue
		}

		select {
		case request := <-e.requests:
			batch = append(batch, request)
			if len(batch) >= maxSize {
				flush()
			}
		case <-timer.C:
			timerActive = false
			flush()
		case <-e.closed:
			e.drain(&batch, maxSize)
			flush()
			return
		}
	}
}

func (e *AutoBatchExecutor) drain(batch *[]autoBatchRequest, maxSize int) {
	for len(*batch) < maxSize {
		select {
		case request := <-e.requests:
			*batch = append(*batch, request)
		default:
			return
		}
	}
}

func (e *AutoBatchExecutor) flush(batch []autoBatchRequest) {
	commands := make([][]any, 0, len(batch))
	active := make([]autoBatchRequest, 0, len(batch))
	for _, request := range batch {
		select {
		case <-request.ctx.Done():
			request.result <- autoBatchResult{err: request.ctx.Err()}
			continue
		default:
		}
		active = append(active, request)
		commands = append(commands, request.args)
	}
	if len(active) == 0 {
		return
	}
	values, err := e.client.Pipeline(context.Background(), commands)
	if err != nil {
		for _, request := range active {
			request.result <- autoBatchResult{err: err}
		}
		return
	}
	if len(values) != len(active) {
		err = fmt.Errorf("ferricstore autobatch returned %d results for %d commands", len(values), len(active))
		for _, request := range active {
			request.result <- autoBatchResult{err: err}
		}
		return
	}
	for i, request := range active {
		request.result <- autoBatchResult{value: values[i]}
	}
}
