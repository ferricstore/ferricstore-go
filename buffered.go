package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var errBufferedClientRequired = errors.New("ferricstore buffered executor requires a client")

const (
	defaultBufferedMaxCommands = 4096
	defaultBufferedMaxBytes    = 16 * 1024 * 1024
)

// ErrBufferedCapacity reports that admitting a command would exceed a
// BufferedExecutor command-count or retained-byte limit.
var ErrBufferedCapacity = errors.New("ferricstore buffered executor capacity exceeded")

// BufferedOptions bounds the commands and retained data waiting for Flush.
type BufferedOptions struct {
	MaxCommands int
	MaxBytes    int
}

type BufferedExecutor struct {
	mu          sync.Mutex
	flushMu     contextMutex
	client      *Client
	commands    [][]any
	queuedBytes int
	maxCommands int
	maxBytes    int
	tooLargeErr error
	flushes     atomic.Int64
	sent        atomic.Int64
	maxDepth    atomic.Int64

	// Deprecated: use Stats for race-free concurrent reads. These fields are
	// retained for source compatibility and are updated under the queue lock.
	Flushes      int64
	CommandsSent int64
	MaxDepth     int64
}

type BufferedStats struct {
	Flushes      int64
	CommandsSent int64
	MaxDepth     int64
}

type BufferedFlushError struct {
	Err      error
	Commands [][]any
}

func (e *BufferedFlushError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("ferricstore buffered flush failed for %d commands: %v", len(e.Commands), e.Err)
}

func (e *BufferedFlushError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// NewBufferedExecutor queues commands until Flush. Typed status helpers such as
// KV.Set may be queued through a Client backed by BufferedExecutor. Typed
// helpers that require an immediate reply fail with ErrTypedReplyBuffered
// before enqueue; use Client.Command and match the command with its Flush
// result when queueing those operations deliberately.
func NewBufferedExecutor(client *Client) *BufferedExecutor {
	return NewBufferedExecutorWithOptions(client, BufferedOptions{})
}

// NewBufferedExecutorWithOptions creates a manually flushed executor with
// bounded retained memory. Non-positive limits select the bounded defaults.
func NewBufferedExecutorWithOptions(client *Client, opt BufferedOptions) *BufferedExecutor {
	if opt.MaxCommands <= 0 {
		opt.MaxCommands = defaultBufferedMaxCommands
	}
	if opt.MaxBytes <= 0 {
		opt.MaxBytes = defaultBufferedMaxBytes
	}
	return &BufferedExecutor{
		client:      client,
		maxCommands: opt.MaxCommands,
		maxBytes:    opt.MaxBytes,
		tooLargeErr: fmt.Errorf("%w: command exceeds %d retained bytes", ErrBufferedCapacity, opt.MaxBytes),
	}
}

func (e *BufferedExecutor) Do(ctx context.Context, args ...any) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateCommandArgs(args); err != nil {
		return nil, err
	}
	e.mu.Lock()
	if len(e.commands) >= e.maxCommands {
		e.mu.Unlock()
		return nil, fmt.Errorf("%w: maximum command count is %d", ErrBufferedCapacity, e.maxCommands)
	}
	e.mu.Unlock()
	commandBytes, fits := bufferedCommandRetainedSize(args, e.maxBytes)
	if !fits {
		return nil, e.tooLargeErr
	}
	copied, err := snapshotCommandArgs(args)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(e.commands) >= e.maxCommands {
		return nil, fmt.Errorf("%w: maximum command count is %d", ErrBufferedCapacity, e.maxCommands)
	}
	if commandBytes > e.maxBytes-e.queuedBytes {
		return nil, fmt.Errorf("%w: maximum retained bytes is %d", ErrBufferedCapacity, e.maxBytes)
	}
	e.commands = append(e.commands, copied)
	e.queuedBytes += commandBytes
	return []byte("QUEUED"), nil
}

func (e *BufferedExecutor) doTypedWithState(
	ctx context.Context,
	allowQueued bool,
	args ...any,
) (any, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if !allowQueued {
		return nil, false, ErrTypedReplyBuffered
	}
	value, err := e.Do(ctx, args...)
	return value, err == nil, err
}

func (e *BufferedExecutor) Flush(ctx context.Context) ([]any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := e.flushMu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer e.flushMu.Unlock()
	e.mu.Lock()
	if err := ctx.Err(); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	if len(e.commands) == 0 {
		e.mu.Unlock()
		return nil, nil
	}
	client := e.client
	if client == nil {
		e.mu.Unlock()
		return nil, errBufferedClientRequired
	}
	commands := e.commands
	e.commands = nil
	e.queuedBytes = 0
	depth := int64(len(commands))
	e.flushes.Add(1)
	e.sent.Add(depth)
	for previous := e.maxDepth.Load(); depth > previous && !e.maxDepth.CompareAndSwap(previous, depth); previous = e.maxDepth.Load() {
	}
	e.Flushes++
	e.CommandsSent += depth
	if depth > e.MaxDepth {
		e.MaxDepth = depth
	}
	e.mu.Unlock()
	values, err := client.Pipeline(ctx, commands)
	if err != nil {
		return values, &BufferedFlushError{Err: err, Commands: commands}
	}
	return values, nil
}

func (e *BufferedExecutor) Stats() BufferedStats {
	if e == nil {
		return BufferedStats{}
	}
	return BufferedStats{
		Flushes:      e.flushes.Load(),
		CommandsSent: e.sent.Load(),
		MaxDepth:     e.maxDepth.Load(),
	}
}
