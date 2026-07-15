package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var errBufferedClientRequired = errors.New("ferricstore buffered executor requires a client")

type BufferedExecutor struct {
	mu       sync.Mutex
	flushMu  contextMutex
	client   *Client
	commands [][]any
	flushes  atomic.Int64
	sent     atomic.Int64
	maxDepth atomic.Int64

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
	return fmt.Sprintf("ferricstore buffered flush failed for %d commands: %v", len(e.Commands), e.Err)
}

func (e *BufferedFlushError) Unwrap() error { return e.Err }

// NewBufferedExecutor queues commands until Flush. Typed status helpers such as
// KV.Set may be queued through a Client backed by BufferedExecutor. Typed
// helpers that require an immediate reply fail with ErrTypedReplyBuffered
// before enqueue; use Client.Command and match the command with its Flush
// result when queueing those operations deliberately.
func NewBufferedExecutor(client *Client) *BufferedExecutor {
	return &BufferedExecutor{client: client}
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
	copied, err := snapshotCommandArgs(args)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.commands = append(e.commands, copied)
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
