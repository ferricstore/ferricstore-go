package ferricstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
)

// ErrTypedReplyInTransaction reports a typed call that cannot produce its
// declared result while a legacy MULTI session is queueing. Use Client.Command
// or Transaction.Command when commands must be queued explicitly.
var ErrTypedReplyInTransaction = errors.New("ferricstore: typed reply is unavailable during MULTI; use Client.Command or Transaction.Command")

// ErrTypedReplyBuffered reports a typed call whose reply cannot exist until a
// later BufferedExecutor.Flush. Use Client.Command when intentionally queueing
// a reply-producing command and inspect the corresponding Flush result.
var ErrTypedReplyBuffered = errors.New("ferricstore: typed reply is unavailable while buffering; use Client.Command and inspect BufferedExecutor.Flush results")

type typedDirectCommand func() (any, error)

type typedCommandStateValue struct {
	value  any
	queued bool
}

func (c *Client) typedReply(ctx context.Context, args ...any) (any, error) {
	return c.typedReplyOrQueue(ctx, false, args...)
}

func (c *Client) typedReplyOrQueue(ctx context.Context, allowQueued bool, args ...any) (any, error) {
	value, _, err := c.typedCommandWithQueuePolicy(ctx, false, allowQueued, nil, func() []any { return args })
	return value, err
}

func (c *Client) typedStatus(ctx context.Context, args ...any) error {
	return c.typedExpectedStatus(ctx, "OK", args...)
}

func (c *Client) typedExpectedStatus(ctx context.Context, expected string, args ...any) error {
	value, queued, err := c.typedCommandWithState(ctx, true, nil, func() []any { return args })
	return typedExpectedStatusResponse(value, queued, expected, err)
}

func (c *Client) typedCommandWithState(
	ctx context.Context,
	allowQueued bool,
	direct typedDirectCommand,
	fallback func() []any,
) (any, bool, error) {
	return c.typedCommandWithQueuePolicy(ctx, allowQueued, allowQueued, direct, fallback)
}

func (c *Client) typedCommandWithQueuePolicy(
	ctx context.Context,
	allowTransactionQueue bool,
	allowExecutorQueue bool,
	direct typedDirectCommand,
	fallback func() []any,
) (any, bool, error) {
	if err := c.legacyGate.readLock(ctx); err != nil {
		return nil, false, err
	}
	defer c.legacyGate.readUnlock()

	c.legacyMu.Lock()
	session, multi := c.legacy, c.legacyMulti
	c.legacyMu.Unlock()
	if session != nil {
		if multi && !allowTransactionQueue {
			return nil, true, ErrTypedReplyInTransaction
		}
		value, err := session.Do(ctx, affineCommandArgs(fallback())...)
		return value, multi, err
	}
	if direct != nil {
		value, err := direct()
		value, queued := unwrapTypedCommandState(value)
		return value, queued, err
	}
	return c.commandWithoutLegacyWithQueuePolicy(ctx, allowExecutorQueue, fallback()...)
}

func wrapTypedCommandState(value any, queued bool) any {
	if !queued {
		return value
	}
	return typedCommandStateValue{value: value, queued: true}
}

func unwrapTypedCommandState(value any) (any, bool) {
	if state, ok := value.(typedCommandStateValue); ok {
		return state.value, state.queued
	}
	return value, false
}

func typedStatusResponse(value any, queued bool, err error) error {
	return typedExpectedStatusResponse(value, queued, "OK", err)
}

func typedExpectedStatusResponse(value any, queued bool, expected string, err error) error {
	if err != nil {
		return err
	}
	if queued {
		expected = "QUEUED"
	}
	valid := false
	switch response := value.(type) {
	case string:
		valid = strings.EqualFold(response, expected)
	case []byte:
		valid = bytes.EqualFold(response, []byte(expected))
	case nativeCompactOKCount:
		valid = !queued && expected == "OK" && response == 1
	default:
		return fmt.Errorf("expected %s response, got %T", expected, value)
	}
	if !valid {
		return fmt.Errorf("expected %s response, got %q", expected, value)
	}
	return nil
}
