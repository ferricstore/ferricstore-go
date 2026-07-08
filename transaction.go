package ferricstore

import (
	"context"
	"errors"
)

type Transaction struct {
	client *Client
	closed bool
}

func (c *Client) Watch(ctx context.Context, keys ...string) error {
	args := []any{"WATCH"}
	for _, key := range keys {
		args = append(args, key)
	}
	_, err := c.Command(ctx, args...)
	return err
}

func (c *Client) Unwatch(ctx context.Context) error {
	_, err := c.Command(ctx, "UNWATCH")
	return err
}

func (c *Client) Multi(ctx context.Context) error {
	_, err := c.Command(ctx, "MULTI")
	return err
}

func (c *Client) Exec(ctx context.Context) ([]any, error) {
	value, err := c.Command(ctx, "EXEC")
	if err != nil || value == nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		return nil, errors.New("expected EXEC array response")
	}
	return items, nil
}

func (c *Client) Discard(ctx context.Context) error {
	_, err := c.Command(ctx, "DISCARD")
	return err
}

func (c *Client) CommandExec(ctx context.Context, command string, args ...any) (any, error) {
	return c.CommandExecWithContext(ctx, command, nil, args...)
}

func (c *Client) CommandExecWithContext(ctx context.Context, command string, requestContext *RequestContext, args ...any) (any, error) {
	payload := []any{"COMMAND_EXEC", command}
	payload = append(payload, args...)
	if requestContext != nil {
		payload = append(payload, "REQUEST_CONTEXT", requestContext)
	}
	return c.Command(ctx, payload...)
}

func (c *Client) Transaction(ctx context.Context) (*Transaction, error) {
	if err := c.Multi(ctx); err != nil {
		return nil, err
	}
	return &Transaction{client: c}, nil
}

func (t *Transaction) Command(ctx context.Context, args ...any) (any, error) {
	if t.closed {
		return nil, errors.New("transaction is closed")
	}
	if len(args) == 0 {
		return nil, errors.New("transaction command requires at least a command name")
	}
	return t.client.CommandExec(ctx, asString(args[0]), args[1:]...)
}

func (t *Transaction) Exec(ctx context.Context) ([]any, error) {
	if t.closed {
		return nil, errors.New("transaction is closed")
	}
	t.closed = true
	return t.client.Exec(ctx)
}

func (t *Transaction) Discard(ctx context.Context) error {
	if t.closed {
		return errors.New("transaction is closed")
	}
	t.closed = true
	return t.client.Discard(ctx)
}
