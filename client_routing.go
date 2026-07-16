package ferricstore

import (
	"context"
	"errors"
	"fmt"
)

type explicitRoutingExecutor interface {
	doForKey(context.Context, any, ...any) (any, error)
}

// CommandForKey executes a command using an explicit topology routing key.
// It is intended for server/module commands that this SDK does not yet know
// how to route. On non-topology executors it behaves like Command.
func (c *Client) CommandForKey(ctx context.Context, key any, args ...any) (any, error) {
	if !isRouteKey(key) {
		return nil, fmt.Errorf("unsupported explicit routing key type %T", key)
	}
	routed, ok := c.exec.(explicitRoutingExecutor)
	if !ok {
		return c.Command(ctx, args...)
	}
	if err := validateCommandArgs(args); err != nil {
		return nil, err
	}
	if name, stateful := connectionStateCommand(args); stateful {
		return nil, fmt.Errorf("%s requires a connection-affine transaction and cannot use CommandForKey", name)
	}
	if err := c.legacyGate.readLock(ctx); err != nil {
		return nil, err
	}
	defer c.legacyGate.readUnlock()
	if c.currentLegacySession() != nil {
		return nil, errors.New("CommandForKey cannot run inside a connection-affine transaction")
	}
	return routed.doForKey(ctx, key, commandArgsForExecutor(c.exec, args)...)
}
