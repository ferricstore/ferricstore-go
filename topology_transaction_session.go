package ferricstore

import (
	"context"
	"fmt"
	"strings"
)

// topologyCommandSession retains the slot identity that selected its physical
// endpoint and lane. Native transactions are connection-affine, so attempting
// to queue work for another slot must fail locally instead of silently sending
// that work to the wrong shard.
type topologyCommandSession struct {
	commandSession
	slot int
}

type topologyTransactionRouteError struct{ err error }

func (e *topologyTransactionRouteError) Error() string { return e.err.Error() }
func (e *topologyTransactionRouteError) Unwrap() error { return e.err }

func (s *topologyCommandSession) Do(ctx context.Context, args ...any) (any, error) {
	if err := validateTopologyTransactionRoute(s.slot, args); err != nil {
		return nil, &topologyTransactionRouteError{err: err}
	}
	return s.commandSession.Do(ctx, args...)
}

func validateTopologyTransactionRoute(slot int, args []any) error {
	if _, stateful := connectionStateCommand(args); stateful {
		return nil
	}
	routeArgs := topologyCommandArgs(args)
	if len(routeArgs) == 0 {
		return fmt.Errorf("topology transaction pinned to slot %d received an empty command", slot)
	}
	name := commandName(routeArgs)
	if v080TransactionLocalNoKeyCommand(name) {
		return nil
	}
	if strings.HasPrefix(name, "FLOW.") {
		key, ok := flowRoutingKey(name, routeArgs)
		if !ok {
			return fmt.Errorf("%s has no explicit slot route for a topology transaction", name)
		}
		return validateTopologyTransactionKeys(name, slot, []any{key})
	}
	policy, known := topologyCommandPolicies[name]
	if !known {
		return fmt.Errorf("%s has no slot routing policy for a topology transaction", name)
	}
	keys, ok := topologyKeysForMode(policy.keyMode, routeArgs)
	if !ok || len(keys) == 0 {
		return fmt.Errorf("%s has no valid slot route for a topology transaction", name)
	}
	return validateTopologyTransactionKeys(name, slot, keys)
}

func validateTopologyTransactionKeys(name string, pinnedSlot int, keys []any) error {
	for index, key := range keys {
		slot, ok := routingTargetSlot(key)
		if !ok {
			return fmt.Errorf("%s key %d has unsupported topology routing type %T", name, index, key)
		}
		if slot != pinnedSlot {
			return fmt.Errorf("%s key %d uses slot %d, but the transaction is pinned to slot %d", name, index, slot, pinnedSlot)
		}
	}
	return nil
}
