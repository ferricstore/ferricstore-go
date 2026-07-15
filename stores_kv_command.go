package ferricstore

import (
	"context"
	"fmt"
)

func (s *KeyValueStore) commandReply(ctx context.Context, args ...any) (any, error) {
	value, _, err := s.commandWithState(ctx, false, args)
	return value, err
}

func (s *KeyValueStore) commandReplyDirect(
	ctx context.Context,
	direct typedDirectCommand,
	fallback func() []any,
) (any, error) {
	value, _, err := s.commandWithStateLazy(ctx, false, direct, fallback)
	return value, err
}

func (s *KeyValueStore) commandStatus(ctx context.Context, args ...any) error {
	value, queued, err := s.commandWithState(ctx, true, args)
	return keyValueStatusResponse(value, queued, err)
}

func (s *KeyValueStore) commandStatusDirect(
	ctx context.Context,
	direct typedDirectCommand,
	fallback func() []any,
) error {
	value, queued, err := s.commandWithStateLazy(ctx, true, direct, fallback)
	return keyValueStatusResponse(value, queued, err)
}

func (s *KeyValueStore) commandWithState(
	ctx context.Context,
	allowQueued bool,
	args []any,
) (any, bool, error) {
	return s.client.typedCommandWithState(ctx, allowQueued, nil, func() []any { return args })
}

func (s *KeyValueStore) commandWithStateLazy(
	ctx context.Context,
	allowQueued bool,
	direct typedDirectCommand,
	fallback func() []any,
) (any, bool, error) {
	return s.client.typedCommandWithState(ctx, allowQueued, direct, fallback)
}

func keyValueStatusResponse(value any, queued bool, err error) error {
	return typedStatusResponse(value, queued, err)
}

func keyValueCountResponse(command string, requested int, value any, err error) (int64, error) {
	return boundedCountResponse(command, requested, value, err)
}

func keyValueLengthResponse(command string, value any, err error) (int64, error) {
	length, err := responseInt64(value, err)
	if err != nil {
		return 0, err
	}
	if length < 0 {
		return 0, fmt.Errorf("%s length %d is negative", command, length)
	}
	return length, nil
}

func keyValueTTLResponse(command string, value any, err error) (int64, error) {
	ttl, err := responseInt64(value, err)
	if err != nil {
		return 0, err
	}
	if ttl < -2 {
		return 0, fmt.Errorf("%s returned unknown negative sentinel %d", command, ttl)
	}
	return ttl, nil
}
