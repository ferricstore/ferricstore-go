package ferricstore

import (
	"context"
	"errors"
	"fmt"
)

func (e *TopologyNativeExecutor) retryStringMGetGroup(
	ctx context.Context,
	keys []string,
) ([]any, error) {
	value, err := e.scatterCommandAttempt(ctx, "MGET", stringKeysAsAny(keys), nil, false, false)
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok || len(items) != len(keys) {
		return nil, fmt.Errorf(
			"MGET retry returned %T with %d values, expected %d", value, len(items), len(keys),
		)
	}
	return items, nil
}

func stringKeysAsAny(keys []string) []any {
	values := make([]any, len(keys))
	for index, key := range keys {
		values[index] = key
	}
	return values
}

func (e *TopologyNativeExecutor) retryStringCountGroup(
	ctx context.Context,
	name string,
	keys []string,
) (int64, []error) {
	value, err := e.scatterCommandAttempt(ctx, name, stringKeysAsAny(keys), nil, false, false)
	if err != nil {
		var partial *TopologyPartialWriteError
		if errors.As(err, &partial) {
			return partial.Succeeded, partial.Failures
		}
		return 0, []error{err}
	}
	count, err := responseInt64(value, nil)
	if err != nil {
		return 0, []error{err}
	}
	if count < 0 || count > int64(len(keys)) {
		return 0, []error{fmt.Errorf(
			"%s retry count %d is outside valid range 0..%d", name, count, len(keys),
		)}
	}
	return count, nil
}
