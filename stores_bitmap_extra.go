package ferricstore

import (
	"context"
	"errors"
	"strings"
)

func (s *BitmapStore) Pos(ctx context.Context, key string, bit int, start, end *int64) (int64, error) {
	if err := validateBitValue(bit); err != nil {
		return 0, err
	}
	if end != nil && start == nil {
		return 0, errors.New("BITPOS end requires start")
	}
	args := []any{"BITPOS", key, bit}
	if start != nil {
		args = append(args, *start)
	}
	if end != nil {
		args = append(args, *end)
	}
	value, err := s.client.typedReply(ctx, args...)
	return bitmapPositionResponse(value, err)
}

func (s *BitmapStore) Op(ctx context.Context, operation, destination string, keys ...string) (int64, error) {
	operation = strings.ToUpper(operation)
	switch operation {
	case "AND", "OR", "XOR":
		if len(keys) == 0 {
			return 0, errors.New("BITOP requires at least one source key")
		}
	case "NOT":
		if len(keys) != 1 {
			return 0, errors.New("BITOP NOT requires exactly one source key")
		}
	default:
		return 0, errors.New("BITOP operation must be AND, OR, XOR, or NOT")
	}
	args := []any{"BITOP", operation, destination}
	for _, key := range keys {
		args = append(args, key)
	}
	value, err := s.client.typedReply(ctx, args...)
	return nonNegativeInt64Response("BITOP", value, err)
}

func (s *HyperLogLogStore) Merge(ctx context.Context, destination string, sources ...string) error {
	if len(sources) == 0 {
		return errors.New("PFMERGE requires at least one source key")
	}
	args := []any{"PFMERGE", destination}
	for _, source := range sources {
		args = append(args, source)
	}
	return s.client.typedStatus(ctx, args...)
}
