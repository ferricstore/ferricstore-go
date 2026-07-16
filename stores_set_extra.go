package ferricstore

import (
	"context"
	"errors"
)

func (s *SetStore) IsMember(ctx context.Context, key string, member any) (bool, error) {
	encoded, err := s.client.encode(member)
	if err != nil {
		return false, err
	}
	value, err := s.client.typedReply(ctx, "SISMEMBER", key, encoded)
	return responseBool(value, err)
}

func (s *SetStore) MIsMember(ctx context.Context, key string, members ...any) ([]bool, error) {
	if len(members) == 0 {
		return nil, nil
	}
	args := make([]any, 2, len(members)+2)
	args[0], args[1] = "SMISMEMBER", key
	for _, member := range members {
		encoded, err := s.client.encode(member)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.typedReply(ctx, args...)
	return boolArrayExact(value, err, len(members), "SMISMEMBER")
}

func (s *SetStore) Card(ctx context.Context, key string) (int64, error) {
	value, err := s.client.typedReply(ctx, "SCARD", key)
	return nonNegativeInt64Response("SCARD", value, err)
}

func (s *SetStore) RandMember(ctx context.Context, key string, count *int) ([]any, error) {
	args := []any{"SRANDMEMBER", key}
	if count != nil {
		args = append(args, *count)
	}
	value, err := s.client.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	if count == nil {
		decoded, err := s.client.codec.Decode(value)
		return []any{decoded}, err
	}
	return decodeArrayWithLimit(s.client.codec, value, nil, countMagnitude(*count), "SRANDMEMBER")
}

func (s *SetStore) Pop(ctx context.Context, key string, count *int) ([]any, error) {
	if count != nil && *count < 0 {
		return nil, errors.New("SPOP count must be non-negative")
	}
	args := []any{"SPOP", key}
	if count != nil {
		args = append(args, *count)
	}
	value, err := s.client.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	if count == nil {
		decoded, err := s.client.codec.Decode(value)
		return []any{decoded}, err
	}
	return decodeArrayWithLimit(s.client.codec, value, nil, uint64(*count), "SPOP")
}

func (s *SetStore) Diff(ctx context.Context, keys ...string) ([]any, error) {
	return s.setOp(ctx, "SDIFF", keys...)
}

func (s *SetStore) Inter(ctx context.Context, keys ...string) ([]any, error) {
	return s.setOp(ctx, "SINTER", keys...)
}

func (s *SetStore) Union(ctx context.Context, keys ...string) ([]any, error) {
	return s.setOp(ctx, "SUNION", keys...)
}

func (s *SetStore) setOp(ctx context.Context, command string, keys ...string) ([]any, error) {
	if len(keys) == 0 {
		return nil, errors.New(command + " requires at least one key")
	}
	args := []any{command}
	for _, key := range keys {
		args = append(args, key)
	}
	value, err := s.client.typedReply(ctx, args...)
	return decodeArray(s.client.codec, value, err)
}

func (s *SetStore) DiffStore(ctx context.Context, destination string, keys ...string) (int64, error) {
	return s.setStoreOp(ctx, "SDIFFSTORE", destination, keys...)
}

func (s *SetStore) InterStore(ctx context.Context, destination string, keys ...string) (int64, error) {
	return s.setStoreOp(ctx, "SINTERSTORE", destination, keys...)
}

func (s *SetStore) UnionStore(ctx context.Context, destination string, keys ...string) (int64, error) {
	return s.setStoreOp(ctx, "SUNIONSTORE", destination, keys...)
}

func (s *SetStore) setStoreOp(ctx context.Context, command, destination string, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, errors.New(command + " requires at least one source key")
	}
	args := []any{command, destination}
	for _, key := range keys {
		args = append(args, key)
	}
	value, err := s.client.typedReply(ctx, args...)
	return nonNegativeInt64Response(command, value, err)
}

func (s *SetStore) InterCard(ctx context.Context, keys []string, limit *int64) (int64, error) {
	if len(keys) == 0 {
		return 0, errors.New("SINTERCARD requires at least one key")
	}
	if limit != nil && *limit < 0 {
		return 0, errors.New("SINTERCARD limit must be non-negative")
	}
	args := []any{"SINTERCARD", len(keys)}
	for _, key := range keys {
		args = append(args, key)
	}
	appendInt64Ptr(&args, "LIMIT", limit)
	value, err := s.client.typedReply(ctx, args...)
	count, err := nonNegativeInt64Response("SINTERCARD", value, err)
	if err != nil {
		return 0, err
	}
	if limit != nil && *limit > 0 && count > *limit {
		return 0, errors.New("SINTERCARD returned a count above LIMIT")
	}
	return count, nil
}

func (s *SetStore) Move(ctx context.Context, source, destination string, member any) (bool, error) {
	encoded, err := s.client.encode(member)
	if err != nil {
		return false, err
	}
	value, err := s.client.typedReply(ctx, "SMOVE", source, destination, encoded)
	return responseBool(value, err)
}

func (s *SetStore) Scan(ctx context.Context, key string, cursor int64, match string, count *int) (any, error) {
	return s.scanCursor(ctx, key, cursor, match, count)
}

// ScanCursor continues SSCAN using an opaque cursor returned by FerricStore.
func (s *SetStore) ScanCursor(ctx context.Context, key string, cursor any, match string, count *int) (any, error) {
	return s.scanCursor(ctx, key, cursor, match, count)
}

func (s *SetStore) scanCursor(ctx context.Context, key string, cursor any, match string, count *int) (any, error) {
	normalizedCursor, err := normalizeScanCursor(cursor, true)
	if err != nil {
		return nil, err
	}
	if err := validateScanCount(count); err != nil {
		return nil, err
	}
	args := []any{"SSCAN", key, normalizedCursor}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	appendScanCount(&args, count)
	value, err := s.client.typedReply(ctx, args...)
	return decodeCollectionScan(s.client.codec, value, err, setCollectionScan, "SSCAN")
}
