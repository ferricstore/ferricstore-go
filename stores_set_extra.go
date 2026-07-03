package ferricstore

import "context"

func (s *SetStore) IsMember(ctx context.Context, key string, member any) (bool, error) {
	encoded, err := s.client.encode(member)
	if err != nil {
		return false, err
	}
	value, err := s.client.Command(ctx, "SISMEMBER", key, encoded)
	return asBool(value), err
}

func (s *SetStore) MIsMember(ctx context.Context, key string, members ...any) ([]bool, error) {
	args := []any{"SMISMEMBER", key}
	for _, member := range members {
		encoded, err := s.client.encode(member)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.Command(ctx, args...)
	return boolArray(value, err)
}

func (s *SetStore) Card(ctx context.Context, key string) (int64, error) {
	value, err := s.client.Command(ctx, "SCARD", key)
	return asInt64(value), err
}

func (s *SetStore) RandMember(ctx context.Context, key string, count *int) ([]any, error) {
	args := []any{"SRANDMEMBER", key}
	if count != nil {
		args = append(args, *count)
	}
	value, err := s.client.Command(ctx, args...)
	if count == nil && err == nil && value != nil {
		decoded, err := s.client.codec.Decode(value)
		return []any{decoded}, err
	}
	return decodeArray(s.client.codec, value, err)
}

func (s *SetStore) Pop(ctx context.Context, key string, count *int) ([]any, error) {
	args := []any{"SPOP", key}
	if count != nil {
		args = append(args, *count)
	}
	value, err := s.client.Command(ctx, args...)
	if count == nil && err == nil && value != nil {
		decoded, err := s.client.codec.Decode(value)
		return []any{decoded}, err
	}
	return decodeArray(s.client.codec, value, err)
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
	args := []any{command}
	for _, key := range keys {
		args = append(args, key)
	}
	value, err := s.client.Command(ctx, args...)
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
	args := []any{command, destination}
	for _, key := range keys {
		args = append(args, key)
	}
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *SetStore) InterCard(ctx context.Context, keys []string, limit *int64) (int64, error) {
	args := []any{"SINTERCARD", len(keys)}
	for _, key := range keys {
		args = append(args, key)
	}
	appendInt64Ptr(&args, "LIMIT", limit)
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *SetStore) Move(ctx context.Context, source, destination string, member any) (bool, error) {
	encoded, err := s.client.encode(member)
	if err != nil {
		return false, err
	}
	value, err := s.client.Command(ctx, "SMOVE", source, destination, encoded)
	return asBool(value), err
}

func (s *SetStore) Scan(ctx context.Context, key string, cursor int64, match string, count *int) (any, error) {
	args := []any{"SSCAN", key, cursor}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	appendScanCount(&args, count)
	return s.client.Command(ctx, args...)
}
