package ferricstore

import "context"

func (s *ListStore) Len(ctx context.Context, key string) (int64, error) {
	value, err := s.client.Command(ctx, "LLEN", key)
	return asInt64(value), err
}

func (s *ListStore) Index(ctx context.Context, key string, index int64) (any, error) {
	value, err := s.client.Command(ctx, "LINDEX", key, index)
	if err != nil || value == nil {
		return value, err
	}
	return s.client.codec.Decode(value)
}

func (s *ListStore) Set(ctx context.Context, key string, index int64, value any) error {
	encoded, err := s.client.encode(value)
	if err != nil {
		return err
	}
	_, err = s.client.Command(ctx, "LSET", key, index, encoded)
	return err
}

func (s *ListStore) Rem(ctx context.Context, key string, count int64, value any) (int64, error) {
	encoded, err := s.client.encode(value)
	if err != nil {
		return 0, err
	}
	response, err := s.client.Command(ctx, "LREM", key, count, encoded)
	return asInt64(response), err
}

func (s *ListStore) Trim(ctx context.Context, key string, start, stop int64) error {
	_, err := s.client.Command(ctx, "LTRIM", key, start, stop)
	return err
}

func (s *ListStore) Pos(ctx context.Context, key string, value any, rank, count, maxLen *int64) (any, error) {
	encoded, err := s.client.encode(value)
	if err != nil {
		return nil, err
	}
	args := []any{"LPOS", key, encoded}
	appendInt64Ptr(&args, "RANK", rank)
	appendInt64Ptr(&args, "COUNT", count)
	appendInt64Ptr(&args, "MAXLEN", maxLen)
	return s.client.Command(ctx, args...)
}

func (s *ListStore) Insert(ctx context.Context, key string, before bool, pivot, value any) (int64, error) {
	encodedPivot, err := s.client.encode(pivot)
	if err != nil {
		return 0, err
	}
	encodedValue, err := s.client.encode(value)
	if err != nil {
		return 0, err
	}
	where := "AFTER"
	if before {
		where = "BEFORE"
	}
	response, err := s.client.Command(ctx, "LINSERT", key, where, encodedPivot, encodedValue)
	return asInt64(response), err
}

func (s *ListStore) Move(ctx context.Context, source, destination, whereFrom, whereTo string) (any, error) {
	value, err := s.client.Command(ctx, "LMOVE", source, destination, whereFrom, whereTo)
	if err != nil || value == nil {
		return value, err
	}
	return s.client.codec.Decode(value)
}

func (s *ListStore) RPopLPush(ctx context.Context, source, destination string) (any, error) {
	value, err := s.client.Command(ctx, "RPOPLPUSH", source, destination)
	if err != nil || value == nil {
		return value, err
	}
	return s.client.codec.Decode(value)
}

func (s *ListStore) LPushX(ctx context.Context, key string, values ...any) (int64, error) {
	return s.push(ctx, "LPUSHX", key, values...)
}

func (s *ListStore) RPushX(ctx context.Context, key string, values ...any) (int64, error) {
	return s.push(ctx, "RPUSHX", key, values...)
}

func (s *ListStore) BLPop(ctx context.Context, timeoutSeconds int64, keys ...string) (any, error) {
	args := []any{"BLPOP"}
	for _, key := range keys {
		args = append(args, key)
	}
	args = append(args, timeoutSeconds)
	return s.client.Command(ctx, args...)
}

func (s *ListStore) BRPop(ctx context.Context, timeoutSeconds int64, keys ...string) (any, error) {
	args := []any{"BRPOP"}
	for _, key := range keys {
		args = append(args, key)
	}
	args = append(args, timeoutSeconds)
	return s.client.Command(ctx, args...)
}

func (s *ListStore) BLMove(ctx context.Context, source, destination, whereFrom, whereTo string, timeoutSeconds int64) (any, error) {
	value, err := s.client.Command(ctx, "BLMOVE", source, destination, whereFrom, whereTo, timeoutSeconds)
	if err != nil || value == nil {
		return value, err
	}
	return s.client.codec.Decode(value)
}

func (s *ListStore) BLMPop(ctx context.Context, timeoutSeconds float64, keys []string, where string, count *int) (any, error) {
	args := []any{"BLMPOP", floatArg(timeoutSeconds), len(keys)}
	for _, key := range keys {
		args = append(args, key)
	}
	args = append(args, where)
	appendIntPtr(&args, "COUNT", count)
	return s.client.Command(ctx, args...)
}
