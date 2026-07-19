package ferricstore

import (
	"context"
	"errors"
	"strings"
)

func (s *ListStore) Len(ctx context.Context, key string) (int64, error) {
	value, err := s.client.typedReply(ctx, "LLEN", key)
	return nonNegativeInt64Response("LLEN", value, err)
}

func (s *ListStore) Index(ctx context.Context, key string, index int64) (any, error) {
	value, err := s.client.typedReply(ctx, "LINDEX", key, index)
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
	return s.client.typedStatus(ctx, "LSET", key, index, encoded)
}

func (s *ListStore) Rem(ctx context.Context, key string, count int64, value any) (int64, error) {
	encoded, err := s.client.encode(value)
	if err != nil {
		return 0, err
	}
	response, err := s.client.typedReply(ctx, "LREM", key, count, encoded)
	return nonNegativeInt64Response("LREM", response, err)
}

func (s *ListStore) Trim(ctx context.Context, key string, start, stop int64) error {
	return s.client.typedStatus(ctx, "LTRIM", key, start, stop)
}

func (s *ListStore) Pos(ctx context.Context, key string, value any, rank, count, maxLen *int64) (any, error) {
	if rank != nil && *rank == 0 {
		return nil, errors.New("LPOS rank must not be zero")
	}
	if count != nil && *count < 0 {
		return nil, errors.New("LPOS count must be non-negative")
	}
	if maxLen != nil && *maxLen < 0 {
		return nil, errors.New("LPOS maxlen must be non-negative")
	}
	encoded, err := s.client.encode(value)
	if err != nil {
		return nil, err
	}
	args := []any{"LPOS", key, encoded}
	appendInt64Ptr(&args, "RANK", rank)
	appendInt64Ptr(&args, "COUNT", count)
	appendInt64Ptr(&args, "MAXLEN", maxLen)
	response, err := s.client.typedReply(ctx, args...)
	return listPositionResponse(response, err, rank, count)
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
	response, err := s.client.typedReply(ctx, "LINSERT", key, where, encodedPivot, encodedValue)
	length, err := responseInt64(response, err)
	if err != nil {
		return 0, err
	}
	if length < -1 {
		return 0, errors.New("LINSERT returned an invalid negative length")
	}
	return length, nil
}

func (s *ListStore) Move(ctx context.Context, source, destination, whereFrom, whereTo string) (any, error) {
	if !validListDirection(whereFrom) || !validListDirection(whereTo) {
		return nil, errors.New("LMOVE directions must be LEFT or RIGHT")
	}
	value, err := s.client.typedReply(ctx, "LMOVE", source, destination, whereFrom, whereTo)
	if err != nil || value == nil {
		return value, err
	}
	return s.client.codec.Decode(value)
}

func (s *ListStore) RPopLPush(ctx context.Context, source, destination string) (any, error) {
	value, err := s.client.typedReply(ctx, "RPOPLPUSH", source, destination)
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
	if len(keys) == 0 {
		return nil, errors.New("BLPOP requires at least one key")
	}
	if err := validateBlockingTimeoutSeconds("BLPOP", timeoutSeconds); err != nil {
		return nil, err
	}
	args := []any{"BLPOP"}
	for _, key := range keys {
		args = append(args, key)
	}
	args = append(args, timeoutSeconds)
	value, err := s.client.typedReply(ctx, args...)
	return decodeBlockingListPop(s.client.codec, value, err, "BLPOP", keys)
}

func (s *ListStore) BRPop(ctx context.Context, timeoutSeconds int64, keys ...string) (any, error) {
	if len(keys) == 0 {
		return nil, errors.New("BRPOP requires at least one key")
	}
	if err := validateBlockingTimeoutSeconds("BRPOP", timeoutSeconds); err != nil {
		return nil, err
	}
	args := []any{"BRPOP"}
	for _, key := range keys {
		args = append(args, key)
	}
	args = append(args, timeoutSeconds)
	value, err := s.client.typedReply(ctx, args...)
	return decodeBlockingListPop(s.client.codec, value, err, "BRPOP", keys)
}

func (s *ListStore) BLMove(ctx context.Context, source, destination, whereFrom, whereTo string, timeoutSeconds int64) (any, error) {
	if !validListDirection(whereFrom) || !validListDirection(whereTo) {
		return nil, errors.New("BLMOVE directions must be LEFT or RIGHT")
	}
	if err := validateBlockingTimeoutSeconds("BLMOVE", timeoutSeconds); err != nil {
		return nil, err
	}
	value, err := s.client.typedReply(ctx, "BLMOVE", source, destination, whereFrom, whereTo, timeoutSeconds)
	if err != nil || value == nil {
		return value, err
	}
	return s.client.codec.Decode(value)
}

func (s *ListStore) BLMPop(ctx context.Context, timeoutSeconds float64, keys []string, where string, count *int) (any, error) {
	if err := validateBlockingTimeoutSecondsFloat("BLMPOP", timeoutSeconds); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, errors.New("BLMPOP requires at least one key")
	}
	if !validListDirection(where) {
		return nil, errors.New("BLMPOP direction must be LEFT or RIGHT")
	}
	if count != nil && *count <= 0 {
		return nil, errors.New("BLMPOP count must be positive")
	}
	args := []any{"BLMPOP", floatArg(timeoutSeconds), len(keys)}
	for _, key := range keys {
		args = append(args, key)
	}
	args = append(args, where)
	appendIntPtr(&args, "COUNT", count)
	value, err := s.client.typedReply(ctx, args...)
	return decodeBlockingListMPop(s.client.codec, value, err, keys, count)
}

func validListDirection(direction string) bool {
	return strings.EqualFold(direction, "LEFT") || strings.EqualFold(direction, "RIGHT")
}
