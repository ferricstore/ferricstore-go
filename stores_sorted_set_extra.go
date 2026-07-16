package ferricstore

import (
	"context"
	"errors"
)

type ZAddOptions struct {
	NX bool
	XX bool
	GT bool
	LT bool
	CH bool
}

func (s *SortedSetStore) AddWithOptions(ctx context.Context, key string, opt ZAddOptions, members ...ZAddMember) (int64, error) {
	if opt.NX && opt.XX {
		return 0, errors.New("ZADD NX and XX are mutually exclusive")
	}
	if opt.GT && opt.LT {
		return 0, errors.New("ZADD GT and LT are mutually exclusive")
	}
	if opt.NX && (opt.GT || opt.LT) {
		return 0, errors.New("ZADD NX cannot be combined with GT or LT")
	}
	if len(members) == 0 {
		return 0, nil
	}
	if err := validateZAddMembers(members); err != nil {
		return 0, err
	}
	args := make([]any, 2, len(members)*2+7)
	args[0], args[1] = "ZADD", key
	if opt.NX {
		args = append(args, "NX")
	}
	if opt.XX {
		args = append(args, "XX")
	}
	if opt.GT {
		args = append(args, "GT")
	}
	if opt.LT {
		args = append(args, "LT")
	}
	if opt.CH {
		args = append(args, "CH")
	}
	for _, member := range members {
		encoded, err := s.client.encode(member.Member)
		if err != nil {
			return 0, err
		}
		args = append(args, member.Score, encoded)
	}
	value, err := s.client.typedReply(ctx, args...)
	return boundedCountResponse("ZADD", len(members), value, err)
}

func (s *SortedSetStore) Score(ctx context.Context, key string, member any) (float64, error) {
	encoded, err := s.client.encode(member)
	if err != nil {
		return 0, err
	}
	value, err := s.client.typedReply(ctx, "ZSCORE", key, encoded)
	return responseFloat64(value, err)
}

func (s *SortedSetStore) Rank(ctx context.Context, key string, member any) (int64, error) {
	return s.rank(ctx, "ZRANK", key, member)
}

func (s *SortedSetStore) RevRank(ctx context.Context, key string, member any) (int64, error) {
	return s.rank(ctx, "ZREVRANK", key, member)
}

func (s *SortedSetStore) rank(ctx context.Context, command, key string, member any) (int64, error) {
	encoded, err := s.client.encode(member)
	if err != nil {
		return 0, err
	}
	value, err := s.client.typedReply(ctx, command, key, encoded)
	return nonNegativeInt64Response(command, value, err)
}

func (s *SortedSetStore) RevRange(ctx context.Context, key string, start, stop int64) ([]any, error) {
	value, err := s.client.typedReply(ctx, "ZREVRANGE", key, start, stop)
	return decodeArray(s.client.codec, value, err)
}

func (s *SortedSetStore) Card(ctx context.Context, key string) (int64, error) {
	value, err := s.client.typedReply(ctx, "ZCARD", key)
	return nonNegativeInt64Response("ZCARD", value, err)
}

func (s *SortedSetStore) Rem(ctx context.Context, key string, members ...any) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	args := make([]any, 2, len(members)+2)
	args[0], args[1] = "ZREM", key
	for _, member := range members {
		encoded, err := s.client.encode(member)
		if err != nil {
			return 0, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.typedReply(ctx, args...)
	return boundedCountResponse("ZREM", len(members), value, err)
}

func (s *SortedSetStore) IncrBy(ctx context.Context, key string, increment float64, member any) (float64, error) {
	if err := validateFiniteFloat("ZINCRBY", increment); err != nil {
		return 0, err
	}
	encoded, err := s.client.encode(member)
	if err != nil {
		return 0, err
	}
	value, err := s.client.typedReply(ctx, "ZINCRBY", key, increment, encoded)
	return responseFloat64(value, err)
}

func (s *SortedSetStore) Count(ctx context.Context, key, min, max string) (int64, error) {
	value, err := s.client.typedReply(ctx, "ZCOUNT", key, min, max)
	return nonNegativeInt64Response("ZCOUNT", value, err)
}

func validateZAddMembers(members []ZAddMember) error {
	for _, member := range members {
		if err := validateFiniteFloat("ZADD score", member.Score); err != nil {
			return err
		}
	}
	return nil
}

func (s *SortedSetStore) PopMin(ctx context.Context, key string, count *int) (any, error) {
	return s.zpop(ctx, "ZPOPMIN", key, count)
}

func (s *SortedSetStore) PopMax(ctx context.Context, key string, count *int) (any, error) {
	return s.zpop(ctx, "ZPOPMAX", key, count)
}

func (s *SortedSetStore) zpop(ctx context.Context, command, key string, count *int) (any, error) {
	if count != nil && *count < 0 {
		return nil, errors.New(command + " count must be non-negative")
	}
	args := []any{command, key}
	if count != nil {
		args = append(args, *count)
	}
	value, err := s.client.typedReply(ctx, args...)
	maximum := uint64(1)
	if count != nil {
		maximum = uint64(*count)
	}
	order := sortedSetScoresAscending
	if command == "ZPOPMAX" {
		order = sortedSetScoresDescending
	}
	return decodeSortedSetPairs(s.client.codec, value, err, maximum, true, order, command)
}

func (s *SortedSetStore) RandMember(ctx context.Context, key string, count *int, withScores bool) (any, error) {
	if withScores && count == nil {
		return nil, errors.New("ZRANDMEMBER WITHSCORES requires count")
	}
	args := []any{"ZRANDMEMBER", key}
	if count != nil {
		args = append(args, *count)
	}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	value, err := s.client.typedReply(ctx, args...)
	if value == nil || err != nil {
		return value, err
	}
	if count == nil {
		return decodeValue(s.client.codec, value)
	}
	maximum := countMagnitude(*count)
	if withScores {
		return decodeSortedSetPairs(s.client.codec, value, nil, maximum, true, sortedSetScoresUnordered, "ZRANDMEMBER")
	}
	return decodeArrayWithLimit(s.client.codec, value, nil, maximum, "ZRANDMEMBER")
}

func (s *SortedSetStore) MScore(ctx context.Context, key string, members ...any) ([]float64, error) {
	if len(members) == 0 {
		return nil, nil
	}
	args := make([]any, 2, len(members)+2)
	args[0], args[1] = "ZMSCORE", key
	for _, member := range members {
		encoded, err := s.client.encode(member)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.typedReply(ctx, args...)
	return nullableFloatArrayExact(value, err, len(members), "ZMSCORE")
}

func (s *SortedSetStore) RangeByScore(ctx context.Context, key, min, max string, withScores bool, limitOffset, limitCount *int64) (any, error) {
	if (limitOffset == nil) != (limitCount == nil) {
		return nil, errors.New("ZRANGEBYSCORE LIMIT requires both offset and count")
	}
	if limitOffset != nil && *limitOffset < 0 {
		return nil, errors.New("ZRANGEBYSCORE LIMIT offset must be non-negative")
	}
	args := []any{"ZRANGEBYSCORE", key, min, max}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	if limitOffset != nil && limitCount != nil {
		args = append(args, "LIMIT", *limitOffset, *limitCount)
	}
	value, err := s.client.typedReply(ctx, args...)
	if withScores {
		maximum, limited := rangeResultLimit(limitCount)
		return decodeSortedSetPairs(s.client.codec, value, err, maximum, limited, sortedSetScoresAscending, "ZRANGEBYSCORE")
	}
	if maximum, limited := rangeResultLimit(limitCount); limited {
		return decodeArrayWithLimit(s.client.codec, value, err, maximum, "ZRANGEBYSCORE")
	}
	return decodeArray(s.client.codec, value, err)
}

func (s *SortedSetStore) RevRangeByScore(ctx context.Context, key, max, min string, withScores bool, limitOffset, limitCount *int64) (any, error) {
	if (limitOffset == nil) != (limitCount == nil) {
		return nil, errors.New("ZREVRANGEBYSCORE LIMIT requires both offset and count")
	}
	if limitOffset != nil && *limitOffset < 0 {
		return nil, errors.New("ZREVRANGEBYSCORE LIMIT offset must be non-negative")
	}
	args := []any{"ZREVRANGEBYSCORE", key, max, min}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	if limitOffset != nil && limitCount != nil {
		args = append(args, "LIMIT", *limitOffset, *limitCount)
	}
	value, err := s.client.typedReply(ctx, args...)
	if withScores {
		maximum, limited := rangeResultLimit(limitCount)
		return decodeSortedSetPairs(s.client.codec, value, err, maximum, limited, sortedSetScoresDescending, "ZREVRANGEBYSCORE")
	}
	if maximum, limited := rangeResultLimit(limitCount); limited {
		return decodeArrayWithLimit(s.client.codec, value, err, maximum, "ZREVRANGEBYSCORE")
	}
	return decodeArray(s.client.codec, value, err)
}

func (s *SortedSetStore) Scan(ctx context.Context, key string, cursor int64, match string, count *int) (any, error) {
	return s.scanCursor(ctx, key, cursor, match, count)
}

// ScanCursor continues ZSCAN using an opaque cursor returned by FerricStore.
func (s *SortedSetStore) ScanCursor(ctx context.Context, key string, cursor any, match string, count *int) (any, error) {
	return s.scanCursor(ctx, key, cursor, match, count)
}

func (s *SortedSetStore) scanCursor(ctx context.Context, key string, cursor any, match string, count *int) (any, error) {
	normalizedCursor, err := normalizeScanCursor(cursor, true)
	if err != nil {
		return nil, err
	}
	if err := validateScanCount(count); err != nil {
		return nil, err
	}
	args := []any{"ZSCAN", key, normalizedCursor}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	appendScanCount(&args, count)
	value, err := s.client.typedReply(ctx, args...)
	return decodeCollectionScan(s.client.codec, value, err, sortedSetCollectionScan, "ZSCAN")
}

func rangeResultLimit(count *int64) (uint64, bool) {
	if count == nil || *count < 0 {
		return 0, false
	}
	return uint64(*count), true
}
