package ferricstore

import "context"

type ZAddOptions struct {
	NX bool
	XX bool
	GT bool
	LT bool
	CH bool
}

func (s *SortedSetStore) AddWithOptions(ctx context.Context, key string, opt ZAddOptions, members ...ZAddMember) (int64, error) {
	args := []any{"ZADD", key}
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
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *SortedSetStore) Score(ctx context.Context, key string, member any) (float64, error) {
	encoded, err := s.client.encode(member)
	if err != nil {
		return 0, err
	}
	value, err := s.client.Command(ctx, "ZSCORE", key, encoded)
	return asFloat64(value), err
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
	value, err := s.client.Command(ctx, command, key, encoded)
	return asInt64(value), err
}

func (s *SortedSetStore) RevRange(ctx context.Context, key string, start, stop int64) ([]any, error) {
	value, err := s.client.Command(ctx, "ZREVRANGE", key, start, stop)
	return decodeArray(s.client.codec, value, err)
}

func (s *SortedSetStore) Card(ctx context.Context, key string) (int64, error) {
	value, err := s.client.Command(ctx, "ZCARD", key)
	return asInt64(value), err
}

func (s *SortedSetStore) Rem(ctx context.Context, key string, members ...any) (int64, error) {
	args := []any{"ZREM", key}
	for _, member := range members {
		encoded, err := s.client.encode(member)
		if err != nil {
			return 0, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *SortedSetStore) IncrBy(ctx context.Context, key string, increment float64, member any) (float64, error) {
	encoded, err := s.client.encode(member)
	if err != nil {
		return 0, err
	}
	value, err := s.client.Command(ctx, "ZINCRBY", key, increment, encoded)
	return asFloat64(value), err
}

func (s *SortedSetStore) Count(ctx context.Context, key, min, max string) (int64, error) {
	value, err := s.client.Command(ctx, "ZCOUNT", key, min, max)
	return asInt64(value), err
}

func (s *SortedSetStore) PopMin(ctx context.Context, key string, count *int) (any, error) {
	return s.zpop(ctx, "ZPOPMIN", key, count)
}

func (s *SortedSetStore) PopMax(ctx context.Context, key string, count *int) (any, error) {
	return s.zpop(ctx, "ZPOPMAX", key, count)
}

func (s *SortedSetStore) zpop(ctx context.Context, command, key string, count *int) (any, error) {
	args := []any{command, key}
	if count != nil {
		args = append(args, *count)
	}
	return s.client.Command(ctx, args...)
}

func (s *SortedSetStore) RandMember(ctx context.Context, key string, count *int, withScores bool) (any, error) {
	args := []any{"ZRANDMEMBER", key}
	if count != nil {
		args = append(args, *count)
	}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	return s.client.Command(ctx, args...)
}

func (s *SortedSetStore) MScore(ctx context.Context, key string, members ...any) ([]float64, error) {
	args := []any{"ZMSCORE", key}
	for _, member := range members {
		encoded, err := s.client.encode(member)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.Command(ctx, args...)
	return floatArray(value, err)
}

func (s *SortedSetStore) RangeByScore(ctx context.Context, key, min, max string, withScores bool, limitOffset, limitCount *int64) (any, error) {
	args := []any{"ZRANGEBYSCORE", key, min, max}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	if limitOffset != nil && limitCount != nil {
		args = append(args, "LIMIT", *limitOffset, *limitCount)
	}
	return s.client.Command(ctx, args...)
}

func (s *SortedSetStore) RevRangeByScore(ctx context.Context, key, max, min string, withScores bool, limitOffset, limitCount *int64) (any, error) {
	args := []any{"ZREVRANGEBYSCORE", key, max, min}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	if limitOffset != nil && limitCount != nil {
		args = append(args, "LIMIT", *limitOffset, *limitCount)
	}
	return s.client.Command(ctx, args...)
}

func (s *SortedSetStore) Scan(ctx context.Context, key string, cursor int64, match string, count *int) (any, error) {
	args := []any{"ZSCAN", key, cursor}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	appendScanCount(&args, count)
	return s.client.Command(ctx, args...)
}
