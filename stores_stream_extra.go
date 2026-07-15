package ferricstore

import "context"

func (s *StreamStore) Len(ctx context.Context, key string) (int64, error) {
	value, err := s.client.typedReply(ctx, "XLEN", key)
	return nonNegativeInt64Response("XLEN", value, err)
}

func (s *StreamStore) RevRange(ctx context.Context, key, end, start string, count *int) (any, error) {
	if err := validateNonNegativeCount("XREVRANGE", count); err != nil {
		return nil, err
	}
	args := []any{"XREVRANGE", key, end, start}
	appendIntPtr(&args, "COUNT", count)
	value, err := s.client.typedReply(ctx, args...)
	return decodeStreamEntries(s.client.codec, value, err)
}

type StreamReadOptions struct {
	Count   *int
	BlockMS *int64
	Streams []StreamRef
}

type StreamRef struct {
	Key string
	ID  string
}

func (s *StreamStore) Read(ctx context.Context, opt StreamReadOptions) (any, error) {
	if err := validateStreamRead("XREAD", opt.Count, opt.BlockMS, opt.Streams); err != nil {
		return nil, err
	}
	args := []any{"XREAD"}
	appendIntPtr(&args, "COUNT", opt.Count)
	appendInt64Ptr(&args, "BLOCK", opt.BlockMS)
	args = append(args, "STREAMS")
	for _, stream := range opt.Streams {
		args = append(args, stream.Key)
	}
	for _, stream := range opt.Streams {
		args = append(args, stream.ID)
	}
	value, err := s.client.typedReply(ctx, args...)
	return decodeStreamRead(s.client.codec, value, err)
}

func (s *StreamStore) Trim(ctx context.Context, key string, approximate bool, threshold string, limit *int) (int64, error) {
	if err := validateStreamTrim(threshold, limit); err != nil {
		return 0, err
	}
	args := []any{"XTRIM", key, "MAXLEN"}
	if approximate {
		args = append(args, "~")
	}
	args = append(args, threshold)
	appendIntPtr(&args, "LIMIT", limit)
	value, err := s.client.typedReply(ctx, args...)
	return nonNegativeInt64Response("XTRIM", value, err)
}

func (s *StreamStore) Del(ctx context.Context, key string, ids ...string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	args := make([]any, 2, len(ids)+2)
	args[0], args[1] = "XDEL", key
	for _, id := range ids {
		args = append(args, id)
	}
	value, err := s.client.typedReply(ctx, args...)
	return boundedCountResponse("XDEL", len(ids), value, err)
}

func (s *StreamStore) Info(ctx context.Context, key string) (any, error) {
	return s.client.typedReply(ctx, "XINFO", "STREAM", key)
}

func (s *StreamStore) GroupCreate(ctx context.Context, key, group, id string, mkStream bool) error {
	args := []any{"XGROUP", "CREATE", key, group, id}
	if mkStream {
		args = append(args, "MKSTREAM")
	}
	return s.client.typedStatus(ctx, args...)
}

type StreamReadGroupOptions struct {
	Group    string
	Consumer string
	Count    *int
	BlockMS  *int64
	Streams  []StreamRef
}

func (s *StreamStore) ReadGroup(ctx context.Context, opt StreamReadGroupOptions) (any, error) {
	if err := validateStreamRead("XREADGROUP", opt.Count, opt.BlockMS, opt.Streams); err != nil {
		return nil, err
	}
	args := []any{"XREADGROUP", "GROUP", opt.Group, opt.Consumer}
	appendIntPtr(&args, "COUNT", opt.Count)
	appendInt64Ptr(&args, "BLOCK", opt.BlockMS)
	args = append(args, "STREAMS")
	for _, stream := range opt.Streams {
		args = append(args, stream.Key)
	}
	for _, stream := range opt.Streams {
		args = append(args, stream.ID)
	}
	value, err := s.client.typedReply(ctx, args...)
	return decodeStreamRead(s.client.codec, value, err)
}

func (s *StreamStore) Ack(ctx context.Context, key, group string, ids ...string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	args := make([]any, 3, len(ids)+3)
	args[0], args[1], args[2] = "XACK", key, group
	for _, id := range ids {
		args = append(args, id)
	}
	value, err := s.client.typedReply(ctx, args...)
	return boundedCountResponse("XACK", len(ids), value, err)
}
