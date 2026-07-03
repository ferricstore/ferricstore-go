package ferricstore

import "context"

func (s *StreamStore) Len(ctx context.Context, key string) (int64, error) {
	value, err := s.client.Command(ctx, "XLEN", key)
	return asInt64(value), err
}

func (s *StreamStore) RevRange(ctx context.Context, key, end, start string, count *int) (any, error) {
	args := []any{"XREVRANGE", key, end, start}
	appendIntPtr(&args, "COUNT", count)
	return s.client.Command(ctx, args...)
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
	return s.client.Command(ctx, args...)
}

func (s *StreamStore) Trim(ctx context.Context, key string, approximate bool, threshold string, limit *int) (int64, error) {
	args := []any{"XTRIM", key, "MAXLEN"}
	if approximate {
		args = append(args, "~")
	}
	args = append(args, threshold)
	appendIntPtr(&args, "LIMIT", limit)
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *StreamStore) Del(ctx context.Context, key string, ids ...string) (int64, error) {
	args := []any{"XDEL", key}
	for _, id := range ids {
		args = append(args, id)
	}
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *StreamStore) Info(ctx context.Context, key string) (any, error) {
	return s.client.Command(ctx, "XINFO", "STREAM", key)
}

func (s *StreamStore) GroupCreate(ctx context.Context, key, group, id string, mkStream bool) error {
	args := []any{"XGROUP", "CREATE", key, group, id}
	if mkStream {
		args = append(args, "MKSTREAM")
	}
	_, err := s.client.Command(ctx, args...)
	return err
}

type StreamReadGroupOptions struct {
	Group    string
	Consumer string
	Count    *int
	BlockMS  *int64
	Streams  []StreamRef
}

func (s *StreamStore) ReadGroup(ctx context.Context, opt StreamReadGroupOptions) (any, error) {
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
	return s.client.Command(ctx, args...)
}

func (s *StreamStore) Ack(ctx context.Context, key, group string, ids ...string) (int64, error) {
	args := []any{"XACK", key, group}
	for _, id := range ids {
		args = append(args, id)
	}
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}
