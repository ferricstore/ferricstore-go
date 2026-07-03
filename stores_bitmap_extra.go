package ferricstore

import "context"

func (s *BitmapStore) Pos(ctx context.Context, key string, bit int, start, end *int64) (int64, error) {
	args := []any{"BITPOS", key, bit}
	if start != nil {
		args = append(args, *start)
	}
	if end != nil {
		args = append(args, *end)
	}
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *BitmapStore) Op(ctx context.Context, operation, destination string, keys ...string) (int64, error) {
	args := []any{"BITOP", operation, destination}
	for _, key := range keys {
		args = append(args, key)
	}
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *HyperLogLogStore) Merge(ctx context.Context, destination string, sources ...string) error {
	args := []any{"PFMERGE", destination}
	for _, source := range sources {
		args = append(args, source)
	}
	_, err := s.client.Command(ctx, args...)
	return err
}
