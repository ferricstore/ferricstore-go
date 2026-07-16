package ferricstore

import (
	"context"
	"errors"
	"fmt"
)

type StreamStore struct{ client *Client }

type StreamAddOptions struct {
	NoMkStream  bool
	MaxLen      *int64
	MinID       string
	Approximate bool
}

func (s *StreamStore) Add(ctx context.Context, key, id string, fields map[string]any) (string, error) {
	return s.AddWithOptions(ctx, key, id, fields, StreamAddOptions{})
}

func (s *StreamStore) AddWithOptions(
	ctx context.Context,
	key, id string,
	fields map[string]any,
	opt StreamAddOptions,
) (string, error) {
	if err := validateStreamAdd(fields, opt); err != nil {
		return "", err
	}
	if id == "" {
		id = "*"
	}
	args := []any{"XADD", key}
	if opt.NoMkStream {
		args = append(args, "NOMKSTREAM")
	}
	if opt.MaxLen != nil {
		args = append(args, "MAXLEN")
		if opt.Approximate {
			args = append(args, "~")
		}
		args = append(args, *opt.MaxLen)
	}
	if opt.MinID != "" {
		args = append(args, "MINID")
		if opt.Approximate {
			args = append(args, "~")
		}
		args = append(args, opt.MinID)
	}
	args = append(args, id)
	for _, field := range mapKeysForCodec(fields, s.client.codec) {
		encoded, err := s.client.encode(fields[field])
		if err != nil {
			return "", err
		}
		args = append(args, field, encoded)
	}
	response, err := s.client.typedReply(ctx, args...)
	if err != nil {
		return "", err
	}
	if response == nil && opt.NoMkStream {
		return "", nil
	}
	return streamIDStringResponse("XADD", response, nil)
}

func validateStreamAdd(fields map[string]any, opt StreamAddOptions) error {
	if len(fields) == 0 {
		return errors.New("XADD requires at least one field/value pair")
	}
	if opt.MaxLen != nil && opt.MinID != "" {
		return errors.New("XADD accepts exactly one of MAXLEN or MINID")
	}
	if opt.Approximate && opt.MaxLen == nil && opt.MinID == "" {
		return errors.New("XADD approximate trimming requires MAXLEN or MINID")
	}
	if opt.MaxLen != nil && *opt.MaxLen < 0 {
		return errors.New("XADD MAXLEN must be non-negative")
	}
	if opt.MinID != "" {
		if _, _, ok := parseStreamIDText(opt.MinID); !ok {
			return fmt.Errorf("XADD MINID has invalid stream ID %q", opt.MinID)
		}
	}
	return nil
}

func (s *StreamStore) Range(ctx context.Context, key, start, stop string, count *int) (any, error) {
	if err := validateNonNegativeCount("XRANGE", count); err != nil {
		return nil, err
	}
	args := []any{"XRANGE", key, start, stop}
	appendIntPtr(&args, "COUNT", count)
	value, err := s.client.typedReply(ctx, args...)
	return decodeStreamEntriesLimited(s.client.codec, value, err, count)
}
