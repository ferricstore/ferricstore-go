package ferricstore

import (
	"context"
	"errors"
	"fmt"
)

// StreamTrimOptions selects the MAXLEN or MINID strategy used by XTRIM.
type StreamTrimOptions struct {
	MaxLen      *int64
	MinID       string
	Approximate bool
}

// TrimWithOptions trims a stream by maximum length or minimum entry ID.
func (s *StreamStore) TrimWithOptions(
	ctx context.Context,
	key string,
	opt StreamTrimOptions,
) (int64, error) {
	if err := validateStreamTrimOptions(opt); err != nil {
		return 0, err
	}
	args := []any{"XTRIM", key}
	if opt.MaxLen != nil {
		args = append(args, "MAXLEN")
		if opt.Approximate {
			args = append(args, "~")
		}
		args = append(args, *opt.MaxLen)
	} else {
		args = append(args, "MINID")
		if opt.Approximate {
			args = append(args, "~")
		}
		args = append(args, opt.MinID)
	}
	value, err := s.client.typedReply(ctx, args...)
	return nonNegativeInt64Response("XTRIM", value, err)
}

func validateStreamTrimOptions(opt StreamTrimOptions) error {
	if (opt.MaxLen == nil) == (opt.MinID == "") {
		return errors.New("XTRIM requires exactly one of MAXLEN or MINID")
	}
	if opt.MaxLen != nil && *opt.MaxLen < 0 {
		return errors.New("XTRIM MAXLEN must be non-negative")
	}
	if opt.MinID != "" {
		if !validStreamIDOrPartialText(opt.MinID) {
			return fmt.Errorf("XTRIM MINID has invalid stream ID %q", opt.MinID)
		}
	}
	return nil
}
