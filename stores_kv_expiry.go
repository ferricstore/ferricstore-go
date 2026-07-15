package ferricstore

import (
	"context"
	"errors"
)

// ExpireOptions conditionally applies an expiration. The conditions are
// mutually exclusive and match the server's NX, XX, GT, and LT options.
type ExpireOptions struct {
	NX bool
	XX bool
	GT bool
	LT bool
}

func (opt ExpireOptions) condition() (string, error) {
	if boolInt(opt.NX)+boolInt(opt.XX)+boolInt(opt.GT)+boolInt(opt.LT) > 1 {
		return "", errors.New("expiration accepts only one of NX, XX, GT, or LT")
	}
	switch {
	case opt.NX:
		return "NX", nil
	case opt.XX:
		return "XX", nil
	case opt.GT:
		return "GT", nil
	case opt.LT:
		return "LT", nil
	default:
		return "", nil
	}
}

// ExpireWithOptions sets a relative expiration in seconds, optionally subject
// to an NX, XX, GT, or LT condition.
func (s *KeyValueStore) ExpireWithOptions(
	ctx context.Context,
	key string,
	seconds int64,
	opt ExpireOptions,
) (bool, error) {
	return s.expireWithOptions(ctx, "EXPIRE", key, seconds, opt)
}

// PExpireWithOptions sets a relative expiration in milliseconds, optionally
// subject to an NX, XX, GT, or LT condition.
func (s *KeyValueStore) PExpireWithOptions(
	ctx context.Context,
	key string,
	milliseconds int64,
	opt ExpireOptions,
) (bool, error) {
	return s.expireWithOptions(ctx, "PEXPIRE", key, milliseconds, opt)
}

// ExpireAtWithOptions sets an absolute expiration in Unix seconds, optionally
// subject to an NX, XX, GT, or LT condition.
func (s *KeyValueStore) ExpireAtWithOptions(
	ctx context.Context,
	key string,
	unixSeconds int64,
	opt ExpireOptions,
) (bool, error) {
	return s.expireWithOptions(ctx, "EXPIREAT", key, unixSeconds, opt)
}

// PExpireAtWithOptions sets an absolute expiration in Unix milliseconds,
// optionally subject to an NX, XX, GT, or LT condition.
func (s *KeyValueStore) PExpireAtWithOptions(
	ctx context.Context,
	key string,
	unixMilliseconds int64,
	opt ExpireOptions,
) (bool, error) {
	return s.expireWithOptions(ctx, "PEXPIREAT", key, unixMilliseconds, opt)
}

func (s *KeyValueStore) expireWithOptions(
	ctx context.Context,
	command string,
	key string,
	value int64,
	opt ExpireOptions,
) (bool, error) {
	condition, err := opt.condition()
	if err != nil {
		return false, err
	}
	args := []any{command, key, value}
	if condition != "" {
		args = append(args, condition)
	}
	response, err := s.commandReply(ctx, args...)
	return responseBool(response, err)
}
