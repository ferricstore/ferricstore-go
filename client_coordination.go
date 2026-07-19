package ferricstore

import (
	"context"
	"errors"
	"fmt"
)

func (c *Client) CAS(ctx context.Context, key string, expected, value any, ex *int64) (bool, error) {
	if ex != nil {
		if err := validateNativeTTLSecondsV080("CAS", *ex); err != nil {
			return false, err
		}
	}
	encodedExpected, err := c.encode(expected)
	if err != nil {
		return false, err
	}
	encodedValue, err := c.encode(value)
	if err != nil {
		return false, err
	}
	args := []any{"CAS", key, encodedExpected, encodedValue}
	appendInt64Ptr(&args, "EX", ex)
	response, err := c.typedReply(ctx, args...)
	return responseOptionalBool(response, err)
}

func (c *Client) Lock(ctx context.Context, key, owner string, ttlMS int64) (bool, error) {
	if err := validateNativeTTLMSV080("LOCK", ttlMS, false); err != nil {
		return false, err
	}
	response, err := c.typedReply(ctx, "LOCK", key, owner, ttlMS)
	return responseOK(response, err)
}

func (c *Client) Unlock(ctx context.Context, key, owner string) (int64, error) {
	response, err := c.typedReply(ctx, "UNLOCK", key, owner)
	return requiredOneResponse("UNLOCK", response, err)
}

func (c *Client) ExtendLock(ctx context.Context, key, owner string, ttlMS int64) (int64, error) {
	if err := validateNativeTTLMSV080("EXTEND", ttlMS, false); err != nil {
		return 0, err
	}
	response, err := c.typedReply(ctx, "EXTEND", key, owner, ttlMS)
	return requiredOneResponse("EXTEND", response, err)
}

func (c *Client) RateLimitAdd(ctx context.Context, key string, windowMS, max, count int64) (RateLimitResult, error) {
	if err := validateNativeWindowMSV080(windowMS); err != nil {
		return RateLimitResult{}, err
	}
	if max <= 0 {
		return RateLimitResult{}, errors.New("RATELIMIT.ADD maximum must be positive")
	}
	if count <= 0 {
		return RateLimitResult{}, errors.New("RATELIMIT.ADD count must be positive")
	}
	response, err := c.typedReply(ctx, "RATELIMIT.ADD", key, windowMS, max, count)
	if err != nil {
		return RateLimitResult{}, err
	}
	items, ok := response.([]any)
	if !ok || len(items) != 4 {
		return RateLimitResult{}, fmt.Errorf("expected ratelimit result array")
	}
	if items[0] == nil {
		return RateLimitResult{}, errors.New("invalid ratelimit status: response is nil")
	}
	status, err := responseString(items[0], nil)
	if err != nil {
		return RateLimitResult{}, fmt.Errorf("invalid ratelimit status: %w", err)
	}
	if status != "allowed" && status != "denied" {
		return RateLimitResult{}, fmt.Errorf("invalid ratelimit status %q", status)
	}
	resultCount, err := responseInt64(items[1], nil)
	if err != nil {
		return RateLimitResult{}, fmt.Errorf("invalid ratelimit count: %w", err)
	}
	if resultCount < 0 {
		return RateLimitResult{}, fmt.Errorf("invalid ratelimit count %d", resultCount)
	}
	remaining, err := responseInt64(items[2], nil)
	if err != nil {
		return RateLimitResult{}, fmt.Errorf("invalid ratelimit remaining: %w", err)
	}
	if remaining < 0 || remaining > max {
		return RateLimitResult{}, fmt.Errorf("invalid ratelimit remaining %d", remaining)
	}
	resetMS, err := responseInt64(items[3], nil)
	if err != nil {
		return RateLimitResult{}, fmt.Errorf("invalid ratelimit reset_ms: %w", err)
	}
	if resetMS < 0 || resetMS > windowMS {
		return RateLimitResult{}, fmt.Errorf("invalid ratelimit reset_ms %d", resetMS)
	}
	if err := validateRateLimitOutcome(status, resultCount, remaining, max, count); err != nil {
		return RateLimitResult{}, err
	}
	return RateLimitResult{Status: status, Count: resultCount, Remaining: remaining, ResetMS: resetMS}, nil
}

func validateRateLimitOutcome(status string, resultCount, remaining, maximum, increment int64) error {
	expectedRemaining := int64(0)
	if resultCount < maximum {
		expectedRemaining = maximum - resultCount
	}
	if remaining != expectedRemaining {
		return fmt.Errorf(
			"invalid ratelimit remaining %d for count %d and maximum %d",
			remaining, resultCount, maximum,
		)
	}
	switch status {
	case "allowed":
		if resultCount < increment || resultCount > maximum {
			return fmt.Errorf("invalid allowed ratelimit count %d", resultCount)
		}
	case "denied":
		if increment <= maximum && resultCount <= maximum-increment {
			return fmt.Errorf("invalid denied ratelimit count %d", resultCount)
		}
	}
	return nil
}

func (c *Client) KeyInfo(ctx context.Context, key string) (KeyInfo, error) {
	response, err := c.typedReply(ctx, "FERRICSTORE.KEY_INFO", key)
	if err != nil {
		return KeyInfo{}, err
	}
	raw, err := nativeMap(response)
	if err != nil {
		return KeyInfo{}, err
	}
	if raw["type"] == nil {
		return KeyInfo{}, errors.New("invalid key_info type: response is nil")
	}
	typeName, err := responseString(raw["type"], nil)
	if err != nil {
		return KeyInfo{}, fmt.Errorf("invalid key_info type: %w", err)
	}
	if typeName == "" {
		return KeyInfo{}, errors.New("invalid key_info type: response is empty")
	}
	valueSize, err := responseInt64(raw["value_size"], nil)
	if err != nil {
		return KeyInfo{}, fmt.Errorf("invalid key_info value_size: %w", err)
	}
	if valueSize < 0 {
		return KeyInfo{}, fmt.Errorf("invalid key_info value_size %d", valueSize)
	}
	ttlMS, err := responseInt64(raw["ttl_ms"], nil)
	if err != nil {
		return KeyInfo{}, fmt.Errorf("invalid key_info ttl_ms: %w", err)
	}
	if ttlMS < -2 {
		return KeyInfo{}, fmt.Errorf("invalid key_info ttl_ms %d", ttlMS)
	}
	if raw["hot_cache_status"] == nil {
		return KeyInfo{}, errors.New("invalid key_info hot_cache_status: response is nil")
	}
	hotCacheStatus, err := responseString(raw["hot_cache_status"], nil)
	if err != nil {
		return KeyInfo{}, fmt.Errorf("invalid key_info hot_cache_status: %w", err)
	}
	if hotCacheStatus != "hot" && hotCacheStatus != "cold" {
		return KeyInfo{}, fmt.Errorf("invalid key_info hot_cache_status %q", hotCacheStatus)
	}
	lastWriteShard, err := responseInt64(raw["last_write_shard"], nil)
	if err != nil {
		return KeyInfo{}, fmt.Errorf("invalid key_info last_write_shard: %w", err)
	}
	if lastWriteShard < 0 {
		return KeyInfo{}, fmt.Errorf("invalid key_info last_write_shard %d", lastWriteShard)
	}
	if typeName == "none" {
		if valueSize != 0 || ttlMS != -2 || hotCacheStatus != "cold" {
			return KeyInfo{}, errors.New("invalid key_info metadata for missing key")
		}
	} else if ttlMS == -2 {
		return KeyInfo{}, errors.New("invalid key_info ttl_ms -2 for existing key")
	}
	return KeyInfo{
		Type:           typeName,
		ValueSize:      valueSize,
		TTLMS:          ttlMS,
		HotCacheStatus: hotCacheStatus,
		LastWriteShard: lastWriteShard,
		Raw:            raw,
	}, nil
}
