package ferricstore

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"
)

const (
	nativeStatusBusy    = 4
	nativeStatusReroute = 5

	nativeMaxServerRetries = 1
)

type nativeRetryDisposition struct {
	busy       bool
	reroute    bool
	retryable  bool
	retryAfter time.Duration
}

func nativeServerRetryDisposition(err error) nativeRetryDisposition {
	nativeErr, ok := nativeErrorValue(err)
	if !ok {
		return nativeRetryDisposition{}
	}
	disposition := nativeRetryDisposition{
		busy:    nativeErrorHasKind(nativeErr, nativeStatusBusy, "busy"),
		reroute: nativeErrorHasKind(nativeErr, nativeStatusReroute, "reroute"),
	}
	if !disposition.busy && !disposition.reroute {
		return nativeRetryDisposition{}
	}
	retryable, retryableOK := nativeErrorField(nativeErr.Value, "retryable").(bool)
	safe, safeOK := nativeErrorField(nativeErr.Value, "safe_to_retry").(bool)
	disposition.retryable = retryableOK && safeOK && retryable && safe
	disposition.retryAfter = nativeRetryAfter(nativeErrorField(nativeErr.Value, "retry_after_ms"))
	return disposition
}

func nativeErrorValue(err error) (NativeError, bool) {
	var value NativeError
	if errors.As(err, &value) {
		return value, true
	}
	var pointer *NativeError
	if errors.As(err, &pointer) && pointer != nil {
		return *pointer, true
	}
	return NativeError{}, false
}

func nativeErrorHasKind(err NativeError, status uint16, kind string) bool {
	if err.Status == status || strings.EqualFold(err.Kind, kind) {
		return true
	}
	code := nativeErrorField(err.Value, "code")
	switch value := code.(type) {
	case string:
		return strings.EqualFold(value, kind)
	case []byte:
		return strings.EqualFold(string(value), kind)
	default:
		return false
	}
}

func nativeErrorField(value any, name string) any {
	switch mapping := value.(type) {
	case map[string]any:
		return mapping[name]
	case map[interface{}]interface{}:
		for key, field := range mapping {
			switch typed := key.(type) {
			case string:
				if typed == name {
					return field
				}
			case []byte:
				if string(typed) == name {
					return field
				}
			}
		}
	}
	return nil
}

func nativeRetryAfter(value any) time.Duration {
	milliseconds, ok := nativeRetryAfterMilliseconds(value)
	if !ok || milliseconds <= 0 {
		return 0
	}
	if milliseconds > math.MaxInt64/int64(time.Millisecond) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func nativeRetryAfterMilliseconds(value any) (int64, bool) {
	switch number := value.(type) {
	case int:
		return int64(number), true
	case int8:
		return int64(number), true
	case int16:
		return int64(number), true
	case int32:
		return int64(number), true
	case int64:
		return number, true
	case uint:
		if uint64(number) <= math.MaxInt64 {
			return int64(number), true
		}
	case uint8:
		return int64(number), true
	case uint16:
		return int64(number), true
	case uint32:
		return int64(number), true
	case uint64:
		if number <= math.MaxInt64 {
			return int64(number), true
		}
	}
	return 0, false
}

func waitNativeRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func retryableBusyPipeline(items []pipelineItemResult) (time.Duration, bool) {
	if len(items) == 0 {
		return 0, false
	}
	var delay time.Duration
	for _, item := range items {
		disposition := nativeServerRetryDisposition(item.err)
		if !disposition.busy || !disposition.retryable {
			return 0, false
		}
		delay = max(delay, disposition.retryAfter)
	}
	return delay, true
}
