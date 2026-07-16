package ferricstore

import "context"

// These compile-time checks protect signatures and type properties shipped in
// v0.1.6. The full module comparison is enforced by scripts/api-compat.sh.
var (
	_ = map[NativeOptions]struct{}{}
	_ = map[PubSub]struct{}{}

	_ func(*Client, context.Context, ...any) (map[string]any, error)   = (*Client).FerricStoreMetrics
	_ func(*Client, context.Context, ...any) (string, error)           = (*Client).FerricStoreMetricsText
	_ func(*Client, context.Context, any, ...any) (any, error)         = (*Client).CommandForKey
	_ func(*Client, context.Context, string, any, int64) (bool, error) = (*Client).FetchOrComputeResult
	_ func(*Client, context.Context, string, string) (bool, error)     = (*Client).FetchOrComputeError
	_ func(*Client, context.Context, int64, string, *int) (any, error) = (*Client).Scan

	_ func(*HashStore, context.Context, string, int64, string, *int) (any, error)      = (*HashStore).Scan
	_ func(*SetStore, context.Context, string, int64, string, *int) (any, error)       = (*SetStore).Scan
	_ func(*SortedSetStore, context.Context, string, int64, string, *int) (any, error) = (*SortedSetStore).Scan
)
