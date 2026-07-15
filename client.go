package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Executor interface {
	Do(ctx context.Context, args ...any) (any, error)
}

var errClientExecutorRequired = errors.New("ferricstore client requires an executor")

type missingClientExecutor struct{}

func (missingClientExecutor) Do(context.Context, ...any) (any, error) {
	return nil, errClientExecutorRequired
}

type pipelineExecutor interface {
	Pipeline(ctx context.Context, commands [][]any) ([]any, error)
}

type pipelineItemResult struct {
	value any
	err   error
}

type commandStateExecutor interface {
	doWithState(ctx context.Context, args ...any) (any, bool, error)
}

type typedCommandStateExecutor interface {
	doTypedWithState(ctx context.Context, allowQueued bool, args ...any) (any, bool, error)
}

// PipelineFailure identifies one failed command in a non-atomic pipeline.
type PipelineFailure struct {
	Index int
	Err   error
}

// PipelineError reports every failed command while Pipeline still returns the
// complete result slice. Failed positions in that slice contain the same error.
type PipelineError struct {
	Failures []PipelineFailure
}

func (e *PipelineError) Error() string {
	if e == nil || len(e.Failures) == 0 {
		return ""
	}
	failure := e.Failures[0]
	if len(e.Failures) == 1 {
		return fmt.Sprintf("ferricstore pipeline command %d failed: %v", failure.Index, failure.Err)
	}
	return fmt.Sprintf("ferricstore pipeline completed with %d command failures (first at command %d: %v)", len(e.Failures), failure.Index, failure.Err)
}

func (e *PipelineError) Unwrap() []error {
	if e == nil {
		return nil
	}
	errs := make([]error, 0, len(e.Failures))
	for _, failure := range e.Failures {
		if failure.Err != nil {
			errs = append(errs, failure.Err)
		}
	}
	return errs
}

type detailedPipelineExecutor interface {
	pipelineDetailed(ctx context.Context, commands [][]any) ([]pipelineItemResult, error)
}

type ClientOption func(*Client)

func WithCodec(codec Codec) ClientOption {
	return func(c *Client) {
		if !codecIsNil(codec) {
			c.codec = codecForClient(codec)
		}
	}
}

// WithConcurrentCodec installs a custom codec without serialization or output
// snapshots. The codec must support overlapping calls and must return mutable
// values whose ownership is transferred to the caller. Prefer WithCodec unless
// those guarantees are known to hold.
func WithConcurrentCodec(codec Codec) ClientOption {
	return func(c *Client) {
		if !codecIsNil(codec) {
			c.codec = concurrentCodecForClient(codec)
		}
	}
}

func WithNativeOptions(opts ...NativeOption) ClientOption {
	return func(c *Client) {
		native, ok := c.exec.(*NativeExecutor)
		if !ok || native == nil {
			return
		}
		applyNativeOptions(&native.opts, opts...)
		if native.flow != nil {
			native.flow.mu.Lock()
			native.flow.maxQueued = max(0, native.opts.MaxQueuedRequests)
			native.flow.mu.Unlock()
		}
	}
}

type Client struct {
	exec   Executor
	closer func() error
	codec  Codec

	sessionGate  sessionGate
	legacyGate   sessionGate
	legacyMu     sync.Mutex
	legacy       commandSession
	legacyMulti  bool
	legacyActive atomic.Bool

	kv          *KeyValueStore
	hash        *HashStore
	list        *ListStore
	set         *SetStore
	zset        *SortedSetStore
	stream      *StreamStore
	bitmap      *BitmapStore
	hyperloglog *HyperLogLogStore
	geo         *GeoStore
	bloom       *BloomFilterStore
	cuckoo      *CuckooFilterStore
	cms         *CountMinSketchStore
	topk        *TopKStore
	tdigest     *TDigestStore
}

func NewClient(addr string, opts ...ClientOption) *Client {
	exec := NewNativeExecutor(addr)
	client := NewClientWithExecutor(exec, opts...)
	client.closer = exec.Close
	return client
}

func NewClientFromURL(rawurl string, opts ...ClientOption) (*Client, error) {
	exec, err := NewNativeExecutorFromURL(rawurl)
	if err != nil {
		return nil, err
	}
	client := NewClientWithExecutor(exec, opts...)
	client.closer = exec.Close
	return client, nil
}

func NewClientWithExecutor(exec Executor, opts ...ClientOption) *Client {
	if interfaceIsNil(exec) {
		exec = missingClientExecutor{}
	}
	client := &Client{exec: exec, codec: RawCodec{}}
	for _, opt := range opts {
		if opt != nil {
			opt(client)
		}
	}
	client.kv = &KeyValueStore{client: client}
	client.hash = &HashStore{client: client}
	client.list = &ListStore{client: client}
	client.set = &SetStore{client: client}
	client.zset = &SortedSetStore{client: client}
	client.stream = &StreamStore{client: client}
	client.bitmap = &BitmapStore{client: client}
	client.hyperloglog = &HyperLogLogStore{client: client}
	client.geo = &GeoStore{client: client}
	client.bloom = &BloomFilterStore{client: client}
	client.cuckoo = &CuckooFilterStore{client: client}
	client.cms = &CountMinSketchStore{client: client}
	client.topk = &TopKStore{client: client}
	client.tdigest = &TDigestStore{client: client}
	return client
}

func (c *Client) Close() error {
	c.legacyMu.Lock()
	legacy := c.legacy
	c.setLegacySessionLocked(nil)
	c.legacyMu.Unlock()
	if legacy != nil {
		legacy.Abort(net.ErrClosed)
	}
	if c.closer == nil {
		return nil
	}
	return c.closer()
}

func (c *Client) DroppedEvents() uint64 {
	if c == nil {
		return 0
	}
	native, ok := c.exec.(*NativeExecutor)
	if !ok {
		return 0
	}
	return native.DroppedEvents()
}

func (c *Client) Codec() Codec                         { return originalCodec(c.codec) }
func (c *Client) KV() *KeyValueStore                   { return c.kv }
func (c *Client) Hash() *HashStore                     { return c.hash }
func (c *Client) ListStore() *ListStore                { return c.list }
func (c *Client) SetStore() *SetStore                  { return c.set }
func (c *Client) SortedSet() *SortedSetStore           { return c.zset }
func (c *Client) Stream() *StreamStore                 { return c.stream }
func (c *Client) Bitmap() *BitmapStore                 { return c.bitmap }
func (c *Client) HyperLogLog() *HyperLogLogStore       { return c.hyperloglog }
func (c *Client) Geo() *GeoStore                       { return c.geo }
func (c *Client) Bloom() *BloomFilterStore             { return c.bloom }
func (c *Client) Cuckoo() *CuckooFilterStore           { return c.cuckoo }
func (c *Client) CountMinSketch() *CountMinSketchStore { return c.cms }
func (c *Client) TopK() *TopKStore                     { return c.topk }
func (c *Client) TDigest() *TDigestStore               { return c.tdigest }

func (c *Client) Command(ctx context.Context, args ...any) (any, error) {
	if err := validateCommandArgs(args); err != nil {
		return nil, err
	}
	if name, stateful := connectionStateCommand(args); stateful {
		return nil, fmt.Errorf("%s requires Watch, Multi, Transaction, Exec, Discard, or Unwatch so the SDK can pin a connection", name)
	}
	if err := c.legacyGate.readLock(ctx); err != nil {
		return nil, err
	}
	defer c.legacyGate.readUnlock()
	if session := c.currentLegacySession(); session != nil {
		return session.Do(ctx, affineCommandArgs(args)...)
	}
	return c.commandWithoutLegacy(ctx, args...)
}

func (c *Client) commandWithoutLegacy(ctx context.Context, args ...any) (any, error) {
	value, _, err := c.commandWithoutLegacyWithState(ctx, args...)
	return value, err
}

func (c *Client) commandWithoutLegacyWithState(ctx context.Context, args ...any) (any, bool, error) {
	return c.commandWithoutLegacyWithQueuePolicy(ctx, true, args...)
}

func (c *Client) commandWithoutLegacyWithQueuePolicy(
	ctx context.Context,
	allowQueued bool,
	args ...any,
) (any, bool, error) {
	if _, ownsSessions := c.exec.(commandSessionProvider); ownsSessions {
		return c.commandUnlockedWithQueuePolicy(ctx, allowQueued, args...)
	}
	if err := c.sessionGate.readLock(ctx); err != nil {
		return nil, false, err
	}
	defer c.sessionGate.readUnlock()
	return c.commandUnlockedWithQueuePolicy(ctx, allowQueued, args...)
}

func (c *Client) commandUnlocked(ctx context.Context, args ...any) (any, error) {
	value, _, err := c.commandUnlockedWithState(ctx, args...)
	return value, err
}

func (c *Client) commandUnlockedWithState(ctx context.Context, args ...any) (any, bool, error) {
	args = commandArgsForExecutor(c.exec, args)
	return c.commandUnlockedWithPreparedState(ctx, args...)
}

func (c *Client) commandUnlockedWithPreparedState(ctx context.Context, args ...any) (any, bool, error) {
	if exec, ok := c.exec.(commandStateExecutor); ok {
		return exec.doWithState(ctx, args...)
	}
	value, err := c.exec.Do(ctx, args...)
	return value, false, err
}

func (c *Client) commandUnlockedWithQueuePolicy(
	ctx context.Context,
	allowQueued bool,
	args ...any,
) (any, bool, error) {
	args = commandArgsForExecutor(c.exec, args)
	if exec, ok := c.exec.(typedCommandStateExecutor); ok {
		return exec.doTypedWithState(ctx, allowQueued, args...)
	}
	return c.commandUnlockedWithPreparedState(ctx, args...)
}

func nowMS() int64 {
	return time.Now().UnixMilli()
}

func (c *Client) encode(value any) (any, error) {
	if executorSupportsDeferredCodec(c.exec, c.codec) {
		return nativeDeferredCodecValue{codec: c.codec, value: value}, nil
	}
	return c.codec.Encode(value)
}

func appendOpt(args *[]any, name string, value any) {
	switch v := value.(type) {
	case string:
		if v != "" {
			*args = append(*args, name, v)
		}
	case []byte:
		if v != nil {
			*args = append(*args, name, v)
		}
	case int64:
		*args = append(*args, name, v)
	case int:
		*args = append(*args, name, v)
	default:
		if value != nil {
			*args = append(*args, name, value)
		}
	}
}

func appendBoolPtr(args *[]any, name string, value *bool) {
	if value == nil {
		return
	}
	if *value {
		*args = append(*args, name, "true")
	} else {
		*args = append(*args, name, "false")
	}
}

func appendPayloadRead(args *[]any, payload *bool, maxBytes *int64) {
	if payload != nil && !*payload {
		*args = append(*args, "NOPAYLOAD")
		return
	}
	if (payload != nil && *payload) || maxBytes != nil {
		*args = append(*args, "PAYLOAD")
	}
	if maxBytes != nil {
		*args = append(*args, "MAXBYTES", *maxBytes)
	}
}

func appendInt64Ptr(args *[]any, name string, value *int64) {
	if value != nil {
		*args = append(*args, name, *value)
	}
}

func appendIntPtr(args *[]any, name string, value *int) {
	if value != nil {
		*args = append(*args, name, *value)
	}
}

func appendScanCount(args *[]any, count *int) {
	if count != nil && *count != 10 {
		*args = append(*args, "COUNT", *count)
	}
}

func (c *Client) appendEncoded(args *[]any, name string, value any) error {
	if value == nil {
		return nil
	}
	encoded, err := c.encode(value)
	if err != nil {
		return err
	}
	appendOpt(args, name, encoded)
	return nil
}

func (c *Client) appendNamedValues(args *[]any, named NamedValues) error {
	for name, value := range named.Values {
		encoded, err := c.encode(value)
		if err != nil {
			return err
		}
		*args = append(*args, "VALUE", name, encoded)
	}
	for name, ref := range named.ValueRefs {
		*args = append(*args, "VALUE_REF", name, ref)
	}
	for _, name := range named.DropValues {
		*args = append(*args, "DROP_VALUE", name)
	}
	for _, name := range named.OverrideValues {
		*args = append(*args, "OVERRIDE_VALUE", name)
	}
	return nil
}

func appendValueReturn(args *[]any, values []string, maxBytes *int64) {
	for _, name := range values {
		*args = append(*args, "VALUE", name)
	}
	appendInt64Ptr(args, "VALUE_MAX_BYTES", maxBytes)
}

func (c *Client) CAS(ctx context.Context, key string, expected, value any, ex *int64) (bool, error) {
	if ex != nil && *ex <= 0 {
		return false, errors.New("CAS expiration must be positive")
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
	if ttlMS <= 0 {
		return false, errors.New("LOCK ttl must be positive")
	}
	response, err := c.typedReply(ctx, "LOCK", key, owner, ttlMS)
	return responseOK(response, err)
}

func (c *Client) Unlock(ctx context.Context, key, owner string) (int64, error) {
	response, err := c.typedReply(ctx, "UNLOCK", key, owner)
	return requiredOneResponse("UNLOCK", response, err)
}

func (c *Client) ExtendLock(ctx context.Context, key, owner string, ttlMS int64) (int64, error) {
	if ttlMS <= 0 {
		return 0, errors.New("EXTEND ttl must be positive")
	}
	response, err := c.typedReply(ctx, "EXTEND", key, owner, ttlMS)
	return requiredOneResponse("EXTEND", response, err)
}

func (c *Client) RateLimitAdd(ctx context.Context, key string, windowMS, max, count int64) (RateLimitResult, error) {
	if windowMS <= 0 {
		return RateLimitResult{}, errors.New("RATELIMIT.ADD window must be positive")
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
	return RateLimitResult{Status: status, Count: resultCount, Remaining: remaining, ResetMS: resetMS}, nil
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
	valueSize, err := responseInt64(raw["value_size"], nil)
	if err != nil {
		return KeyInfo{}, fmt.Errorf("invalid key_info value_size: %w", err)
	}
	ttlMS, err := responseInt64(raw["ttl_ms"], nil)
	if err != nil {
		return KeyInfo{}, fmt.Errorf("invalid key_info ttl_ms: %w", err)
	}
	if raw["hot_cache_status"] == nil {
		return KeyInfo{}, errors.New("invalid key_info hot_cache_status: response is nil")
	}
	hotCacheStatus, err := responseString(raw["hot_cache_status"], nil)
	if err != nil {
		return KeyInfo{}, fmt.Errorf("invalid key_info hot_cache_status: %w", err)
	}
	lastWriteShard, err := responseInt64(raw["last_write_shard"], nil)
	if err != nil {
		return KeyInfo{}, fmt.Errorf("invalid key_info last_write_shard: %w", err)
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

func (c *Client) FetchOrCompute(ctx context.Context, key string, ttlMS int64, hint string) (FetchOrComputeResult, error) {
	args := []any{"FETCH_OR_COMPUTE", key, ttlMS}
	if hint != "" {
		args = append(args, hint)
	}
	response, err := c.typedReply(ctx, args...)
	if err != nil {
		return FetchOrComputeResult{}, err
	}
	items, ok := response.([]any)
	if !ok || len(items) < 1 {
		return FetchOrComputeResult{}, fmt.Errorf("expected fetch_or_compute response")
	}
	status, err := responseString(items[0], nil)
	if err != nil {
		return FetchOrComputeResult{}, fmt.Errorf("invalid fetch_or_compute status: %w", err)
	}
	switch status {
	case "hit":
		if len(items) != 2 {
			return FetchOrComputeResult{}, fmt.Errorf("invalid fetch_or_compute hit response length %d", len(items))
		}
		decoded, err := decodeValue(c.codec, items[1])
		return FetchOrComputeResult{Status: status, Value: decoded}, err
	case "compute":
		if len(items) != 3 {
			return FetchOrComputeResult{}, fmt.Errorf("invalid fetch_or_compute compute response length %d", len(items))
		}
		token, err := fetchOrComputeToken(items[2:])
		if err != nil {
			return FetchOrComputeResult{}, err
		}
		return FetchOrComputeResult{Status: status, ComputeToken: token}, nil
	default:
		return FetchOrComputeResult{}, fmt.Errorf("unsupported fetch_or_compute status %q", status)
	}
}

// FetchOrComputeResult publishes a computed value. computeToken must contain
// the single token returned by FetchOrCompute when its status is "compute".
// The variadic form preserves source compatibility with older SDK releases;
// omitting the token now returns an error instead of issuing an invalid command.
func (c *Client) FetchOrComputeResult(ctx context.Context, key string, value any, ttlMS int64, computeToken ...any) (bool, error) {
	token, err := fetchOrComputeToken(computeToken)
	if err != nil {
		return false, err
	}
	encoded, err := c.encode(value)
	if err != nil {
		return false, err
	}
	response, err := c.typedReply(ctx, "FETCH_OR_COMPUTE_RESULT", key, token, encoded, ttlMS)
	return responseOK(response, err)
}

// FetchOrComputeError publishes a compute failure. computeToken follows the
// same compatibility rules as FetchOrComputeResult.
func (c *Client) FetchOrComputeError(ctx context.Context, key, message string, computeToken ...any) (bool, error) {
	token, err := fetchOrComputeToken(computeToken)
	if err != nil {
		return false, err
	}
	response, err := c.typedReply(ctx, "FETCH_OR_COMPUTE_ERROR", key, token, message)
	return responseOK(response, err)
}

func fetchOrComputeToken(tokens []any) (any, error) {
	if len(tokens) != 1 {
		return nil, fmt.Errorf("fetch_or_compute completion requires exactly one compute token, got %d", len(tokens))
	}
	switch token := tokens[0].(type) {
	case string:
		if token != "" {
			return token, nil
		}
	case []byte:
		if len(token) != 0 {
			return token, nil
		}
	}
	return nil, fmt.Errorf("fetch_or_compute compute token must be a non-empty string or byte slice, got %T", tokens[0])
}
