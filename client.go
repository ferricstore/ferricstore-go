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

// WithNativeOptions configures the NativeExecutor owned by NewClient or
// NewClientFromURL. NewClientWithExecutor never mutates its caller-owned
// executor; configure an injected NativeExecutor when constructing it.
func WithNativeOptions(opts ...NativeOption) ClientOption {
	return func(c *Client) {
		if !c.ownsNativeConfiguration {
			return
		}
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
	exec                    Executor
	closer                  func() error
	codec                   Codec
	ownsNativeConfiguration bool

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
	client := newClientWithExecutor(exec, true, opts...)
	client.closer = exec.Close
	return client
}

func NewClientFromURL(rawurl string, opts ...ClientOption) (*Client, error) {
	exec, err := NewNativeExecutorFromURL(rawurl)
	if err != nil {
		return nil, err
	}
	client := newClientWithExecutor(exec, true, opts...)
	client.closer = exec.Close
	return client, nil
}

func NewClientWithExecutor(exec Executor, opts ...ClientOption) *Client {
	return newClientWithExecutor(exec, false, opts...)
}

func newClientWithExecutor(exec Executor, ownsNativeConfiguration bool, opts ...ClientOption) *Client {
	if interfaceIsNil(exec) {
		exec = missingClientExecutor{}
	}
	client := &Client{
		exec:                    exec,
		codec:                   RawCodec{},
		ownsNativeConfiguration: ownsNativeConfiguration,
	}
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
	switch exec := eventExecutor(c.exec).(type) {
	case *NativeExecutor:
		return exec.DroppedEvents()
	case *TopologyNativeExecutor:
		return exec.DroppedEvents()
	default:
		return 0
	}
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
	if keys := deterministicMapKeysForCodec(named.Values, c.codec); keys != nil {
		for _, name := range keys {
			if err := c.appendNamedValue(args, name, named.Values[name]); err != nil {
				return err
			}
		}
	} else {
		for name, value := range named.Values {
			if err := c.appendNamedValue(args, name, value); err != nil {
				return err
			}
		}
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

func (c *Client) appendNamedValue(args *[]any, name string, value any) error {
	encoded, err := c.encode(value)
	if err != nil {
		return err
	}
	*args = append(*args, "VALUE", name, encoded)
	return nil
}

func appendValueReturn(args *[]any, values []string, maxBytes *int64) {
	for _, name := range values {
		*args = append(*args, "VALUE", name)
	}
	appendInt64Ptr(args, "VALUE_MAX_BYTES", maxBytes)
}
