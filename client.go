package ferricstore

import (
	"context"
	"fmt"
	"time"
)

type Executor interface {
	Do(ctx context.Context, args ...any) (any, error)
}

type pipelineExecutor interface {
	Pipeline(ctx context.Context, commands [][]any) ([]any, error)
}

type ClientOption func(*Client)

func WithCodec(codec Codec) ClientOption {
	return func(c *Client) {
		if codec != nil {
			c.codec = codec
		}
	}
}

func WithNativeOptions(opts ...NativeOption) ClientOption {
	return func(c *Client) {
		native, ok := c.exec.(*NativeExecutor)
		if !ok || native == nil {
			return
		}
		for _, opt := range opts {
			if opt != nil {
				opt(&native.opts)
			}
		}
	}
}

type Client struct {
	exec   Executor
	closer func() error
	codec  Codec

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
	client := &Client{exec: exec, codec: RawCodec{}}
	for _, opt := range opts {
		opt(client)
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

func (c *Client) Codec() Codec                         { return c.codec }
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
	return c.exec.Do(ctx, args...)
}

func (c *Client) Pipeline(ctx context.Context, commands [][]any) ([]any, error) {
	if len(commands) == 0 {
		return nil, nil
	}
	if exec, ok := c.exec.(pipelineExecutor); ok {
		return exec.Pipeline(ctx, commands)
	}
	results := make([]any, 0, len(commands))
	for _, command := range commands {
		value, err := c.Command(ctx, command...)
		if err != nil {
			return nil, err
		}
		results = append(results, value)
	}
	return results, nil
}

func nowMS() int64 {
	return time.Now().UnixMilli()
}

func (c *Client) encode(value any) (any, error) {
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
	response, err := c.Command(ctx, args...)
	return asBool(response), err
}

func (c *Client) Lock(ctx context.Context, key, owner string, ttlMS int64) (bool, error) {
	response, err := c.Command(ctx, "LOCK", key, owner, ttlMS)
	return isOK(response), err
}

func (c *Client) Unlock(ctx context.Context, key, owner string) (int64, error) {
	response, err := c.Command(ctx, "UNLOCK", key, owner)
	return asInt64(response), err
}

func (c *Client) ExtendLock(ctx context.Context, key, owner string, ttlMS int64) (int64, error) {
	response, err := c.Command(ctx, "EXTEND", key, owner, ttlMS)
	return asInt64(response), err
}

func (c *Client) RateLimitAdd(ctx context.Context, key string, windowMS, max, count int64) (RateLimitResult, error) {
	response, err := c.Command(ctx, "RATELIMIT.ADD", key, windowMS, max, count)
	if err != nil {
		return RateLimitResult{}, err
	}
	items, ok := response.([]any)
	if !ok || len(items) < 4 {
		return RateLimitResult{}, fmt.Errorf("expected ratelimit result array")
	}
	return RateLimitResult{Status: asString(items[0]), Count: asInt64(items[1]), Remaining: asInt64(items[2]), ResetMS: asInt64(items[3])}, nil
}

func (c *Client) KeyInfo(ctx context.Context, key string) (KeyInfo, error) {
	response, err := c.Command(ctx, "FERRICSTORE.KEY_INFO", key)
	if err != nil {
		return KeyInfo{}, err
	}
	raw, err := nativeMap(response)
	if err != nil {
		return KeyInfo{}, err
	}
	return KeyInfo{
		Type:           asString(raw["type"]),
		ValueSize:      asInt64(raw["value_size"]),
		TTLMS:          asInt64(raw["ttl_ms"]),
		HotCacheStatus: asString(raw["hot_cache_status"]),
		LastWriteShard: asInt64(raw["last_write_shard"]),
		Raw:            raw,
	}, nil
}

func (c *Client) FetchOrCompute(ctx context.Context, key string, ttlMS int64, hint string) (FetchOrComputeResult, error) {
	args := []any{"FETCH_OR_COMPUTE", key, ttlMS}
	if hint != "" {
		args = append(args, hint)
	}
	response, err := c.Command(ctx, args...)
	if err != nil {
		return FetchOrComputeResult{}, err
	}
	items, ok := response.([]any)
	if !ok || len(items) < 2 {
		return FetchOrComputeResult{}, fmt.Errorf("expected fetch_or_compute response")
	}
	status := asString(items[0])
	if status == "hit" {
		decoded, err := decodeValue(c.codec, items[1])
		return FetchOrComputeResult{Status: status, Value: decoded}, err
	}
	return FetchOrComputeResult{Status: status, ComputeToken: items[1]}, nil
}

func (c *Client) FetchOrComputeResult(ctx context.Context, key string, value any, ttlMS int64) (bool, error) {
	encoded, err := c.encode(value)
	if err != nil {
		return false, err
	}
	response, err := c.Command(ctx, "FETCH_OR_COMPUTE_RESULT", key, encoded, ttlMS)
	return isOK(response), err
}

func (c *Client) FetchOrComputeError(ctx context.Context, key, message string) (bool, error) {
	response, err := c.Command(ctx, "FETCH_OR_COMPUTE_ERROR", key, message)
	return isOK(response), err
}

func (c *Client) ClusterHealth(ctx context.Context) (map[string]any, error) {
	value, err := c.Command(ctx, "CLUSTER.HEALTH")
	if err != nil {
		return nil, err
	}
	return kvResponse(value)
}

func (c *Client) ClusterStats(ctx context.Context) (map[string]any, error) {
	value, err := c.Command(ctx, "CLUSTER.STATS")
	if err != nil {
		return nil, err
	}
	return kvResponse(value)
}

func (c *Client) ClusterKeySlot(ctx context.Context, key string) (int64, error) {
	value, err := c.Command(ctx, "CLUSTER.KEYSLOT", key)
	return asInt64(value), err
}

func (c *Client) ClusterSlots(ctx context.Context) (any, error) {
	return c.Command(ctx, "CLUSTER.SLOTS")
}

func (c *Client) ClusterStatus(ctx context.Context) (map[string]any, error) {
	value, err := c.Command(ctx, "CLUSTER.STATUS")
	if err != nil {
		return nil, err
	}
	return kvResponse(value)
}

func (c *Client) ClusterRole(ctx context.Context) (any, error) {
	return c.Command(ctx, "CLUSTER.ROLE")
}

func (c *Client) ClusterJoin(ctx context.Context, node string, replace bool) (bool, error) {
	args := []any{"CLUSTER.JOIN", node}
	if replace {
		args = append(args, "REPLACE")
	}
	value, err := c.Command(ctx, args...)
	return isOK(value), err
}

func (c *Client) ClusterLeave(ctx context.Context) (bool, error) {
	value, err := c.Command(ctx, "CLUSTER.LEAVE")
	return isOK(value), err
}

func (c *Client) ClusterFailover(ctx context.Context, shardIndex int, targetNode string) (bool, error) {
	value, err := c.Command(ctx, "CLUSTER.FAILOVER", shardIndex, targetNode)
	return isOK(value), err
}

func (c *Client) ClusterPromote(ctx context.Context, node string) (bool, error) {
	value, err := c.Command(ctx, "CLUSTER.PROMOTE", node)
	return isOK(value), err
}

func (c *Client) ClusterDemote(ctx context.Context, node string) (bool, error) {
	value, err := c.Command(ctx, "CLUSTER.DEMOTE", node)
	return isOK(value), err
}

func (c *Client) FerricStoreConfig(ctx context.Context, args ...any) (any, error) {
	command := append([]any{"FERRICSTORE.CONFIG"}, args...)
	return c.Command(ctx, command...)
}

func (c *Client) FerricStoreHotness(ctx context.Context, args ...any) (map[string]any, error) {
	command := append([]any{"FERRICSTORE.HOTNESS"}, args...)
	value, err := c.Command(ctx, command...)
	if err != nil {
		return nil, err
	}
	return kvResponse(value)
}

func (c *Client) FerricStoreMetrics(ctx context.Context, args ...any) (map[string]any, error) {
	command := append([]any{"FERRICSTORE.METRICS"}, args...)
	value, err := c.Command(ctx, command...)
	if err != nil {
		return nil, err
	}
	return kvResponse(value)
}

func (c *Client) FerricStoreBlobGC(ctx context.Context, args ...any) (any, error) {
	command := append([]any{"FERRICSTORE.BLOBGC"}, args...)
	return c.Command(ctx, command...)
}
