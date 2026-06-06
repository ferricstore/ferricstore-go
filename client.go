package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Executor interface {
	Do(ctx context.Context, args ...any) *redis.Cmd
}

type ClientOption func(*Client)

func WithCodec(codec Codec) ClientOption {
	return func(c *Client) {
		if codec != nil {
			c.codec = codec
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
	json        *JSONStore
	bloom       *BloomFilterStore
	cuckoo      *CuckooFilterStore
	cms         *CountMinSketchStore
	topk        *TopKStore
	tdigest     *TDigestStore
}

func NewClient(addr string, opts ...ClientOption) *Client {
	return NewClientFromRedis(redis.NewClient(&redis.Options{
		Addr:     addr,
		Protocol: 3,
	}), opts...)
}

func NewClientFromURL(url string, opts ...ClientOption) (*Client, error) {
	redisOptions, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	redisOptions.Protocol = 3
	return NewClientFromRedis(redis.NewClient(redisOptions), opts...), nil
}

func NewClientFromRedis(rdb *redis.Client, opts ...ClientOption) *Client {
	client := NewClientWithExecutor(rdb, opts...)
	client.closer = rdb.Close
	return client
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
	client.json = &JSONStore{client: client}
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
func (c *Client) JSON() *JSONStore                     { return c.json }
func (c *Client) Bloom() *BloomFilterStore             { return c.bloom }
func (c *Client) Cuckoo() *CuckooFilterStore           { return c.cuckoo }
func (c *Client) CountMinSketch() *CountMinSketchStore { return c.cms }
func (c *Client) TopK() *TopKStore                     { return c.topk }
func (c *Client) TDigest() *TDigestStore               { return c.tdigest }

func (c *Client) Command(ctx context.Context, args ...any) (any, error) {
	cmd := c.exec.Do(ctx, args...)
	return cmd.Result()
}

func (c *Client) Pipeline(ctx context.Context, commands [][]any) ([]any, error) {
	if len(commands) == 0 {
		return nil, nil
	}
	if pipeliner, ok := c.exec.(interface{ Pipeline() redis.Pipeliner }); ok {
		pipe := pipeliner.Pipeline()
		cmds := make([]*redis.Cmd, 0, len(commands))
		for _, command := range commands {
			cmds = append(cmds, pipe.Do(ctx, command...))
		}
		_, err := pipe.Exec(ctx)
		results := make([]any, 0, len(cmds))
		for _, cmd := range cmds {
			results = append(results, cmd.Val())
		}
		return results, err
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

func (c *Client) Create(ctx context.Context, opt CreateOptions) (*FlowRecord, error) {
	state := opt.State
	if state == "" {
		state = "queued"
	}
	now := opt.NowMS
	if now == 0 {
		now = nowMS()
	}
	runAt := opt.RunAtMS
	if runAt == 0 {
		runAt = now
	}
	args := []any{"FLOW.CREATE", opt.ID, "TYPE", opt.Type, "STATE", state, "NOW", now}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	appendOpt(&args, "PARENT_FLOW_ID", opt.ParentFlowID)
	appendOpt(&args, "ROOT_FLOW_ID", opt.RootFlowID)
	appendOpt(&args, "CORRELATION_ID", opt.CorrelationID)
	appendOpt(&args, "RUN_AT", runAt)
	appendInt64Ptr(&args, "PRIORITY", opt.Priority)
	appendBoolPtr(&args, "IDEMPOTENT", opt.Idempotent)
	appendInt64Ptr(&args, "RETENTION_TTL_MS", opt.RetentionTTLMS)
	if err := c.appendNamedValues(&args, NamedValues{Values: opt.Values, ValueRefs: opt.ValueRefs}); err != nil {
		return nil, err
	}
	value, err := c.Command(ctx, args...)
	if err != nil || !opt.ReturnRecord {
		return nil, err
	}
	return recordOrNil(value, c.codec)
}

func (c *Client) Enqueue(ctx context.Context, opt CreateOptions) (*FlowRecord, error) {
	if opt.State == "" {
		opt.State = "queued"
	}
	return c.Create(ctx, opt)
}

func (c *Client) CreateMany(ctx context.Context, opt CreateManyOptions) ([]FlowRecord, error) {
	if len(opt.Items) == 0 {
		return nil, nil
	}
	state := opt.State
	if state == "" {
		state = "queued"
	}
	now := opt.NowMS
	if now == 0 {
		now = nowMS()
	}
	runAt := opt.RunAtMS
	if runAt == 0 {
		runAt = now
	}
	mixed := opt.PartitionKey == "" && anyItemPartition(opt.Items)
	wirePartition := opt.PartitionKey
	if mixed {
		wirePartition = "MIXED"
	} else if wirePartition == "" {
		wirePartition = "AUTO"
	}
	args := []any{"FLOW.CREATE_MANY", wirePartition, "TYPE", opt.Type, "STATE", state, "NOW", now}
	appendOpt(&args, "RUN_AT", runAt)
	appendInt64Ptr(&args, "PRIORITY", opt.Priority)
	appendBoolPtr(&args, "IDEMPOTENT", opt.Idempotent)
	appendBoolPtr(&args, "INDEPENDENT", opt.Independent)
	appendInt64Ptr(&args, "RETENTION_TTL_MS", opt.RetentionTTLMS)
	extended := anyCreateItemValues(opt.Items)
	if extended {
		args = append(args, "ITEMS_EXT", len(opt.Items))
		for _, item := range opt.Items {
			partition := "-"
			if mixed {
				if item.PartitionKey == "" {
					return nil, errors.New("mixed create_many items require partition key")
				}
				partition = item.PartitionKey
			}
			encoded, err := c.encode(item.Payload)
			if err != nil {
				return nil, err
			}
			args = append(args, item.ID, partition, encoded)
			if err := c.appendNamedCounts(&args, mergeValues(opt.Values, item.Values), mergeRefs(opt.ValueRefs, item.ValueRefs)); err != nil {
				return nil, err
			}
		}
	} else {
		if err := c.appendNamedValues(&args, NamedValues{Values: opt.Values, ValueRefs: opt.ValueRefs}); err != nil {
			return nil, err
		}
		args = append(args, "ITEMS")
		for _, item := range opt.Items {
			encoded, err := c.encode(item.Payload)
			if err != nil {
				return nil, err
			}
			if mixed {
				if item.PartitionKey == "" {
					return nil, errors.New("mixed create_many items require partition key")
				}
				args = append(args, item.ID, item.PartitionKey, encoded)
			} else {
				args = append(args, item.ID, encoded)
			}
		}
	}
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsOrNil(value, c.codec)
}

func (c *Client) EnqueueMany(ctx context.Context, opt CreateManyOptions) ([]FlowRecord, error) {
	if opt.State == "" {
		opt.State = "queued"
	}
	return c.CreateMany(ctx, opt)
}

func (c *Client) appendNamedCounts(args *[]any, values map[string]any, refs map[string]string) error {
	*args = append(*args, len(values))
	for name, value := range values {
		encoded, err := c.encode(value)
		if err != nil {
			return err
		}
		*args = append(*args, name, encoded)
	}
	*args = append(*args, len(refs))
	for name, ref := range refs {
		*args = append(*args, name, ref)
	}
	return nil
}

func (c *Client) ValuePut(ctx context.Context, value any, opt ValuePutOptions) (any, error) {
	encoded, err := c.encode(value)
	if err != nil {
		return nil, err
	}
	args := []any{"FLOW.VALUE.PUT", encoded, "NOW", valueOrNow(opt.NowMS)}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "OWNER_FLOW_ID", opt.OwnerFlowID)
	appendOpt(&args, "NAME", opt.Name)
	appendBoolPtr(&args, "OVERRIDE", opt.Override)
	appendInt64Ptr(&args, "TTL", opt.TTLMS)
	return c.Command(ctx, args...)
}

func (c *Client) PutValue(ctx context.Context, name string, value any, opt ValuePutOptions) (any, error) {
	opt.Name = name
	return c.ValuePut(ctx, value, opt)
}

func (c *Client) ValueMGet(ctx context.Context, refs []string, maxBytes *int64) ([]any, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	args := []any{"FLOW.VALUE.MGET"}
	for _, ref := range refs {
		args = append(args, ref)
	}
	appendInt64Ptr(&args, "MAX_BYTES", maxBytes)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected value array, got %T", value)
	}
	out := make([]any, 0, len(items))
	for _, item := range items {
		decoded, err := decodeValue(c.codec, item)
		if err != nil {
			return nil, err
		}
		out = append(out, decoded)
	}
	return out, nil
}

func (c *Client) Signal(ctx context.Context, opt SignalOptions) (any, error) {
	args := []any{"FLOW.SIGNAL", opt.ID, "SIGNAL", opt.Signal}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "IDEMPOTENCY", opt.IdempotencyKey)
	for _, state := range opt.IfStates {
		appendOpt(&args, "IF_STATE", state)
	}
	appendOpt(&args, "TRANSITION_TO", opt.TransitionTo)
	if opt.RunAtMS != 0 {
		appendOpt(&args, "RUN_AT", opt.RunAtMS)
	}
	now := opt.NowMS
	if now == 0 {
		now = nowMS()
	}
	appendOpt(&args, "NOW", now)
	appendInt64Ptr(&args, "PRIORITY", opt.Priority)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	return c.Command(ctx, args...)
}

func (c *Client) FlowSignal(ctx context.Context, opt SignalOptions) (any, error) {
	return c.Signal(ctx, opt)
}

func (c *Client) ClaimDue(ctx context.Context, opt ClaimDueOptions) ([]FlowRecord, error) {
	value, err := c.claimDue(ctx, opt)
	if err != nil {
		return nil, err
	}
	return recordsFromRESP(value, c.codec)
}

func (c *Client) ClaimJobs(ctx context.Context, opt ClaimDueOptions) ([]ClaimedItem, error) {
	opt.JobOnly = true
	value, err := c.claimDue(ctx, opt)
	if err != nil {
		return nil, err
	}
	return claimedItemsFromRESP(value)
}

func (c *Client) claimDue(ctx context.Context, opt ClaimDueOptions) (any, error) {
	if opt.State != "" && len(opt.States) > 0 {
		return nil, errors.New("state and states are mutually exclusive")
	}
	if opt.PartitionKey != "" && len(opt.PartitionKeys) > 0 {
		return nil, errors.New("partition key and partition keys are mutually exclusive")
	}
	leaseMS := opt.LeaseMS
	if leaseMS == 0 {
		leaseMS = 30000
	}
	limit := opt.Limit
	if limit == 0 {
		limit = 1
	}
	args := []any{"FLOW.CLAIM_DUE", opt.Type}
	if len(opt.States) > 0 {
		for _, state := range opt.States {
			appendOpt(&args, "STATE", state)
		}
	} else {
		appendOpt(&args, "STATE", opt.State)
	}
	args = append(args, "WORKER", opt.Worker, "LEASE_MS", leaseMS, "LIMIT", limit)
	appendOpt(&args, "NOW", opt.NowMS)
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	if len(opt.PartitionKeys) > 0 {
		args = append(args, "PARTITIONS", len(opt.PartitionKeys))
		for _, key := range opt.PartitionKeys {
			args = append(args, key)
		}
	}
	appendInt64Ptr(&args, "PRIORITY", opt.Priority)
	if opt.IncludeState && !opt.JobOnly {
		return nil, errors.New("include state requires job only")
	}
	if opt.JobOnly {
		if opt.IncludeState {
			appendOpt(&args, "RETURN", "JOBS_COMPACT_STATE")
		} else {
			appendOpt(&args, "RETURN", "JOBS_COMPACT")
		}
	}
	appendInt64Ptr(&args, "BLOCK", opt.BlockMS)
	appendPayloadRead(&args, opt.Payload, opt.PayloadMaxBytes)
	appendValueReturn(&args, opt.Values, opt.ValueMaxBytes)
	appendBoolPtr(&args, "RECLAIM_EXPIRED", opt.ReclaimExpired)
	appendInt64Ptr(&args, "RECLAIM_RATIO", opt.ReclaimRatio)
	return c.Command(ctx, args...)
}

func (c *Client) Reclaim(ctx context.Context, opt ReclaimOptions) ([]FlowRecord, error) {
	value, err := c.reclaim(ctx, opt)
	if err != nil {
		return nil, err
	}
	return recordsFromRESP(value, c.codec)
}

func (c *Client) ReclaimJobs(ctx context.Context, opt ReclaimOptions) ([]ClaimedItem, error) {
	opt.JobOnly = true
	value, err := c.reclaim(ctx, opt)
	if err != nil {
		return nil, err
	}
	return claimedItemsFromRESP(value)
}

func (c *Client) reclaim(ctx context.Context, opt ReclaimOptions) (any, error) {
	if opt.State != "" && opt.State != "running" {
		return nil, errors.New("FLOW.RECLAIM only supports running state")
	}
	if opt.PartitionKey != "" && len(opt.PartitionKeys) > 0 {
		return nil, errors.New("partition key and partition keys are mutually exclusive")
	}
	leaseMS := opt.LeaseMS
	if leaseMS == 0 {
		leaseMS = 30000
	}
	limit := opt.Limit
	if limit == 0 {
		limit = 1
	}
	now := opt.NowMS
	if now == 0 {
		now = nowMS()
	}
	args := []any{"FLOW.RECLAIM", opt.Type, "WORKER", opt.Worker, "LEASE_MS", leaseMS, "LIMIT", limit, "NOW", now}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	if len(opt.PartitionKeys) > 0 {
		args = append(args, "PARTITIONS", len(opt.PartitionKeys))
		for _, key := range opt.PartitionKeys {
			args = append(args, key)
		}
	}
	appendInt64Ptr(&args, "PRIORITY", opt.Priority)
	if opt.JobOnly {
		appendOpt(&args, "RETURN", "JOBS_COMPACT")
	}
	appendPayloadRead(&args, opt.Payload, opt.PayloadMaxBytes)
	appendValueReturn(&args, opt.Values, opt.ValueMaxBytes)
	return c.Command(ctx, args...)
}

func (c *Client) ExtendLease(ctx context.Context, id, leaseToken string, fencingToken, leaseMS int64, partitionKey string) (*FlowRecord, error) {
	args := []any{"FLOW.EXTEND_LEASE", id, leaseToken, "FENCING", fencingToken, "LEASE_MS", leaseMS, "NOW", nowMS()}
	appendOpt(&args, "PARTITION", partitionKey)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordOrNil(value, c.codec)
}

func (c *Client) Transition(ctx context.Context, opt TransitionOptions) (*FlowRecord, error) {
	now := opt.NowMS
	if now == 0 {
		now = nowMS()
	}
	runAt := opt.RunAtMS
	if runAt == 0 {
		runAt = now
	}
	args := []any{"FLOW.TRANSITION", opt.ID, opt.FromState, opt.ToState, "LEASE_TOKEN", opt.LeaseToken, "FENCING", opt.FencingToken, "NOW", now}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	appendOpt(&args, "RUN_AT", runAt)
	appendInt64Ptr(&args, "PRIORITY", opt.Priority)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	value, err := c.Command(ctx, args...)
	if err != nil || !opt.ReturnRecord {
		return nil, err
	}
	return recordOrNil(value, c.codec)
}

func (c *Client) Complete(ctx context.Context, opt CompleteOptions) (*FlowRecord, error) {
	args := []any{"FLOW.COMPLETE", opt.ID, opt.LeaseToken, "FENCING", opt.FencingToken, "NOW", valueOrNow(opt.NowMS)}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	if err := c.appendEncoded(&args, "RESULT", opt.Result); err != nil {
		return nil, err
	}
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	appendInt64Ptr(&args, "TTL", opt.TTLMS)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	value, err := c.Command(ctx, args...)
	if err != nil || !opt.ReturnRecord {
		return nil, err
	}
	return recordOrNil(value, c.codec)
}

func (c *Client) Retry(ctx context.Context, opt RetryOptions) (*FlowRecord, error) {
	args := []any{"FLOW.RETRY", opt.ID, opt.LeaseToken, "FENCING", opt.FencingToken, "NOW", valueOrNow(opt.NowMS)}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	if err := c.appendEncoded(&args, "ERROR", opt.Error); err != nil {
		return nil, err
	}
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	if opt.RunAtMS != 0 {
		appendOpt(&args, "RUN_AT", opt.RunAtMS)
	}
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	value, err := c.Command(ctx, args...)
	if err != nil || !opt.ReturnRecord {
		return nil, err
	}
	return recordOrNil(value, c.codec)
}

func (c *Client) Fail(ctx context.Context, opt FailOptions) (*FlowRecord, error) {
	args := []any{"FLOW.FAIL", opt.ID, opt.LeaseToken, "FENCING", opt.FencingToken, "NOW", valueOrNow(opt.NowMS)}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	if err := c.appendEncoded(&args, "ERROR", opt.Error); err != nil {
		return nil, err
	}
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	appendInt64Ptr(&args, "TTL", opt.TTLMS)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	value, err := c.Command(ctx, args...)
	if err != nil || !opt.ReturnRecord {
		return nil, err
	}
	return recordOrNil(value, c.codec)
}

func (c *Client) Cancel(ctx context.Context, opt CancelOptions) (*FlowRecord, error) {
	args := []any{"FLOW.CANCEL", opt.ID, "FENCING", opt.FencingToken, "NOW", valueOrNow(opt.NowMS)}
	appendOpt(&args, "LEASE_TOKEN", opt.LeaseToken)
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	if err := c.appendEncoded(&args, "REASON", opt.Reason); err != nil {
		return nil, err
	}
	appendInt64Ptr(&args, "TTL", opt.TTLMS)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	value, err := c.Command(ctx, args...)
	if err != nil || !opt.ReturnRecord {
		return nil, err
	}
	return recordOrNil(value, c.codec)
}

func (c *Client) Rewind(ctx context.Context, opt RewindOptions) (*FlowRecord, error) {
	args := []any{"FLOW.REWIND", opt.ID, "TO_EVENT", opt.ToEvent, "NOW", valueOrNow(opt.NowMS)}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "EXPECT_STATE", opt.ExpectState)
	if opt.RunAtMS != 0 {
		appendOpt(&args, "RUN_AT", opt.RunAtMS)
	}
	appendOpt(&args, "REASON_REF", opt.ReasonRef)
	value, err := c.Command(ctx, args...)
	if err != nil || !opt.ReturnRecord {
		return nil, err
	}
	return recordOrNil(value, c.codec)
}

func (c *Client) CompleteMany(ctx context.Context, opt CompleteManyOptions) ([]FlowRecord, error) {
	if len(opt.Items) == 0 {
		return nil, nil
	}
	args := []any{"FLOW.COMPLETE_MANY", mixedPartition(opt.PartitionKey)}
	if err := c.appendEncoded(&args, "RESULT", opt.Result); err != nil {
		return nil, err
	}
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	appendInt64Ptr(&args, "TTL", opt.TTLMS)
	appendOpt(&args, "NOW", valueOrNow(opt.NowMS))
	appendBoolPtr(&args, "INDEPENDENT", opt.Independent)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	if err := appendClaimedItems(&args, opt.PartitionKey, opt.Items, "FLOW.COMPLETE_MANY"); err != nil {
		return nil, err
	}
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsOrNil(value, c.codec)
}

func (c *Client) TransitionMany(ctx context.Context, opt TransitionManyOptions) ([]FlowRecord, error) {
	if len(opt.Items) == 0 {
		return nil, nil
	}
	args := []any{"FLOW.TRANSITION_MANY", mixedPartition(opt.PartitionKey), opt.FromState, opt.ToState}
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	if opt.RunAtMS != 0 {
		appendOpt(&args, "RUN_AT", opt.RunAtMS)
	}
	appendInt64Ptr(&args, "PRIORITY", opt.Priority)
	appendOpt(&args, "NOW", valueOrNow(opt.NowMS))
	appendBoolPtr(&args, "INDEPENDENT", opt.Independent)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	if err := appendFencedItems(&args, opt.PartitionKey, opt.Items, "FLOW.TRANSITION_MANY", true); err != nil {
		return nil, err
	}
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsOrNil(value, c.codec)
}

func (c *Client) RetryMany(ctx context.Context, opt RetryManyOptions) ([]FlowRecord, error) {
	if len(opt.Items) == 0 {
		return nil, nil
	}
	args := []any{"FLOW.RETRY_MANY", mixedPartition(opt.PartitionKey)}
	if err := c.appendEncoded(&args, "ERROR", opt.Error); err != nil {
		return nil, err
	}
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	if opt.RunAtMS != 0 {
		appendOpt(&args, "RUN_AT", opt.RunAtMS)
	}
	appendOpt(&args, "NOW", valueOrNow(opt.NowMS))
	appendBoolPtr(&args, "INDEPENDENT", opt.Independent)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	if err := appendClaimedItems(&args, opt.PartitionKey, opt.Items, "FLOW.RETRY_MANY"); err != nil {
		return nil, err
	}
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsOrNil(value, c.codec)
}

func (c *Client) FailMany(ctx context.Context, opt FailManyOptions) ([]FlowRecord, error) {
	if len(opt.Items) == 0 {
		return nil, nil
	}
	args := []any{"FLOW.FAIL_MANY", mixedPartition(opt.PartitionKey)}
	if err := c.appendEncoded(&args, "ERROR", opt.Error); err != nil {
		return nil, err
	}
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	appendInt64Ptr(&args, "TTL", opt.TTLMS)
	appendOpt(&args, "NOW", valueOrNow(opt.NowMS))
	appendBoolPtr(&args, "INDEPENDENT", opt.Independent)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	if err := appendClaimedItems(&args, opt.PartitionKey, opt.Items, "FLOW.FAIL_MANY"); err != nil {
		return nil, err
	}
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsOrNil(value, c.codec)
}

func (c *Client) CancelMany(ctx context.Context, opt CancelManyOptions) ([]FlowRecord, error) {
	if len(opt.Items) == 0 {
		return nil, nil
	}
	args := []any{"FLOW.CANCEL_MANY", mixedPartition(opt.PartitionKey)}
	if err := c.appendEncoded(&args, "REASON", opt.Reason); err != nil {
		return nil, err
	}
	appendInt64Ptr(&args, "TTL", opt.TTLMS)
	appendOpt(&args, "NOW", valueOrNow(opt.NowMS))
	appendBoolPtr(&args, "INDEPENDENT", opt.Independent)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	if err := appendFencedItems(&args, opt.PartitionKey, opt.Items, "FLOW.CANCEL_MANY", false); err != nil {
		return nil, err
	}
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsOrNil(value, c.codec)
}

func (c *Client) Get(ctx context.Context, id string, partitionKey string, values []string, valueMaxBytes *int64) (*FlowRecord, error) {
	args := []any{"FLOW.GET", id}
	appendOpt(&args, "PARTITION", partitionKey)
	appendValueReturn(&args, values, valueMaxBytes)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordOrNil(value, c.codec)
}

func (c *Client) List(ctx context.Context, flowType string, opt ReadOptions) ([]FlowRecord, error) {
	args := []any{"FLOW.LIST", flowType}
	appendOpt(&args, "STATE", opt.State)
	appendIntPtr(&args, "COUNT", opt.Count)
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendBoolPtr(&args, "INCLUDE_COLD", opt.IncludeCold)
	appendBoolPtr(&args, "CONSISTENT_PROJECTION", opt.ConsistentProjection)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsFromRESP(value, c.codec)
}

func (c *Client) Terminals(ctx context.Context, flowType string, opt ReadOptions) ([]FlowRecord, error) {
	return c.indexRead(ctx, "FLOW.TERMINALS", flowType, opt)
}

func (c *Client) Failures(ctx context.Context, flowType string, opt ReadOptions) ([]FlowRecord, error) {
	return c.indexRead(ctx, "FLOW.FAILURES", flowType, opt)
}

func (c *Client) ByParent(ctx context.Context, parentFlowID string, opt ReadOptions) ([]FlowRecord, error) {
	return c.indexRead(ctx, "FLOW.BY_PARENT", parentFlowID, opt)
}

func (c *Client) ByRoot(ctx context.Context, rootFlowID string, opt ReadOptions) ([]FlowRecord, error) {
	return c.indexRead(ctx, "FLOW.BY_ROOT", rootFlowID, opt)
}

func (c *Client) ByCorrelation(ctx context.Context, correlationID string, opt ReadOptions) ([]FlowRecord, error) {
	return c.indexRead(ctx, "FLOW.BY_CORRELATION", correlationID, opt)
}

func (c *Client) indexRead(ctx context.Context, command, key string, opt ReadOptions) ([]FlowRecord, error) {
	args := []any{command, key}
	appendReadOptions(&args, opt)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsFromRESP(value, c.codec)
}

func appendReadOptions(args *[]any, opt ReadOptions) {
	appendIntPtr(args, "COUNT", opt.Count)
	appendOpt(args, "PARTITION", opt.PartitionKey)
	appendInt64Ptr(args, "FROM_MS", opt.FromMS)
	appendInt64Ptr(args, "TO_MS", opt.ToMS)
	appendBoolPtr(args, "REV", opt.Rev)
	appendOpt(args, "STATE", opt.State)
	appendBoolPtr(args, "TERMINAL_ONLY", opt.TerminalOnly)
	appendBoolPtr(args, "INCLUDE_COLD", opt.IncludeCold)
	appendBoolPtr(args, "CONSISTENT_PROJECTION", opt.ConsistentProjection)
}

func (c *Client) Info(ctx context.Context, flowType, partitionKey string, includeCold, consistentProjection *bool) (map[string]any, error) {
	args := []any{"FLOW.INFO", flowType}
	appendOpt(&args, "PARTITION", partitionKey)
	appendBoolPtr(&args, "INCLUDE_COLD", includeCold)
	appendBoolPtr(&args, "CONSISTENT_PROJECTION", consistentProjection)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return respMap(value)
}

func (c *Client) Stuck(ctx context.Context, flowType string, partitionKey string, count *int, olderThanMS, now *int64) ([]FlowRecord, error) {
	args := []any{"FLOW.STUCK", flowType}
	appendOpt(&args, "PARTITION", partitionKey)
	appendIntPtr(&args, "COUNT", count)
	appendInt64Ptr(&args, "OLDER_THAN", olderThanMS)
	appendInt64Ptr(&args, "NOW", now)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsFromRESP(value, c.codec)
}

func (c *Client) History(ctx context.Context, opt HistoryOptions) ([]any, error) {
	count := opt.Count
	if count == 0 {
		count = 100
	}
	args := []any{"FLOW.HISTORY", opt.ID, "COUNT", count}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "FROM_EVENT", opt.FromEvent)
	appendOpt(&args, "TO_EVENT", opt.ToEvent)
	appendInt64Ptr(&args, "FROM_MS", opt.FromMS)
	appendInt64Ptr(&args, "TO_MS", opt.ToMS)
	appendInt64Ptr(&args, "FROM_VERSION", opt.FromVersion)
	appendInt64Ptr(&args, "TO_VERSION", opt.ToVersion)
	appendBoolPtr(&args, "REV", opt.Rev)
	appendOpt(&args, "EVENT", opt.Event)
	appendOpt(&args, "WORKER", opt.Worker)
	appendBoolPtr(&args, "INCLUDE_COLD", opt.IncludeCold)
	appendBoolPtr(&args, "CONSISTENT_PROJECTION", opt.ConsistentProjection)
	appendBoolPtr(&args, "VALUES", opt.Values)
	appendInt64Ptr(&args, "PAYLOAD_MAX_BYTES", opt.PayloadMaxBytes)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected history array, got %T", value)
	}
	return items, nil
}

func (c *Client) SpawnChildren(ctx context.Context, opt SpawnChildrenOptions) (any, error) {
	group := opt.GroupID
	if group == "" {
		group = "default"
	}
	wait := opt.Wait
	if wait == "" {
		wait = "all"
	}
	args := []any{"FLOW.SPAWN_CHILDREN", opt.ParentID, "GROUP", group, "WAIT", wait, "NOW", valueOrNow(opt.NowMS)}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "LEASE_TOKEN", opt.LeaseToken)
	appendInt64Ptr(&args, "FENCING", opt.FencingToken)
	appendOpt(&args, "WAIT_STATE", opt.WaitState)
	appendOpt(&args, "SUCCESS", opt.Success)
	appendOpt(&args, "FAILURE", opt.Failure)
	appendOpt(&args, "FROM_STATE", opt.FromState)
	appendOpt(&args, "ON_CHILD_FAILED", opt.OnChildFailed)
	appendOpt(&args, "ON_PARENT_CLOSED", opt.OnParentClosed)
	mixed := anyChildPartition(opt.Children)
	extended := anyChildValues(opt.Children)
	if extended {
		args = append(args, "ITEMS_EXT", len(opt.Children))
		for _, child := range opt.Children {
			if mixed && child.PartitionKey == "" {
				return nil, errors.New("mixed spawn children require partition key")
			}
			partition := child.PartitionKey
			if partition == "" {
				partition = "-"
			}
			encoded, err := c.encode(child.Payload)
			if err != nil {
				return nil, err
			}
			args = append(args, child.ID, partition, child.Type, encoded)
			if err := c.appendNamedCounts(&args, mergeValues(opt.Values, child.Values), mergeRefs(opt.ValueRefs, child.ValueRefs)); err != nil {
				return nil, err
			}
		}
	} else {
		if err := c.appendNamedValues(&args, NamedValues{Values: opt.Values, ValueRefs: opt.ValueRefs}); err != nil {
			return nil, err
		}
		args = append(args, "ITEMS")
		if mixed {
			args = append(args, "MIXED")
		}
		for _, child := range opt.Children {
			encoded, err := c.encode(child.Payload)
			if err != nil {
				return nil, err
			}
			if mixed {
				if child.PartitionKey == "" {
					return nil, errors.New("mixed spawn children require partition key")
				}
				args = append(args, child.ID, child.PartitionKey, child.Type, encoded)
			} else {
				args = append(args, child.ID, child.Type, encoded)
			}
		}
	}
	return c.Command(ctx, args...)
}

func (c *Client) InstallPolicy(ctx context.Context, flowType string, retry *RetryPolicy, states map[string]RetryPolicy) (any, error) {
	args := []any{"FLOW.POLICY.SET", flowType}
	if retry != nil {
		appendRetryPolicy(&args, *retry)
	}
	for state, policy := range states {
		args = append(args, "STATE", state)
		appendRetryPolicy(&args, policy)
	}
	return c.Command(ctx, args...)
}

func (c *Client) PolicyGet(ctx context.Context, flowType, state string) (map[string]any, error) {
	args := []any{"FLOW.POLICY.GET", flowType}
	appendOpt(&args, "STATE", state)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return respMap(value)
}

func (c *Client) RetentionCleanup(ctx context.Context, opt RetentionCleanupOptions) (map[string]any, error) {
	args := []any{"FLOW.RETENTION_CLEANUP"}
	appendIntPtr(&args, "LIMIT", opt.Limit)
	appendInt64Ptr(&args, "NOW", opt.NowMS)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return respMap(value)
}

func appendRetryPolicy(args *[]any, policy RetryPolicy) {
	if policy.MaxRetries != 0 {
		appendOpt(args, "MAX_RETRIES", policy.MaxRetries)
	}
	appendOpt(args, "BACKOFF", policy.Backoff)
	if policy.BaseMS != 0 {
		appendOpt(args, "BASE_MS", policy.BaseMS)
	}
	if policy.MaxMS != 0 {
		appendOpt(args, "MAX_MS", policy.MaxMS)
	}
	if policy.JitterPct != 0 {
		appendOpt(args, "JITTER_PCT", policy.JitterPct)
	}
	appendOpt(args, "EXHAUSTED_TO", policy.ExhaustedTo)
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
	raw, err := respMap(response)
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

func valueOrNow(value int64) int64 {
	if value != 0 {
		return value
	}
	return nowMS()
}

func mixedPartition(partitionKey string) string {
	if partitionKey == "" {
		return "MIXED"
	}
	return partitionKey
}

func appendClaimedItems(args *[]any, partitionKey string, items []ClaimedItem, command string) error {
	*args = append(*args, "ITEMS")
	mixed := partitionKey == ""
	for _, item := range items {
		if mixed {
			if item.PartitionKey == "" {
				return fmt.Errorf("%s mixed items require partition key", command)
			}
			*args = append(*args, item.ID, item.PartitionKey, item.LeaseToken, item.FencingToken)
		} else {
			*args = append(*args, item.ID, item.LeaseToken, item.FencingToken)
		}
	}
	return nil
}

func appendFencedItems(args *[]any, partitionKey string, items []FencedItem, command string, includeLease bool) error {
	*args = append(*args, "ITEMS")
	mixed := partitionKey == ""
	for _, item := range items {
		if mixed {
			if item.PartitionKey == "" {
				return fmt.Errorf("%s mixed items require partition key", command)
			}
			*args = append(*args, item.ID, item.PartitionKey)
		} else {
			*args = append(*args, item.ID)
		}
		*args = append(*args, item.FencingToken)
		if includeLease {
			*args = append(*args, item.LeaseToken)
		}
	}
	return nil
}

func anyItemPartition(items []CreateItem) bool {
	for _, item := range items {
		if item.PartitionKey != "" {
			return true
		}
	}
	return false
}

func anyCreateItemValues(items []CreateItem) bool {
	for _, item := range items {
		if len(item.Values) > 0 || len(item.ValueRefs) > 0 {
			return true
		}
	}
	return false
}

func anyChildPartition(items []ChildSpec) bool {
	for _, item := range items {
		if item.PartitionKey != "" {
			return true
		}
	}
	return false
}

func anyChildValues(items []ChildSpec) bool {
	for _, item := range items {
		if len(item.Values) > 0 || len(item.ValueRefs) > 0 {
			return true
		}
	}
	return false
}

func mergeValues(base, item map[string]any) map[string]any {
	merged := map[string]any{}
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range item {
		merged[key] = value
	}
	return merged
}

func mergeRefs(base, item map[string]string) map[string]string {
	merged := map[string]string{}
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range item {
		merged[key] = value
	}
	return merged
}
