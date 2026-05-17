package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Executor interface {
	Do(ctx context.Context, args ...any) *redis.Cmd
}

type Client struct {
	exec Executor
}

func NewClient(addr string) *Client {
	return NewClientFromRedis(redis.NewClient(&redis.Options{
		Addr:     addr,
		Protocol: 3,
	}))
}

func NewClientFromRedis(rdb *redis.Client) *Client {
	return &Client{exec: rdb}
}

func NewClientWithExecutor(exec Executor) *Client {
	return &Client{exec: exec}
}

func nowMS() int64 {
	return time.Now().UnixMilli()
}

type FlowRecord struct {
	ID            string
	Type          string
	State         string
	PartitionKey  string
	Payload       []byte
	LeaseToken    string
	FencingToken  int64
	Version       int64
	ParentFlowID  string
	RootFlowID    string
	CorrelationID string
	Raw           map[string]any
}

type CreateOptions struct {
	ID            string
	Type          string
	State         string
	Payload       []byte
	PartitionKey  string
	ParentFlowID  string
	RootFlowID    string
	CorrelationID string
	RunAtMS       int64
	NowMS         int64
	Priority      *int64
	Idempotent    *bool
	ReturnRecord  bool
}

type CreateItem struct {
	ID           string
	Payload      []byte
	PartitionKey string
}

type CreateManyOptions struct {
	PartitionKey string
	Items        []CreateItem
	Type         string
	State        string
	RunAtMS      int64
	NowMS        int64
	Priority     *int64
	Idempotent   *bool
	Independent  *bool
}

type ClaimDueOptions struct {
	Type           string
	State          string
	Worker         string
	PartitionKey   string
	LeaseMS        int64
	Limit          int
	NowMS          int64
	ReclaimExpired *bool
	ReclaimRatio   *int64
}

type CompleteOptions struct {
	ID           string
	LeaseToken   string
	FencingToken int64
	PartitionKey string
	Result       []byte
	Payload      []byte
	TTLMS        *int64
	NowMS        int64
	ReturnRecord bool
}

type TransitionOptions struct {
	ID           string
	FromState    string
	ToState      string
	LeaseToken   string
	FencingToken int64
	PartitionKey string
	Payload      []byte
	RunAtMS      int64
	NowMS        int64
	Priority     *int64
	ReturnRecord bool
}

type ClaimedItem struct {
	ID           string
	LeaseToken   string
	FencingToken int64
	PartitionKey string
}

type CompleteManyOptions struct {
	PartitionKey string
	Items        []ClaimedItem
	Result       []byte
	Payload      []byte
	TTLMS        *int64
	NowMS        int64
	Independent  *bool
}

func (c *Client) Create(ctx context.Context, opt CreateOptions) error {
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
	appendOpt(&args, "PAYLOAD", opt.Payload)
	appendOpt(&args, "PARENT_FLOW_ID", opt.ParentFlowID)
	appendOpt(&args, "ROOT_FLOW_ID", opt.RootFlowID)
	appendOpt(&args, "CORRELATION_ID", opt.CorrelationID)
	appendOpt(&args, "RUN_AT", runAt)
	appendIntPtr(&args, "PRIORITY", opt.Priority)
	appendBoolPtr(&args, "IDEMPOTENT", opt.Idempotent)
	return c.exec.Do(ctx, args...).Err()
}

func (c *Client) CreateMany(ctx context.Context, opt CreateManyOptions) error {
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
	mixed := opt.PartitionKey == ""
	wirePartition := opt.PartitionKey
	if mixed {
		wirePartition = "MIXED"
	}
	args := []any{"FLOW.CREATE_MANY", wirePartition, "TYPE", opt.Type, "STATE", state, "NOW", now}
	appendOpt(&args, "RUN_AT", runAt)
	appendIntPtr(&args, "PRIORITY", opt.Priority)
	appendBoolPtr(&args, "IDEMPOTENT", opt.Idempotent)
	appendBoolPtr(&args, "INDEPENDENT", opt.Independent)
	args = append(args, "ITEMS")
	for _, item := range opt.Items {
		if mixed {
			if item.PartitionKey == "" {
				return errors.New("mixed create_many items require partition key")
			}
			args = append(args, item.ID, item.PartitionKey, item.Payload)
		} else {
			args = append(args, item.ID, item.Payload)
		}
	}
	return c.exec.Do(ctx, args...).Err()
}

func (c *Client) ClaimDue(ctx context.Context, opt ClaimDueOptions) ([]FlowRecord, error) {
	state := opt.State
	if state == "" {
		state = "queued"
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
	args := []any{
		"FLOW.CLAIM_DUE", opt.Type,
		"STATE", state,
		"WORKER", opt.Worker,
		"LEASE_MS", leaseMS,
		"LIMIT", limit,
		"NOW", now,
	}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendBoolPtr(&args, "RECLAIM_EXPIRED", opt.ReclaimExpired)
	appendIntPtr(&args, "RECLAIM_RATIO", opt.ReclaimRatio)
	value, err := c.exec.Do(ctx, args...).Result()
	if err != nil {
		return nil, err
	}
	return recordsFromRESP(value)
}

func (c *Client) Complete(ctx context.Context, opt CompleteOptions) error {
	now := opt.NowMS
	if now == 0 {
		now = nowMS()
	}
	args := []any{"FLOW.COMPLETE", opt.ID, opt.LeaseToken, "FENCING", opt.FencingToken, "NOW", now}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "RESULT", opt.Result)
	appendOpt(&args, "PAYLOAD", opt.Payload)
	appendIntPtr(&args, "TTL", opt.TTLMS)
	return c.exec.Do(ctx, args...).Err()
}

func (c *Client) Transition(ctx context.Context, opt TransitionOptions) error {
	now := opt.NowMS
	if now == 0 {
		now = nowMS()
	}
	runAt := opt.RunAtMS
	if runAt == 0 {
		runAt = now
	}
	args := []any{
		"FLOW.TRANSITION", opt.ID, opt.FromState, opt.ToState,
		"LEASE_TOKEN", opt.LeaseToken,
		"FENCING", opt.FencingToken,
		"NOW", now,
	}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "PAYLOAD", opt.Payload)
	appendOpt(&args, "RUN_AT", runAt)
	appendIntPtr(&args, "PRIORITY", opt.Priority)
	return c.exec.Do(ctx, args...).Err()
}

func (c *Client) CompleteMany(ctx context.Context, opt CompleteManyOptions) error {
	wirePartition := opt.PartitionKey
	mixed := wirePartition == ""
	if mixed {
		wirePartition = "MIXED"
	}
	now := opt.NowMS
	if now == 0 {
		now = nowMS()
	}
	args := []any{"FLOW.COMPLETE_MANY", wirePartition}
	appendOpt(&args, "RESULT", opt.Result)
	appendOpt(&args, "PAYLOAD", opt.Payload)
	appendIntPtr(&args, "TTL", opt.TTLMS)
	appendOpt(&args, "NOW", now)
	appendBoolPtr(&args, "INDEPENDENT", opt.Independent)
	args = append(args, "ITEMS")
	for _, item := range opt.Items {
		if mixed {
			if item.PartitionKey == "" {
				return errors.New("mixed complete_many items require partition key")
			}
			args = append(args, item.ID, item.PartitionKey, item.LeaseToken, item.FencingToken)
		} else {
			args = append(args, item.ID, item.LeaseToken, item.FencingToken)
		}
	}
	return c.exec.Do(ctx, args...).Err()
}

func (c *Client) Incr(ctx context.Context, key string) error {
	return c.exec.Do(ctx, "INCR", key).Err()
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
	if value != nil {
		if *value {
			*args = append(*args, name, "true")
		} else {
			*args = append(*args, name, "false")
		}
	}
}

func appendIntPtr(args *[]any, name string, value *int64) {
	if value != nil {
		*args = append(*args, name, *value)
	}
}

func recordsFromRESP(value any) ([]FlowRecord, error) {
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected RESP array, got %T", value)
	}
	records := make([]FlowRecord, 0, len(items))
	for _, item := range items {
		mapping, err := respMap(item)
		if err != nil {
			return nil, err
		}
		records = append(records, recordFromMap(mapping))
	}
	return records, nil
}

func recordFromMap(m map[string]any) FlowRecord {
	return FlowRecord{
		ID:            asString(m["id"]),
		Type:          asString(m["type"]),
		State:         asString(m["state"]),
		PartitionKey:  asString(m["partition_key"]),
		Payload:       asBytes(m["payload"]),
		LeaseToken:    asString(m["lease_token"]),
		FencingToken:  asInt64(m["fencing_token"]),
		Version:       asInt64(m["version"]),
		ParentFlowID:  asString(m["parent_flow_id"]),
		RootFlowID:    asString(m["root_flow_id"]),
		CorrelationID: asString(m["correlation_id"]),
		Raw:           m,
	}
}

func respMap(value any) (map[string]any, error) {
	switch v := value.(type) {
	case map[interface{}]interface{}:
		out := make(map[string]any, len(v))
		for key, val := range v {
			out[asString(key)] = val
		}
		return out, nil
	case map[string]interface{}:
		out := make(map[string]any, len(v))
		for key, val := range v {
			out[key] = val
		}
		return out, nil
	case []any:
		if len(v)%2 != 0 {
			return nil, fmt.Errorf("odd RESP map array length %d", len(v))
		}
		out := make(map[string]any, len(v)/2)
		for i := 0; i < len(v); i += 2 {
			out[asString(v[i])] = v[i+1]
		}
		return out, nil
	default:
		return nil, fmt.Errorf("expected RESP map, got %T", value)
	}
}

func asString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	default:
		return fmt.Sprint(v)
	}
}

func asBytes(value any) []byte {
	switch v := value.(type) {
	case nil:
		return nil
	case []byte:
		return v
	case string:
		return []byte(v)
	default:
		return []byte(fmt.Sprint(v))
	}
}

func asInt64(value any) int64 {
	switch v := value.(type) {
	case nil:
		return 0
	case int64:
		return v
	case int:
		return int64(v)
	case string:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	case []byte:
		n, _ := strconv.ParseInt(string(v), 10, 64)
		return n
	default:
		n, _ := strconv.ParseInt(fmt.Sprint(v), 10, 64)
		return n
	}
}
