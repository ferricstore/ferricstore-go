package ferricstore

type FlowRecord struct {
	ID               string
	Type             string
	State            string
	PartitionKey     string
	Payload          any
	LeaseToken       string
	FencingToken     int64
	Version          int64
	ParentFlowID     string
	RootFlowID       string
	CorrelationID    string
	RunState         string
	Attributes       map[string]any
	StateMeta        map[string]any
	IndexedStateMeta string
	Values           map[string]any
	ValueRefs        map[string]any
	ValueSizes       map[string]any
	ValueOmitted     map[string]any
	ValueMissing     map[string]any
	Raw              map[string]any
}

type CreateItem struct {
	ID           string
	Payload      any
	PartitionKey string
	Values       map[string]any
	ValueRefs    map[string]string
	Attributes   map[string]any
	StateMeta    map[string]any
}

type ChildSpec struct {
	ID           string
	Type         string
	Payload      any
	PartitionKey string
	Values       map[string]any
	ValueRefs    map[string]string
	Attributes   map[string]any
}

type ClaimedItem struct {
	ID           string
	LeaseToken   string
	FencingToken int64
	PartitionKey string
	Type         string
	State        string
	RunState     string
	Payload      any
	Attributes   map[string]any
}

type FencedItem struct {
	ID           string
	FencingToken int64
	LeaseToken   string
	PartitionKey string
}

type RetryPolicy struct {
	MaxRetries int
	// MaxRetriesSet sends MaxRetries even when it is zero.
	MaxRetriesSet bool
	Backoff       string
	BaseMS        int64
	// BaseMSSet sends BaseMS even when it is zero.
	BaseMSSet bool
	MaxMS     int64
	// MaxMSSet sends MaxMS even when it is zero.
	MaxMSSet  bool
	JitterPct int
	// JitterPctSet sends JitterPct even when it is zero.
	JitterPctSet bool
	ExhaustedTo  string
}

// FlowStateMode controls how FerricStore claims due work within one Flow state.
type FlowStateMode string

const (
	// FlowStateModeParallel keeps the default parallel claim behavior for a state.
	FlowStateModeParallel FlowStateMode = "PARALLEL"
	// FlowStateModeFIFO opts a state into per-partition FIFO claiming.
	FlowStateModeFIFO FlowStateMode = "FIFO"
)

// FlowStatePolicy configures state-scoped Flow policy such as FIFO mode and retry overrides.
type FlowStatePolicy struct {
	Mode  FlowStateMode
	Retry *RetryPolicy
}

// RequestContext carries safe caller metadata for server-side policy checks.
type RequestContext struct {
	Subject string
	Tenant  string
	Scopes  []string
}

type RequestContextOptions struct {
	RequestContext *RequestContext
}

type InvocationCreateOptions struct {
	Context        map[string]any
	IdempotencyKey string
	RequestContext *RequestContext
}

type InvocationPartitionListOptions struct {
	Scope          string
	RequestContext *RequestContext
}

type RateLimitResult struct {
	Status    string
	Count     int64
	Remaining int64
	ResetMS   int64
}

func (r RateLimitResult) Allowed() bool {
	return r.Status == "allowed"
}

type KeyInfo struct {
	Type           string
	ValueSize      int64
	TTLMS          int64
	HotCacheStatus string
	LastWriteShard int64
	Raw            map[string]any
}

type FetchOrComputeResult struct {
	Status       string
	Value        any
	ComputeToken any
}

type CreateOptions struct {
	ID             string
	Type           string
	State          string
	Payload        any
	PartitionKey   string
	ParentFlowID   string
	RootFlowID     string
	CorrelationID  string
	RunAtMS        int64
	NowMS          int64
	Priority       *int64
	Idempotent     *bool
	RetentionTTLMS *int64
	Values         map[string]any
	ValueRefs      map[string]string
	Attributes     map[string]any
	StateMeta      map[string]any
	ReturnRecord   bool
}

type CreateManyOptions struct {
	PartitionKey   string
	Items          []CreateItem
	Type           string
	State          string
	RunAtMS        int64
	NowMS          int64
	Priority       *int64
	Idempotent     *bool
	Independent    *bool
	RetentionTTLMS *int64
	Values         map[string]any
	ValueRefs      map[string]string
	Attributes     map[string]any
	StateMeta      map[string]any
}

type ClaimDueOptions struct {
	Type              string
	State             string
	States            []string
	Worker            string
	PartitionKey      string
	PartitionKeys     []string
	LeaseMS           int64
	Limit             int
	Priority          *int64
	NowMS             int64
	BlockMS           *int64
	ReclaimExpired    *bool
	ReclaimRatio      *int64
	JobOnly           bool
	Payload           *bool
	PayloadMaxBytes   *int64
	Values            []string
	ValueMaxBytes     *int64
	IncludeState      bool
	IncludeAttributes *bool
}

type ReclaimOptions struct {
	Type              string
	State             string
	Worker            string
	PartitionKey      string
	PartitionKeys     []string
	LeaseMS           int64
	Limit             int
	Priority          *int64
	NowMS             int64
	JobOnly           bool
	Payload           *bool
	PayloadMaxBytes   *int64
	Values            []string
	ValueMaxBytes     *int64
	IncludeAttributes *bool
}

type NamedValues struct {
	Values           map[string]any
	ValueRefs        map[string]string
	DropValues       []string
	OverrideValues   []string
	AttributesMerge  map[string]any
	AttributesDelete []string
}
