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
	MaxRetries  int
	Backoff     string
	BaseMS      int64
	MaxMS       int64
	JitterPct   int
	ExhaustedTo string
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

type StartAndClaimOptions struct {
	ID             string
	Type           string
	InitialState   string
	Worker         string
	LeaseMS        int64
	Payload        any
	PartitionKey   string
	ParentFlowID   string
	RootFlowID     string
	CorrelationID  string
	NowMS          int64
	Priority       *int64
	RetentionTTLMS *int64
	Values         map[string]any
	ValueRefs      map[string]string
	Attributes     map[string]any
	StateMeta      map[string]any
}

type StepContinueOptions struct {
	ID           string
	LeaseToken   string
	FromState    string
	ToState      string
	FencingToken int64
	LeaseMS      int64
	PartitionKey string
	Payload      any
	Worker       string
	NowMS        int64
	StateMeta    map[string]any
	NamedValues
}

type RunStepsItem struct {
	ID           string
	PartitionKey string
}

type RunStepsManyOptions struct {
	Items          []RunStepsItem
	Type           string
	States         []string
	Steps          int
	Worker         string
	LeaseMS        int64
	NowMS          int64
	Payload        any
	Result         any
	PartitionKey   string
	RetentionTTLMS *int64
}

type SearchOptions struct {
	Type                 string
	State                string
	PartitionKey         string
	Count                *int
	FromMS               *int64
	ToMS                 *int64
	Rev                  *bool
	TerminalOnly         *bool
	IncludeCold          *bool
	ConsistentProjection *bool
	Attributes           map[string]any
	StateMeta            map[string]map[string]any
}

type PolicyOptions struct {
	Retry  *RetryPolicy
	States map[string]RetryPolicy
	// StatePolicies configures full state policies. Use this for FIFO/PARALLEL mode.
	StatePolicies     map[string]FlowStatePolicy
	IndexedAttributes []string
	IndexedStateMeta  string
}

type CompleteOptions struct {
	ID           string
	LeaseToken   string
	FencingToken int64
	PartitionKey string
	Result       any
	Payload      any
	TTLMS        *int64
	NowMS        int64
	ReturnRecord bool
	StateMeta    map[string]any
	NamedValues
}

type TransitionOptions struct {
	ID           string
	FromState    string
	ToState      string
	LeaseToken   string
	FencingToken int64
	PartitionKey string
	Payload      any
	RunAtMS      int64
	NowMS        int64
	Priority     *int64
	ReturnRecord bool
	StateMeta    map[string]any
	NamedValues
}

type RetryOptions struct {
	ID           string
	LeaseToken   string
	FencingToken int64
	PartitionKey string
	Error        any
	Payload      any
	RunAtMS      int64
	NowMS        int64
	ReturnRecord bool
	StateMeta    map[string]any
	NamedValues
}

type FailOptions struct {
	ID           string
	LeaseToken   string
	FencingToken int64
	PartitionKey string
	Error        any
	Payload      any
	TTLMS        *int64
	NowMS        int64
	ReturnRecord bool
	StateMeta    map[string]any
	NamedValues
}

type CancelOptions struct {
	ID           string
	FencingToken int64
	LeaseToken   string
	PartitionKey string
	Reason       any
	TTLMS        *int64
	NowMS        int64
	ReturnRecord bool
	StateMeta    map[string]any
	NamedValues
}

type RewindOptions struct {
	ID           string
	ToEvent      string
	PartitionKey string
	ExpectState  string
	RunAtMS      int64
	ReasonRef    string
	NowMS        int64
	ReturnRecord bool
}

type CompleteManyOptions struct {
	PartitionKey string
	Items        []ClaimedItem
	Result       any
	Payload      any
	TTLMS        *int64
	NowMS        int64
	Independent  *bool
	StateMeta    map[string]any
	NamedValues
}

type TransitionManyOptions struct {
	PartitionKey string
	FromState    string
	ToState      string
	Items        []FencedItem
	Payload      any
	RunAtMS      int64
	NowMS        int64
	Priority     *int64
	Independent  *bool
	StateMeta    map[string]any
	NamedValues
}

type RetryManyOptions struct {
	PartitionKey string
	Items        []ClaimedItem
	Error        any
	Payload      any
	RunAtMS      int64
	NowMS        int64
	Independent  *bool
	StateMeta    map[string]any
	NamedValues
}

type FailManyOptions struct {
	PartitionKey string
	Items        []ClaimedItem
	Error        any
	Payload      any
	TTLMS        *int64
	NowMS        int64
	Independent  *bool
	StateMeta    map[string]any
	NamedValues
}

type CancelManyOptions struct {
	PartitionKey string
	Items        []FencedItem
	Reason       any
	TTLMS        *int64
	NowMS        int64
	Independent  *bool
	StateMeta    map[string]any
	NamedValues
}

type SignalOptions struct {
	ID             string
	Signal         string
	PartitionKey   string
	IdempotencyKey string
	IfStates       []string
	TransitionTo   string
	RunAtMS        int64
	NowMS          int64
	Priority       *int64
	NamedValues
}

type ValuePutOptions struct {
	PartitionKey string
	OwnerFlowID  string
	Name         string
	Override     *bool
	TTLMS        *int64
	NowMS        int64
}

type ReadOptions struct {
	PartitionKey         string
	Count                *int
	FromMS               *int64
	ToMS                 *int64
	Rev                  *bool
	State                string
	TerminalOnly         *bool
	IncludeCold          *bool
	ConsistentProjection *bool
	Attributes           map[string]any
}

type HistoryOptions struct {
	ID                   string
	PartitionKey         string
	Count                int
	FromEvent            string
	ToEvent              string
	FromMS               *int64
	ToMS                 *int64
	FromVersion          *int64
	ToVersion            *int64
	Rev                  *bool
	Event                string
	Worker               string
	IncludeCold          *bool
	ConsistentProjection *bool
	Values               *bool
	PayloadMaxBytes      *int64
}

type SpawnChildrenOptions struct {
	ParentID       string
	Children       []ChildSpec
	PartitionKey   string
	LeaseToken     string
	FencingToken   *int64
	GroupID        string
	Wait           string
	WaitState      string
	Success        string
	Failure        string
	FromState      string
	OnChildFailed  string
	OnParentClosed string
	NowMS          int64
	Values         map[string]any
	ValueRefs      map[string]string
}

type RetentionCleanupOptions struct {
	Limit *int
	NowMS *int64
}

func Bool(value bool) *bool    { return &value }
func Int64(value int64) *int64 { return &value }
func Int(value int) *int       { return &value }
