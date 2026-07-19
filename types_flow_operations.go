package ferricstore

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
	MaxActiveMS    any
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
	MaxActiveMS any
	Retry       *RetryPolicy
	States      map[string]RetryPolicy
	// Replace requests a complete policy replacement. Nil keeps the server's
	// default deep-patch behavior.
	Replace *bool
	// ExpectedGeneration enables compare-and-swap against a policy snapshot.
	ExpectedGeneration *int64
	// StatePolicies configures full state policies. Use this for FIFO/PARALLEL mode.
	StatePolicies map[string]FlowStatePolicy
	// IndexedAttributes is omitted when nil; a non-nil empty slice clears the index.
	IndexedAttributes []string
	IndexedStateMeta  string
	// IndexedStateMetaSet sends IndexedStateMeta even when it is empty, clearing the index.
	IndexedStateMetaSet bool
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
	ID               string
	LeaseToken       string
	FencingToken     int64
	PartitionKey     string
	Error            any
	Payload          any
	RunAtMS          int64
	NowMS            int64
	ReturnRecord     bool
	StateMeta        map[string]any
	AttributesMerge  map[string]any
	AttributesDelete []string
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
	PartitionKey     string
	Items            []ClaimedItem
	Error            any
	Payload          any
	RunAtMS          int64
	NowMS            int64
	Independent      *bool
	StateMeta        map[string]any
	AttributesMerge  map[string]any
	AttributesDelete []string
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
	ID             string
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
	MaxActiveMS    any
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
