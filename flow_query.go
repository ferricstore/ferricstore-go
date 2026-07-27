package ferricstore

import (
	"context"
	"errors"
	"strings"
)

const (
	flowQueryLanguageVersion   = "FQL1"
	flowQueryRequestContract   = "ferric.flow.query.request/v1"
	flowQueryResultContract    = "ferric.flow.query.result/v1"
	flowExplainContract        = "ferric.flow.explain/v1"
	flowQueryIndexesContract   = "ferric.flow.query.indexes/v1"
	flowQueryMaxBytes          = 16 * 1024
	flowQueryMaxParameters     = 64
	flowQueryMaxParameterName  = 128
	flowQueryMaxParameterValue = 65_535
)

// FlowQueryPage describes continuation state for a bounded query page.
type FlowQueryPage struct {
	HasMore bool
	Cursor  string
}

// FlowQueryQuality describes the exactness and freshness guarantees attached
// to one query result.
type FlowQueryQuality struct {
	Exactness  string
	Freshness  string
	Coverage   string
	Pagination string
}

// FlowQueryUsage contains the server-enforced resource counters for one query.
type FlowQueryUsage struct {
	RangeSeeks           int64
	RangePages           int64
	ScannedEntries       int64
	ScannedBytes         int64
	HydratedRecords      int64
	ResidualChecks       int64
	DuplicateEntries     int64
	ResultRecords        int64
	ResponseBytes        int64
	MemoryHighWaterBytes int64
	WallTimeUS           int64
}

// FlowQueryResult is the versioned result of an ordinary FQL1 query. Exactly
// one of Records or Count is populated.
type FlowQueryResult struct {
	Version string
	Records []map[string]any
	Page    *FlowQueryPage
	Count   *int64
	Quality FlowQueryQuality
	Usage   FlowQueryUsage
	Raw     map[string]any
}

// FlowExplainCapabilities describes the specialized executor contracts a
// bounded point, history, or fixed-index plan requested and can use.
type FlowExplainCapabilities struct {
	Requested []string
	Available []string
	Missing   []string
	Raw       map[string]any
}

// FlowExplainResult is the redacted result of EXPLAIN or EXPLAIN ANALYZE.
// Raw retains fields added by future compatible server revisions.
type FlowExplainResult struct {
	Version          string
	QueryFingerprint string
	Status           string
	Plan             map[string]any
	Estimate         map[string]any
	Stats            map[string]any
	Quality          *FlowQueryQuality
	Bounds           map[string]any
	Pressure         map[string]any
	Decision         map[string]any
	Alternatives     []map[string]any
	Capabilities     *FlowExplainCapabilities
	Actual           *FlowQueryUsage
	Diagnostic       *FlowQueryError
	Raw              map[string]any
}

// FlowQueryErrorPosition identifies a one-based FQL diagnostic location.
type FlowQueryErrorPosition struct {
	Byte   int64
	Line   int64
	Column int64
}

// FlowQueryError preserves the server's actionable, value-redacted query
// diagnostic while unwrapping to the original transport error.
type FlowQueryError struct {
	Code         string
	Message      string
	Detail       string
	Hint         string
	Retryable    bool
	SafeToRetry  bool
	RetryAfterMS int64
	Position     *FlowQueryErrorPosition
	Context      map[string]any
	cause        error
}

func (e *FlowQueryError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	return e.Code
}

func (e *FlowQueryError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// FlowQueryIndexRegistry identifies the durable catalog snapshot observed by
// FLOW.QUERY.INDEXES.
type FlowQueryIndexRegistry struct {
	Epoch          uint64
	CatalogVersion uint64
}

// FlowQueryIndexServices reports availability of each catalog subsystem.
type FlowQueryIndexServices struct {
	Registry         string
	LifecycleWorker  string
	StatisticsStore  string
	StatisticsWorker string
	Raw              map[string]any
}

// FlowQueryIndexField describes one ordered composite-key component.
type FlowQueryIndexField struct {
	Name      string
	Direction string
	Encoding  string
	Raw       map[string]any
}

// FlowQueryIndexFormat identifies the derived storage codecs used by one
// index generation. Counter is empty when the index has no count prefix.
type FlowQueryIndexFormat struct {
	QueryRow string
	Key      string
	Entry    string
	Reverse  string
	Counter  string
	Raw      map[string]any
}

// FlowQueryIndexCoverage summarizes shard build and validation coverage.
type FlowQueryIndexCoverage struct {
	CompleteShards uint64
	TotalShards    uint64
	Validation     string
	Raw            map[string]any
}

// FlowQueryIndexBuild reports durable build progress.
type FlowQueryIndexBuild struct {
	Scope           string
	PhaseCounts     map[string]uint64
	CurrentPhases   []string
	CompletedShards uint64
	TotalShards     uint64
	ScannedRecords  uint64
	WrittenEntries  uint64
	WrittenBytes    uint64
	Raw             map[string]any
}

// FlowQueryIndexValidation reports exact catalog validation progress.
type FlowQueryIndexValidation struct {
	Scope           string
	Status          string
	PhaseCounts     map[string]uint64
	CurrentPhases   []string
	CompletedShards uint64
	TotalShards     uint64
	CheckedRecords  uint64
	CheckedEntries  uint64
	Mismatches      uint64
	FailureReason   string
	ValidatedAtMS   *uint64
	Raw             map[string]any
}

// FlowQueryIndexRetirement reports deletion progress for retired generations.
type FlowQueryIndexRetirement struct {
	Status               string
	PhaseCounts          map[string]uint64
	CurrentPhases        []string
	CompletedShards      *uint64
	TotalShards          *uint64
	DeletedEntries       *uint64
	DeletedBytes         *uint64
	RewrittenReverseRows *uint64
	Raw                  map[string]any
}

// FlowQueryIndexStatistics reports bounded index selectivity sample freshness.
type FlowQueryIndexStatistics struct {
	Status              string
	Samples             uint64
	FreshSamples        uint64
	StaleSamples        uint64
	FutureSamples       uint64
	OldestCollectedAtMS *uint64
	NewestCollectedAtMS *uint64
	OldestAgeMS         *uint64
	NewestAgeMS         *uint64
	Raw                 map[string]any
}

// FlowQueryIndex is the typed identity, storage format, lifecycle progress,
// and statistics summary of one index generation.
type FlowQueryIndex struct {
	ID             string
	Version        uint64
	BuildID        string
	Source         string
	State          string
	Queryable      bool
	Fields         []FlowQueryIndexField
	Workloads      []string
	CountPrefixes  []int64
	CoveringFields []string
	Format         FlowQueryIndexFormat
	Coverage       FlowQueryIndexCoverage
	Build          FlowQueryIndexBuild
	Validation     FlowQueryIndexValidation
	Retirement     FlowQueryIndexRetirement
	Statistics     FlowQueryIndexStatistics
	Raw            map[string]any
}

// FlowQueryIndexStatus is the OSS query-index management contract.
type FlowQueryIndexStatus struct {
	ContractVersion    string
	ObservedAtMS       uint64
	StatisticsMaxAgeMS uint64
	Registry           FlowQueryIndexRegistry
	Services           FlowQueryIndexServices
	Indexes            []FlowQueryIndex
	Raw                map[string]any
}

// FlowQuery executes one ordinary FQL1 query. Use FlowExplain or
// FlowExplainAnalyze for plan inspection.
func (c *Client) FlowQuery(ctx context.Context, query string, params map[string]any) (*FlowQueryResult, error) {
	if hasFlowExplainPrefix(query) {
		return nil, errors.New("FlowQuery does not accept EXPLAIN; use FlowExplain or FlowExplainAnalyze")
	}
	value, err := c.executeFlowQuery(ctx, query, params)
	if err != nil {
		return nil, err
	}
	return decodeFlowQueryResult(value)
}

// FlowExplain plans an FQL1 query without executing it.
func (c *Client) FlowExplain(ctx context.Context, query string, params map[string]any) (*FlowExplainResult, error) {
	return c.flowExplain(ctx, "EXPLAIN ", query, params)
}

// FlowExplainAnalyze executes an admitted bounded plan and returns actual
// usage without returning records or count values.
func (c *Client) FlowExplainAnalyze(ctx context.Context, query string, params map[string]any) (*FlowExplainResult, error) {
	return c.flowExplain(ctx, "EXPLAIN ANALYZE ", query, params)
}

func (c *Client) flowExplain(ctx context.Context, prefix, query string, params map[string]any) (*FlowExplainResult, error) {
	if err := validateFlowQueryText(query); err != nil {
		return nil, err
	}
	if hasFlowExplainPrefix(query) {
		return nil, errors.New("query already contains an EXPLAIN prefix")
	}
	query = prefix + query
	value, err := c.executeFlowQuery(ctx, query, params)
	if err != nil {
		return nil, err
	}
	return decodeFlowExplainResult(value)
}

func (c *Client) executeFlowQuery(ctx context.Context, query string, params map[string]any) (any, error) {
	prepared, err := prepareFlowQuery(query, params)
	if err != nil {
		return nil, err
	}
	var direct typedDirectCommand
	if exec, ok := c.exec.(flowQueryExecutor); ok {
		direct = func() (any, error) {
			return exec.executePreparedFlowQuery(ctx, prepared)
		}
	}
	value, _, err := c.typedCommandWithState(ctx, false, direct, prepared.commandArgs)
	if err != nil {
		return nil, wrapFlowQueryError(err)
	}
	return value, nil
}

// FlowQueryIndexes returns the bounded OSS index catalog, optionally filtered
// to all generations of one logical index ID.
func (c *Client) FlowQueryIndexes(ctx context.Context, indexIDs ...string) (*FlowQueryIndexStatus, error) {
	if len(indexIDs) > 1 {
		return nil, errors.New("FLOW.QUERY.INDEXES accepts at most one index id")
	}
	args := []any{"FLOW.QUERY.INDEXES"}
	if len(indexIDs) == 1 {
		indexID := indexIDs[0]
		if !validFlowQueryIndexID(indexID) {
			return nil, errors.New("query index id must be 1..64 ASCII letters, digits, '_', '-', ':', or '.'")
		}
		args = append(args, indexID)
	}
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, wrapFlowQueryError(err)
	}
	expectedID := ""
	if len(indexIDs) == 1 {
		expectedID = indexIDs[0]
	}
	return decodeFlowQueryIndexStatus(value, expectedID)
}

func hasFlowExplainPrefix(query string) bool {
	query = trimFlowQueryASCIIWhitespaceStart(query)
	const keyword = "EXPLAIN"
	if len(query) < len(keyword) || !strings.EqualFold(query[:len(keyword)], keyword) {
		return false
	}
	return len(query) == len(keyword) || isFlowQueryWhitespace(query[len(keyword)])
}

func isFlowQueryWhitespace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\n' || value == '\r'
}

func validFlowQueryIndexID(indexID string) bool {
	if len(indexID) == 0 || len(indexID) > 64 {
		return false
	}
	for index := 0; index < len(indexID); index++ {
		value := indexID[index]
		if (value < 'a' || value > 'z') &&
			(value < 'A' || value > 'Z') &&
			(value < '0' || value > '9') &&
			value != '_' && value != '-' && value != ':' && value != '.' {
			return false
		}
	}
	return true
}
