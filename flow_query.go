package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode/utf8"
)

const (
	flowQueryLanguageVersion  = "FQL1"
	flowQueryRequestContract  = "ferric.flow.query.request/v1"
	flowQueryResultContract   = "ferric.flow.query.result/v1"
	flowExplainContract       = "ferric.flow.explain/v1"
	flowQueryIndexesContract  = "ferric.flow.query.indexes/v1"
	flowQueryMaxBytes         = 16 * 1024
	flowQueryMaxParameters    = 64
	flowQueryMaxParameterName = 128
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

// FlowExplainResult is the redacted result of EXPLAIN or EXPLAIN ANALYZE.
// Raw retains fields added by future compatible server revisions.
type FlowExplainResult struct {
	Version          string
	QueryFingerprint string
	Status           string
	Plan             map[string]any
	Estimate         map[string]any
	Bounds           map[string]any
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

// FlowQueryIndex is the stable identity and lifecycle summary of one index
// generation. Raw contains its bounded progress and statistics details.
type FlowQueryIndex struct {
	ID        string
	Version   uint64
	BuildID   string
	State     string
	Queryable bool
	Raw       map[string]any
}

// FlowQueryIndexStatus is the OSS query-index management contract.
type FlowQueryIndexStatus struct {
	ContractVersion    string
	ObservedAtMS       int64
	StatisticsMaxAgeMS int64
	Registry           FlowQueryIndexRegistry
	Services           map[string]any
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
	query = strings.TrimSpace(query)
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
	args, err := flowQueryCommandArgs(query, params)
	if err != nil {
		return nil, err
	}
	value, err := c.typedReply(ctx, args...)
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
	return decodeFlowQueryIndexStatus(value)
}

func flowQueryCommandArgs(query string, params map[string]any) ([]any, error) {
	if err := validateFlowQueryText(query); err != nil {
		return nil, err
	}
	if len(params) > flowQueryMaxParameters {
		return nil, fmt.Errorf("FLOW.QUERY accepts at most %d named parameters", flowQueryMaxParameters)
	}
	type parameter struct {
		name  string
		value any
	}
	parameters := make([]parameter, 0, len(params))
	for name, value := range params {
		if !utf8.ValidString(name) {
			return nil, errors.New("FLOW.QUERY parameter names must be valid UTF-8")
		}
		if name == "" || len(name) > flowQueryMaxParameterName {
			return nil, fmt.Errorf("FLOW.QUERY parameter names must be 1..%d bytes", flowQueryMaxParameterName)
		}
		normalized, err := normalizeFlowQueryParameter(value)
		if err != nil {
			return nil, fmt.Errorf("FLOW.QUERY parameter %q: %w", name, err)
		}
		parameters = append(parameters, parameter{name: name, value: normalized})
	}
	slices.SortFunc(parameters, func(left, right parameter) int {
		return strings.Compare(left.name, right.name)
	})
	args := make([]any, 0, 3+len(parameters)*2)
	args = append(args, "FLOW.QUERY", flowQueryLanguageVersion, query)
	for _, parameter := range parameters {
		args = append(args, parameter.name, parameter.value)
	}
	return args, nil
}

func validateFlowQueryText(query string) error {
	if !utf8.ValidString(query) {
		return errors.New("FLOW.QUERY query must be valid UTF-8")
	}
	if strings.TrimSpace(query) == "" {
		return errors.New("FLOW.QUERY query must not be empty")
	}
	if len(query) > flowQueryMaxBytes {
		return fmt.Errorf("FLOW.QUERY query exceeds %d bytes", flowQueryMaxBytes)
	}
	return nil
}

func normalizeFlowQueryParameter(value any) (any, error) {
	switch typed := value.(type) {
	case string:
		if !utf8.ValidString(typed) {
			return nil, errors.New("text values must be valid UTF-8")
		}
		return typed, nil
	case []byte:
		return typed, nil
	case bool:
		return typed, nil
	case float32:
		value := float64(typed)
		if !math.IsNaN(value) && !math.IsInf(value, 0) {
			return value, nil
		}
	case float64:
		if !math.IsNaN(typed) && !math.IsInf(typed, 0) {
			return typed, nil
		}
	case int:
		return int64(typed), nil
	case int8:
		return int64(typed), nil
	case int16:
		return int64(typed), nil
	case int32:
		return int64(typed), nil
	case int64:
		return typed, nil
	case uint:
		if uint64(typed) <= math.MaxInt64 {
			return int64(typed), nil
		}
	case uint8:
		return int64(typed), nil
	case uint16:
		return int64(typed), nil
	case uint32:
		return int64(typed), nil
	case uint64:
		if typed <= math.MaxInt64 {
			return int64(typed), nil
		}
	}
	return nil, errors.New("value must be a string, byte slice, boolean, finite float, or signed 64-bit integer")
}

func hasFlowExplainPrefix(query string) bool {
	query = strings.TrimSpace(query)
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
