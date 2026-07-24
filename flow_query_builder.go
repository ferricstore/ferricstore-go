package ferricstore

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

const maxFlowQueryPartitionBytes = 65_535

type flowCollectionQuery struct {
	query          strings.Builder
	predicateCount int
	params         map[string]any
	orderField     string
	direction      string
	limit          int
}

func newFlowCollectionQuery(partitionKey string, count *int, reverse *bool, orderField string) (*flowCollectionQuery, error) {
	if partitionKey == "" || len(partitionKey) > maxFlowQueryPartitionBytes {
		return nil, fmt.Errorf("FLOW.QUERY partition key must be 1..%d bytes", maxFlowQueryPartitionBytes)
	}
	limit := defaultFlowResponseLimitV080
	if count != nil {
		limit = *count
	}
	if limit <= 0 || limit > defaultFlowResponseLimitV080 {
		return nil, fmt.Errorf("FLOW.QUERY limit must be between 1 and %d", defaultFlowResponseLimitV080)
	}
	direction := "ASC"
	if reverse != nil && *reverse {
		direction = "DESC"
	}
	builder := &flowCollectionQuery{
		params:     make(map[string]any),
		orderField: orderField,
		direction:  direction,
		limit:      limit,
	}
	builder.query.Grow(256)
	builder.query.WriteString("FROM runs WHERE ")
	builder.addEquality("partition_key", "partition_key", partitionKey)
	return builder, nil
}

func (builder *flowCollectionQuery) addEquality(field, parameter string, value any) {
	builder.startPredicate()
	builder.query.WriteString(field)
	builder.query.WriteString(" = @")
	builder.query.WriteString(parameter)
	builder.params[parameter] = value
}

func (builder *flowCollectionQuery) addPredicate(predicate string) {
	builder.startPredicate()
	builder.query.WriteString(predicate)
}

func (builder *flowCollectionQuery) startPredicate() {
	if builder.predicateCount > 0 {
		builder.query.WriteString(" AND ")
	}
	builder.predicateCount++
}

func (builder *flowCollectionQuery) addMetadataEquality(root, parameter string, value any, names ...string) {
	builder.startPredicate()
	builder.query.WriteString(root)
	for _, name := range names {
		builder.query.WriteString("['")
		writeFlowQueryMetadataName(&builder.query, name)
		builder.query.WriteString("']")
	}
	builder.query.WriteString(" = @")
	builder.query.WriteString(parameter)
	builder.params[parameter] = value
}

func (builder *flowCollectionQuery) addUpdatedWindow(fromMS, toMS *int64) error {
	if fromMS == nil && toMS == nil {
		return nil
	}
	from := int64(0)
	to := maxFlowExactIntegerV080
	if fromMS != nil {
		from = *fromMS
	}
	if toMS != nil {
		to = *toMS
	}
	if err := validateFlowExactNonNegative("from_ms", from); err != nil {
		return err
	}
	if err := validateFlowExactNonNegative("to_ms", to); err != nil {
		return err
	}
	if from > to {
		return errors.New("from_ms must not exceed to_ms")
	}
	builder.addPredicate("updated_at_ms BETWEEN @from_ms AND @to_ms")
	builder.params["from_ms"] = from
	builder.params["to_ms"] = to
	return nil
}

func (builder *flowCollectionQuery) addAttributes(attributes map[string]any) {
	type entry struct {
		name  string
		value any
	}
	entries := make([]entry, 0, len(attributes))
	for rawName, value := range attributes {
		entries = append(entries, entry{name: canonicalFlowMetadataKey(rawName), value: value})
	}
	slices.SortFunc(entries, func(left, right entry) int {
		return strings.Compare(left.name, right.name)
	})
	for index, entry := range entries {
		parameter := indexedFlowQueryParameter("attribute_", index)
		builder.addMetadataEquality("attribute", parameter, entry.value, entry.name)
	}
}

func (builder *flowCollectionQuery) addStateMeta(stateMeta map[string]map[string]any) {
	type entry struct {
		state string
		name  string
		value any
	}
	entryCount := 0
	for _, metadata := range stateMeta {
		entryCount += len(metadata)
	}
	entries := make([]entry, 0, entryCount)
	for rawState, metadata := range stateMeta {
		state := strings.TrimSpace(rawState)
		for rawName, value := range metadata {
			entries = append(entries, entry{state: state, name: canonicalFlowMetadataKey(rawName), value: value})
		}
	}
	slices.SortFunc(entries, func(left, right entry) int {
		if compared := strings.Compare(left.state, right.state); compared != 0 {
			return compared
		}
		return strings.Compare(left.name, right.name)
	})
	for index, entry := range entries {
		parameter := indexedFlowQueryParameter("state_meta_", index)
		builder.addMetadataEquality("state_meta", parameter, entry.value, entry.state, entry.name)
	}
}

func (builder *flowCollectionQuery) build() (string, map[string]any, error) {
	if builder.predicateCount > 12 {
		return "", nil, errors.New("FLOW.QUERY accepts at most 12 predicates")
	}
	builder.query.WriteString(" ORDER BY ")
	builder.query.WriteString(builder.orderField)
	builder.query.WriteByte(' ')
	builder.query.WriteString(builder.direction)
	builder.query.WriteString(" LIMIT ")
	var digits [20]byte
	builder.query.Write(strconv.AppendInt(digits[:0], int64(builder.limit), 10))
	builder.query.WriteString(" RETURN RECORDS")
	query := builder.query.String()
	if err := validateFlowQueryText(query); err != nil {
		return "", nil, err
	}
	return query, builder.params, nil
}

func writeFlowQueryMetadataName(builder *strings.Builder, name string) {
	for {
		prefix, suffix, found := strings.Cut(name, "'")
		builder.WriteString(prefix)
		if !found {
			return
		}
		builder.WriteString("''")
		name = suffix
	}
}

func indexedFlowQueryParameter(prefix string, index int) string {
	var digits [20]byte
	encoded := strconv.AppendInt(digits[:0], int64(index), 10)
	var builder strings.Builder
	builder.Grow(len(prefix) + len(encoded))
	builder.WriteString(prefix)
	builder.Write(encoded)
	return builder.String()
}

func validateFlowQueryReadOptions(opt ReadOptions) error {
	if err := validateFlowReadOptions(opt); err != nil {
		return err
	}
	if opt.IncludeCold != nil && *opt.IncludeCold {
		return errors.New("FLOW.QUERY does not expose INCLUDE_COLD")
	}
	if opt.ConsistentProjection != nil && *opt.ConsistentProjection {
		return errors.New("FLOW.QUERY does not expose CONSISTENT_PROJECTION")
	}
	if opt.Count != nil && *opt.Count > defaultFlowResponseLimitV080 {
		return fmt.Errorf("FLOW.QUERY limit must not exceed %d", defaultFlowResponseLimitV080)
	}
	return nil
}

func buildFlowListQuery(flowType string, opt ReadOptions) (string, map[string]any, error) {
	if err := validatePublicFlowType("flow type", flowType); err != nil {
		return "", nil, err
	}
	if err := validateFlowQueryReadOptions(opt); err != nil {
		return "", nil, err
	}
	if flowType == "any" && len(opt.Attributes) == 0 {
		return "", nil, errors.New("FLOW.QUERY list requires a concrete flow type or an attribute predicate")
	}
	if (opt.TerminalOnly == nil || !*opt.TerminalOnly) && opt.State == "any" && len(opt.Attributes) == 0 {
		return "", nil, errors.New("FLOW.QUERY list state any requires an attribute predicate")
	}
	builder, err := newFlowCollectionQuery(opt.PartitionKey, opt.Count, opt.Rev, "updated_at_ms")
	if err != nil {
		return "", nil, err
	}
	if flowType != "any" {
		builder.addEquality("type", "type", flowType)
	}
	if opt.TerminalOnly != nil && *opt.TerminalOnly {
		if err := addFlowTerminalPredicate(builder, opt.State); err != nil {
			return "", nil, err
		}
	} else if opt.State == "" {
		builder.addEquality("state", "state", "queued")
	} else if opt.State != "any" {
		builder.addEquality("state", "state", opt.State)
	}
	builder.addAttributes(opt.Attributes)
	if err := builder.addUpdatedWindow(opt.FromMS, opt.ToMS); err != nil {
		return "", nil, err
	}
	return builder.build()
}

func buildFlowSearchQuery(opt SearchOptions) (string, map[string]any, error) {
	if err := validateFlowSearch(opt); err != nil {
		return "", nil, err
	}
	if opt.PartitionKey == "" {
		return "", nil, errors.New("FLOW.QUERY search requires partition_key")
	}
	if len(opt.Attributes) == 0 && len(opt.StateMeta) == 0 {
		return "", nil, errors.New("FLOW.QUERY search requires an attribute or state_meta predicate")
	}
	if (opt.Type == "" || opt.Type == "any") && len(opt.StateMeta) > 0 {
		return "", nil, errors.New("FLOW.QUERY state_meta predicates require a concrete flow type")
	}
	if opt.IncludeCold != nil && *opt.IncludeCold {
		return "", nil, errors.New("FLOW.QUERY does not expose INCLUDE_COLD")
	}
	if opt.ConsistentProjection != nil && *opt.ConsistentProjection {
		return "", nil, errors.New("FLOW.QUERY does not expose CONSISTENT_PROJECTION")
	}
	builder, err := newFlowCollectionQuery(opt.PartitionKey, opt.Count, opt.Rev, "updated_at_ms")
	if err != nil {
		return "", nil, err
	}
	if opt.Type != "" && opt.Type != "any" {
		builder.addEquality("type", "type", opt.Type)
	}
	if opt.TerminalOnly != nil && *opt.TerminalOnly {
		if err := addFlowTerminalPredicate(builder, opt.State); err != nil {
			return "", nil, err
		}
	} else if opt.State != "" && opt.State != "any" {
		builder.addEquality("state", "state", opt.State)
	}
	builder.addAttributes(opt.Attributes)
	builder.addStateMeta(opt.StateMeta)
	if err := builder.addUpdatedWindow(opt.FromMS, opt.ToMS); err != nil {
		return "", nil, err
	}
	return builder.build()
}

func buildFlowTerminalQuery(flowType string, opt ReadOptions) (string, map[string]any, error) {
	if err := validatePublicFlowType("flow type", flowType); err != nil {
		return "", nil, err
	}
	if err := validateFlowQueryReadOptions(opt); err != nil {
		return "", nil, err
	}
	if flowType == "any" {
		return "", nil, errors.New("FLOW.QUERY terminals require a concrete flow type")
	}
	if len(opt.Attributes) > 0 {
		return "", nil, errors.New("FLOW.QUERY terminals do not support attribute predicates")
	}
	builder, err := newFlowCollectionQuery(opt.PartitionKey, opt.Count, opt.Rev, "updated_at_ms")
	if err != nil {
		return "", nil, err
	}
	if flowType != "any" {
		builder.addEquality("type", "type", flowType)
	}
	if err := addFlowTerminalPredicate(builder, opt.State); err != nil {
		return "", nil, err
	}
	if err := builder.addUpdatedWindow(opt.FromMS, opt.ToMS); err != nil {
		return "", nil, err
	}
	return builder.build()
}

func addFlowTerminalPredicate(builder *flowCollectionQuery, state string) error {
	switch state {
	case "", "any":
		builder.addPredicate("state IN (@terminal_0, @terminal_1, @terminal_2)")
		builder.params["terminal_0"] = "completed"
		builder.params["terminal_1"] = "failed"
		builder.params["terminal_2"] = "cancelled"
		return nil
	case "completed", "failed", "cancelled":
		builder.addEquality("state", "state", state)
		return nil
	default:
		return errors.New("terminal state must be completed, failed, cancelled, or any")
	}
}

func buildFlowFailureQuery(flowType string, opt ReadOptions) (string, map[string]any, error) {
	if opt.State != "" && opt.State != "any" && opt.State != "failed" {
		return "", nil, errors.New("FLOW failures state must be failed or any")
	}
	opt.State = "failed"
	opt.TerminalOnly = nil
	return buildFlowListQuery(flowType, opt)
}

func buildFlowLineageQuery(field, id string, opt ReadOptions) (string, map[string]any, error) {
	if err := validateRequiredText("flow lineage id", id); err != nil {
		return "", nil, err
	}
	if err := validateFlowQueryReadOptions(opt); err != nil {
		return "", nil, err
	}
	if len(opt.Attributes) > 0 {
		return "", nil, errors.New("FLOW.QUERY lineage does not support attribute predicates")
	}
	builder, err := newFlowCollectionQuery(opt.PartitionKey, opt.Count, opt.Rev, "updated_at_ms")
	if err != nil {
		return "", nil, err
	}
	builder.addEquality(field, "lineage_id", id)
	if opt.State != "" && opt.State != "any" {
		builder.addEquality("state", "state", opt.State)
	}
	if opt.TerminalOnly != nil && *opt.TerminalOnly {
		return "", nil, errors.New("terminal_only cannot be combined with a lineage query")
	}
	builder.addAttributes(opt.Attributes)
	if err := builder.addUpdatedWindow(opt.FromMS, opt.ToMS); err != nil {
		return "", nil, err
	}
	return builder.build()
}

func buildFlowStuckQuery(flowType, partitionKey string, count *int, olderThanMS, suppliedNow *int64) (string, map[string]any, error) {
	if err := validateFlowStuck(flowType, count, olderThanMS, suppliedNow); err != nil {
		return "", nil, err
	}
	if flowType == "any" {
		return "", nil, errors.New("FLOW.QUERY stuck requires a concrete flow type")
	}
	now := nowMS()
	if suppliedNow != nil {
		now = *suppliedNow
	}
	older := int64(0)
	if olderThanMS != nil {
		older = *olderThanMS
	}
	cutoff := now - older
	if cutoff < 0 {
		return "", nil, errors.New("older_than_ms must not exceed now_ms")
	}
	builder, err := newFlowCollectionQuery(partitionKey, count, nil, "lease_deadline_ms")
	if err != nil {
		return "", nil, err
	}
	builder.addEquality("type", "type", flowType)
	builder.addEquality("state", "state", "running")
	builder.addPredicate("lease_deadline_ms BETWEEN @lease_from_ms AND @lease_to_ms")
	builder.params["lease_from_ms"] = int64(0)
	builder.params["lease_to_ms"] = cutoff
	return builder.build()
}
