package ferricstore

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

const maxFlowQueryPartitionBytes = 65_535

type flowCollectionQuery struct {
	predicates []string
	params     map[string]any
	orderField string
	direction  string
	limit      int
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
	return &flowCollectionQuery{
		predicates: []string{"partition_key = @partition_key"},
		params:     map[string]any{"partition_key": partitionKey},
		orderField: orderField,
		direction:  direction,
		limit:      limit,
	}, nil
}

func (builder *flowCollectionQuery) addEquality(field, parameter string, value any) {
	builder.predicates = append(builder.predicates, field+" = @"+parameter)
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
	builder.predicates = append(builder.predicates, "updated_at_ms BETWEEN @from_ms AND @to_ms")
	builder.params["from_ms"] = from
	builder.params["to_ms"] = to
	return nil
}

func (builder *flowCollectionQuery) addAttributes(attributes map[string]any) error {
	if err := validateFlowAttributes(attributes); err != nil {
		return err
	}
	names := make([]string, 0, len(attributes))
	values := make(map[string]any, len(attributes))
	for rawName, value := range attributes {
		name := canonicalFlowMetadataKey(rawName)
		names = append(names, name)
		values[name] = value
	}
	sort.Strings(names)
	for index, name := range names {
		parameter := fmt.Sprintf("attribute_%d", index)
		builder.addEquality(flowQueryMetadataSelector("attribute", name), parameter, values[name])
	}
	return nil
}

func (builder *flowCollectionQuery) addStateMeta(stateMeta map[string]map[string]any) error {
	if err := validateFlowStateMetaQuery(stateMeta); err != nil {
		return err
	}
	type entry struct {
		state string
		name  string
		value any
	}
	entries := make([]entry, 0)
	for rawState, metadata := range stateMeta {
		state := strings.TrimSpace(rawState)
		for rawName, value := range metadata {
			entries = append(entries, entry{state: state, name: canonicalFlowMetadataKey(rawName), value: value})
		}
	}
	sort.Slice(entries, func(left, right int) bool {
		if entries[left].state == entries[right].state {
			return entries[left].name < entries[right].name
		}
		return entries[left].state < entries[right].state
	})
	for index, entry := range entries {
		parameter := fmt.Sprintf("state_meta_%d", index)
		selector := flowQueryMetadataSelector("state_meta", entry.state, entry.name)
		builder.addEquality(selector, parameter, entry.value)
	}
	return nil
}

func (builder *flowCollectionQuery) build() (string, map[string]any, error) {
	if len(builder.predicates) > 12 {
		return "", nil, errors.New("FLOW.QUERY accepts at most 12 predicates")
	}
	query := fmt.Sprintf(
		"FROM runs WHERE %s ORDER BY %s %s LIMIT %d RETURN RECORDS",
		strings.Join(builder.predicates, " AND "), builder.orderField, builder.direction, builder.limit,
	)
	if err := validateFlowQueryText(query); err != nil {
		return "", nil, err
	}
	return query, builder.params, nil
}

func flowQueryMetadataSelector(root string, names ...string) string {
	var builder strings.Builder
	builder.Grow(len(root) + len(names)*8)
	builder.WriteString(root)
	for _, name := range names {
		builder.WriteString("['")
		builder.WriteString(strings.ReplaceAll(name, "'", "''"))
		builder.WriteString("']")
	}
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
	if err := builder.addAttributes(opt.Attributes); err != nil {
		return "", nil, err
	}
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
	if err := builder.addAttributes(opt.Attributes); err != nil {
		return "", nil, err
	}
	if err := builder.addStateMeta(opt.StateMeta); err != nil {
		return "", nil, err
	}
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
		builder.predicates = append(builder.predicates, "state IN (@terminal_0, @terminal_1, @terminal_2)")
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
	if err := builder.addAttributes(opt.Attributes); err != nil {
		return "", nil, err
	}
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
	builder.predicates = append(builder.predicates, "lease_deadline_ms BETWEEN @lease_from_ms AND @lease_to_ms")
	builder.params["lease_from_ms"] = int64(0)
	builder.params["lease_to_ms"] = cutoff
	return builder.build()
}
