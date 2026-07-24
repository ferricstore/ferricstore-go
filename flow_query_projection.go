package ferricstore

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	flowQueryMaxProjectionFields = 32
	flowQueryMaxDynamicNameBytes = 64
)

type FlowProjectionShape string

const (
	FlowProjectionRecord  FlowProjectionShape = "record"
	FlowProjectionRecords FlowProjectionShape = "records"
)

// FlowProjectionField is a validated, source-aware FQL1 return selector.
// Use the FlowRun* and FlowEvent* constants or the dynamic field constructors.
type FlowProjectionField interface {
	flowProjection() (source string, selector string)
}

type FlowRunProjectionField string

const (
	FlowRunID              FlowRunProjectionField = "run_id"
	FlowRunType            FlowRunProjectionField = "type"
	FlowRunState           FlowRunProjectionField = "state"
	FlowRunVersion         FlowRunProjectionField = "version"
	FlowRunPriority        FlowRunProjectionField = "priority"
	FlowRunPartitionKey    FlowRunProjectionField = "partition_key"
	FlowRunCreatedAtMS     FlowRunProjectionField = "created_at_ms"
	FlowRunUpdatedAtMS     FlowRunProjectionField = "updated_at_ms"
	FlowRunNextRunAtMS     FlowRunProjectionField = "next_run_at_ms"
	FlowRunLeaseDeadlineMS FlowRunProjectionField = "lease_deadline_ms"
	FlowRunAttempts        FlowRunProjectionField = "attempts"
	FlowRunStateValue      FlowRunProjectionField = "run_state"
	FlowRunMaxActiveMS     FlowRunProjectionField = "max_active_ms"
	FlowRunParentID        FlowRunProjectionField = "parent_flow_id"
	FlowRunRootID          FlowRunProjectionField = "root_flow_id"
	FlowRunCorrelationID   FlowRunProjectionField = "correlation_id"
	FlowRunAttributes      FlowRunProjectionField = "attributes"
	FlowRunStateMeta       FlowRunProjectionField = "state_meta"
)

func (field FlowRunProjectionField) flowProjection() (string, string) {
	return "runs", string(field)
}

type FlowEventProjectionField string

const (
	FlowEventID     FlowEventProjectionField = "event_id"
	FlowEventFields FlowEventProjectionField = "fields"
)

func (field FlowEventProjectionField) flowProjection() (string, string) {
	return "events", string(field)
}

type flowDynamicProjectionField struct {
	source   string
	selector string
}

func (field flowDynamicProjectionField) flowProjection() (string, string) {
	return field.source, field.selector
}

func FlowAttributeProjection(name string) (FlowProjectionField, error) {
	quoted, err := quoteFlowProjectionName(name, false)
	if err != nil {
		return nil, err
	}
	return flowDynamicProjectionField{source: "runs", selector: "attribute[" + quoted + "]"}, nil
}

func FlowStateMetaProjection(state, name string) (FlowProjectionField, error) {
	quotedState, err := quoteFlowProjectionName(state, true)
	if err != nil {
		return nil, err
	}
	quotedName, err := quoteFlowProjectionName(name, false)
	if err != nil {
		return nil, err
	}
	return flowDynamicProjectionField{
		source: "runs", selector: "state_meta[" + quotedState + "][" + quotedName + "]",
	}, nil
}

func FlowEventFieldProjection(name string) (FlowProjectionField, error) {
	quoted, err := quoteFlowProjectionName(name, false)
	if err != nil {
		return nil, err
	}
	return flowDynamicProjectionField{source: "events", selector: "fields[" + quoted + "]"}, nil
}

// ProjectFlowQuery adds one validated sparse return projection to an FQL1 query
// that does not already contain a RETURN clause.
func ProjectFlowQuery(query string, shape FlowProjectionShape, fields ...FlowProjectionField) (string, error) {
	if err := validateFlowQueryText(query); err != nil {
		return "", err
	}
	if shape != FlowProjectionRecord && shape != FlowProjectionRecords {
		return "", errors.New("Flow query projection shape must be record or records")
	}
	if len(fields) == 0 || len(fields) > flowQueryMaxProjectionFields {
		return "", fmt.Errorf("Flow query projection must contain 1..%d fields", flowQueryMaxProjectionFields)
	}
	source, err := projectedFlowQuerySource(query)
	if err != nil {
		return "", err
	}
	selectors := make([]string, 0, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if field == nil {
			return "", errors.New("Flow query projection accepts only projection fields")
		}
		fieldSource, selector, valid := validatedFlowProjection(field)
		if !valid || fieldSource != source {
			return "", fmt.Errorf("Flow query projection field %q does not belong to %s", selector, source)
		}
		if _, exists := seen[selector]; exists {
			return "", errors.New("Flow query projection contains a duplicate field")
		}
		seen[selector] = struct{}{}
		selectors = append(selectors, selector)
	}
	if containsFlowQueryReturn(query) {
		return "", errors.New("Flow query already contains a RETURN clause")
	}

	base := strings.TrimSpace(query)
	base = strings.TrimSpace(strings.TrimSuffix(base, ";"))
	projected := base + " RETURN " + strings.ToUpper(string(shape)) + " (" + strings.Join(selectors, ", ") + ")"
	if err := validateFlowQueryText(projected); err != nil {
		return "", err
	}
	return projected, nil
}

func projectedFlowQuerySource(query string) (string, error) {
	parts := strings.Fields(query)
	if len(parts) < 2 || !strings.EqualFold(parts[0], "FROM") {
		return "", errors.New("Projected Flow query must start with FROM runs or FROM events")
	}
	source := strings.ToLower(parts[1])
	if source != "runs" && source != "events" {
		return "", errors.New("Projected Flow query must start with FROM runs or FROM events")
	}
	return source, nil
}

func validatedFlowProjection(field FlowProjectionField) (string, string, bool) {
	source, selector := field.flowProjection()
	switch typed := field.(type) {
	case FlowRunProjectionField:
		switch typed {
		case FlowRunID, FlowRunType, FlowRunState, FlowRunVersion, FlowRunPriority,
			FlowRunPartitionKey, FlowRunCreatedAtMS, FlowRunUpdatedAtMS, FlowRunNextRunAtMS,
			FlowRunLeaseDeadlineMS, FlowRunAttempts, FlowRunStateValue, FlowRunMaxActiveMS,
			FlowRunParentID, FlowRunRootID, FlowRunCorrelationID, FlowRunAttributes,
			FlowRunStateMeta:
			return source, selector, true
		}
		return source, selector, false
	case FlowEventProjectionField:
		valid := typed == FlowEventID || typed == FlowEventFields
		return source, selector, valid
	case flowDynamicProjectionField:
		return source, selector, source == "runs" || source == "events"
	default:
		return source, selector, false
	}
}

func containsFlowQueryReturn(query string) bool {
	quoted := false
	for index := 0; index < len(query); {
		if query[index] == '\'' {
			if quoted && index+1 < len(query) && query[index+1] == '\'' {
				index += 2
				continue
			}
			quoted = !quoted
			index++
			continue
		}
		if !quoted && flowQueryWordStart(query[index]) {
			end := index + 1
			for end < len(query) && flowQueryWordContinue(query[end]) {
				end++
			}
			if strings.EqualFold(query[index:end], "RETURN") {
				return true
			}
			index = end
			continue
		}
		index++
	}
	return false
}

func flowQueryWordStart(value byte) bool {
	return value == '_' || value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}

func flowQueryWordContinue(value byte) bool {
	return flowQueryWordStart(value) || value >= '0' && value <= '9'
}

func quoteFlowProjectionName(value string, allowPrivate bool) (string, error) {
	if !utf8.ValidString(value) || value == "" || len(value) > flowQueryMaxDynamicNameBytes ||
		!allowPrivate && strings.HasPrefix(value, "__") {
		return "", fmt.Errorf(
			"Flow query projection metadata names must be 1..%d valid UTF-8 bytes",
			flowQueryMaxDynamicNameBytes,
		)
	}
	return "'" + strings.ReplaceAll(value, "'", "''") + "'", nil
}
