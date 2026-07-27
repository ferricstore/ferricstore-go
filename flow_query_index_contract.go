package ferricstore

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

var flowQueryIntegerIndexFields = map[string]struct{}{
	"version": {}, "priority": {}, "created_at_ms": {}, "updated_at_ms": {},
	"next_run_at_ms": {}, "lease_deadline_ms": {}, "attempts": {}, "max_active_ms": {},
}

var flowQueryKeywordIndexFields = map[string]struct{}{
	"partition_key": {}, "run_id": {}, "event_id": {}, "type": {}, "state": {},
	"run_state": {}, "parent_flow_id": {}, "root_flow_id": {}, "correlation_id": {},
}

func validateFlowQueryIndexContract(status *FlowQueryIndexStatus, expectedID string) error {
	var previousID string
	var previousVersion uint64
	for position := range status.Indexes {
		index := &status.Indexes[position]
		if position > 0 && (index.ID < previousID || (index.ID == previousID && index.Version <= previousVersion)) {
			return flowQueryIndexContractError("indexes must be uniquely sorted by id and version")
		}
		if expectedID != "" && index.ID != expectedID {
			return flowQueryIndexContractError("filtered indexes do not match the requested id")
		}
		if err := validateFlowQueryIndex(index, status); err != nil {
			return err
		}
		previousID, previousVersion = index.ID, index.Version
	}
	if expectedID != "" && len(status.Indexes) == 0 {
		return flowQueryIndexContractError("filtered indexes do not match the requested id")
	}
	if status.Services.StatisticsStore == "unavailable" {
		for _, index := range status.Indexes {
			if index.Statistics.Status != "unavailable" {
				return flowQueryIndexContractError("statistics must be unavailable when the service is unavailable")
			}
		}
	} else {
		for _, index := range status.Indexes {
			if index.Statistics.Status == "unavailable" {
				return flowQueryIndexContractError("statistics cannot be unavailable while the service is ready")
			}
		}
	}
	return nil
}

func validateFlowQueryIndex(index *FlowQueryIndex, status *FlowQueryIndexStatus) error {
	if !validFlowQueryIndexID(index.ID) {
		return flowQueryIndexContractError("index id contains invalid characters")
	}
	for _, workload := range index.Workloads {
		if !validFlowQueryIndexID(workload) {
			return flowQueryIndexContractError("index workload contains invalid characters")
		}
	}
	first := index.Fields[0]
	if first.Name != "partition_key" || first.Direction != "asc" || first.Encoding != "hashed" {
		return flowQueryIndexContractError("index must begin with partition_key asc hashed")
	}
	attributes := 0
	for _, field := range index.Fields {
		if field.Encoding == "hashed" && field.Direction != "asc" {
			return flowQueryIndexContractError("hashed index fields must be ascending")
		}
		kind := flowQueryIndexFieldKind(field.Name)
		if kind == "" {
			return flowQueryIndexContractError("index contains an unsupported field selector")
		}
		if field.Encoding == "ordered" && kind != "integer" {
			return flowQueryIndexContractError("ordered index fields must be integers")
		}
		if kind == "attribute" {
			attributes++
		}
	}
	if attributes > 1 {
		return flowQueryIndexContractError("index may contain at most one attribute field")
	}
	for _, prefix := range index.CountPrefixes {
		for _, field := range index.Fields[:int(prefix)] {
			if field.Encoding != "hashed" {
				return flowQueryIndexContractError("count prefixes may cover only hashed fields")
			}
		}
	}
	if len(index.CoveringFields) > 0 {
		covered := make(map[string]struct{}, len(index.CoveringFields))
		for _, field := range index.CoveringFields {
			if flowQueryIndexFieldKind(field) == "" {
				return flowQueryIndexContractError("index contains an unsupported covering field selector")
			}
			covered[field] = struct{}{}
		}
		required := []string{"run_id", "version"}
		for _, field := range index.Fields {
			required = append(required, field.Name)
		}
		for _, field := range required {
			if _, present := covered[field]; !present {
				return flowQueryIndexContractError("index covering fields omit an identity or index field")
			}
		}
	}
	if (len(index.CountPrefixes) > 0) != (index.Format.Counter != "") {
		return flowQueryIndexContractError("index counter format is inconsistent with count prefixes")
	}

	if err := validateFlowQueryProgress(index.Build.PhaseCounts, index.Build.CurrentPhases, index.Build.CompletedShards, index.Build.TotalShards, flowQueryBuildPhases, "build"); err != nil {
		return err
	}
	if err := validateFlowQueryProgress(index.Validation.PhaseCounts, index.Validation.CurrentPhases, index.Validation.CompletedShards, index.Validation.TotalShards, flowQueryValidationPhases, "validation"); err != nil {
		return err
	}
	if index.Retirement.Status == "not_applicable" {
		for _, field := range []string{"phase_counts", "current_phases", "completed_shards", "total_shards", "deleted_entries", "deleted_bytes", "rewritten_reverse_rows"} {
			if _, present := index.Retirement.Raw[field]; present {
				return flowQueryIndexContractError("not_applicable retirement must not contain progress")
			}
		}
	} else {
		if index.Retirement.CompletedShards == nil || index.Retirement.TotalShards == nil {
			return flowQueryIndexContractError("retirement progress is incomplete")
		}
		if err := validateFlowQueryProgress(index.Retirement.PhaseCounts, index.Retirement.CurrentPhases, *index.Retirement.CompletedShards, *index.Retirement.TotalShards, flowQueryRetirementPhases, "retirement"); err != nil {
			return err
		}
	}

	if index.Coverage.TotalShards != index.Build.TotalShards ||
		index.Coverage.TotalShards != index.Validation.TotalShards ||
		(index.Retirement.TotalShards != nil && index.Coverage.TotalShards != *index.Retirement.TotalShards) {
		return flowQueryIndexContractError("index shard totals are inconsistent")
	}
	if index.Coverage.CompleteShards != index.Build.CompletedShards {
		return flowQueryIndexContractError("coverage and build completion are inconsistent")
	}
	if index.Coverage.Validation != index.Validation.Status {
		return flowQueryIndexContractError("coverage and validation status are inconsistent")
	}
	queryable := index.State == "active" &&
		index.Coverage.CompleteShards == index.Coverage.TotalShards &&
		index.Coverage.Validation == "passed"
	if index.Queryable != queryable {
		return flowQueryIndexContractError("index queryable flag is inconsistent")
	}
	if err := validateFlowQueryLifecycle(index); err != nil {
		return err
	}
	if err := validateFlowQueryValidation(index); err != nil {
		return err
	}
	return validateFlowQueryStatistics(index, status)
}

func validateFlowQueryProgress(counts map[string]uint64, current []string, completed, total uint64, phases []string, section string) error {
	if len(counts) == 0 {
		return flowQueryIndexContractError(section + " phase_counts are invalid")
	}
	sum := uint64(0)
	for phase, count := range counts {
		if !containsFlowQueryString(phases, phase) || count == 0 || count > total-sum {
			return flowQueryIndexContractError(section + " phase_counts are invalid")
		}
		sum += count
	}
	if sum != total {
		return flowQueryIndexContractError(section + " phase_counts do not match total_shards")
	}
	expected := make([]string, 0, len(counts))
	for _, phase := range phases {
		if _, present := counts[phase]; present {
			expected = append(expected, phase)
		}
	}
	if len(current) != len(expected) {
		return flowQueryIndexContractError(section + " current_phases are inconsistent")
	}
	for position := range current {
		if current[position] != expected[position] {
			return flowQueryIndexContractError(section + " current_phases are inconsistent")
		}
	}
	if completed != counts["done"] {
		return flowQueryIndexContractError(section + " completed_shards is inconsistent")
	}
	return nil
}

func validateFlowQueryLifecycle(index *FlowQueryIndex) error {
	built := index.Build.CompletedShards == index.Build.TotalShards
	validation := index.Validation.Status
	retirement := index.Retirement.Status
	valid := false
	switch index.State {
	case "building":
		valid = !built && validation == "pending" && retirement == "not_applicable"
	case "validating":
		valid = built && (validation == "pending" || validation == "passed") && retirement == "not_applicable"
	case "active":
		valid = built && validation == "passed" && retirement == "not_applicable"
	case "retiring":
		valid = built && validation != "pending" && retirement != "not_applicable"
	case "failed":
		valid = validation != "pending" && retirement != "not_applicable"
	}
	if !valid {
		return flowQueryIndexContractError("index lifecycle fields are inconsistent")
	}
	return nil
}

func validateFlowQueryValidation(index *FlowQueryIndex) error {
	validation := index.Validation
	valid := false
	switch validation.Status {
	case "pending":
		valid = validation.Mismatches == 0 && validation.FailureReason == "" && validation.ValidatedAtMS == nil
	case "passed":
		valid = validation.Mismatches == 0 && validation.FailureReason == "" && validation.ValidatedAtMS != nil
	case "failed":
		valid = validation.Mismatches > 0 && validation.FailureReason != "" && validation.ValidatedAtMS != nil
	}
	if !valid {
		return flowQueryIndexContractError("validation status fields are inconsistent")
	}
	return nil
}

func validateFlowQueryStatistics(index *FlowQueryIndex, status *FlowQueryIndexStatus) error {
	statistics := index.Statistics
	times := []*uint64{statistics.OldestCollectedAtMS, statistics.NewestCollectedAtMS, statistics.OldestAgeMS, statistics.NewestAgeMS}
	if statistics.Samples == 0 {
		if (statistics.Status != "missing" && statistics.Status != "unavailable") || anyFlowQueryIntegerPresent(times) {
			return flowQueryIndexContractError("empty statistics fields are inconsistent")
		}
		return nil
	}
	if !allFlowQueryIntegersPresent(times) {
		return flowQueryIndexContractError("sampled statistics require timestamps and ages")
	}
	oldest, newest := *times[0], *times[1]
	oldestAge, newestAge := *times[2], *times[3]
	if oldest > newest || oldestAge != flowQueryAge(status.ObservedAtMS, oldest) || newestAge != flowQueryAge(status.ObservedAtMS, newest) {
		return flowQueryIndexContractError("statistics timestamps or ages are inconsistent")
	}
	expected := "mixed"
	if statistics.FreshSamples == statistics.Samples {
		expected = "fresh"
	} else if statistics.FreshSamples == 0 {
		expected = "stale"
		if statistics.FutureSamples > 0 && statistics.Status == "future" {
			expected = "future"
		}
	}
	if statistics.Status != expected {
		return flowQueryIndexContractError("statistics status does not match sample counters")
	}
	return nil
}

func flowQueryIndexFieldKind(name string) string {
	if _, present := flowQueryIntegerIndexFields[name]; present {
		return "integer"
	}
	if _, present := flowQueryKeywordIndexFields[name]; present {
		return "keyword"
	}
	parts := strings.Split(name, ".")
	if len(parts) == 2 && parts[0] == "attribute" && validFlowQueryUnquotedMetadata(parts[1]) {
		return "attribute"
	}
	if len(parts) == 3 && parts[0] == "state_meta" && validFlowQueryUnquotedMetadata(parts[1]) && validFlowQueryUnquotedMetadata(parts[2]) {
		return "state_meta"
	}
	if segments, ok := parseFlowQueryBracketSelector(name, "attribute", 1); ok && validFlowQueryMetadata(segments[0], true) && name == flowQueryExternalSelector("attribute", segments...) {
		return "attribute"
	}
	if segments, ok := parseFlowQueryBracketSelector(name, "state_meta", 2); ok && validFlowQueryMetadata(segments[0], false) && validFlowQueryMetadata(segments[1], true) && name == flowQueryExternalSelector("state_meta", segments...) {
		return "state_meta"
	}
	return ""
}

func parseFlowQueryBracketSelector(value, root string, count int) ([]string, bool) {
	rest, ok := strings.CutPrefix(value, root)
	if !ok {
		return nil, false
	}
	segments := make([]string, 0, count)
	for len(rest) > 0 && len(segments) < count {
		if !strings.HasPrefix(rest, "['") {
			return nil, false
		}
		rest = rest[2:]
		var segment strings.Builder
		closed := false
		for index := 0; index < len(rest); {
			if rest[index] != '\'' {
				segment.WriteByte(rest[index])
				index++
				continue
			}
			if index+1 < len(rest) && rest[index+1] == '\'' {
				segment.WriteByte('\'')
				index += 2
				continue
			}
			if index+1 >= len(rest) || rest[index+1] != ']' {
				return nil, false
			}
			segments = append(segments, segment.String())
			rest = rest[index+2:]
			closed = true
			break
		}
		if !closed {
			return nil, false
		}
	}
	return segments, len(rest) == 0 && len(segments) == count
}

func validFlowQueryUnquotedMetadata(value string) bool {
	if value == "" || len(value) > 64 || strings.HasPrefix(value, "__") {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') && character != '_' && character != '-' {
			return false
		}
	}
	return true
}

func validFlowQueryMetadata(value string, rejectReserved bool) bool {
	return value != "" && utf8.ValidString(value) && len(value) <= 64 && (!rejectReserved || !strings.HasPrefix(value, "__"))
}

func flowQueryExternalSelector(root string, segments ...string) string {
	allUnquoted := true
	for _, segment := range segments {
		allUnquoted = allUnquoted && validFlowQueryUnquotedMetadata(segment)
	}
	if allUnquoted {
		return strings.Join(append([]string{root}, segments...), ".")
	}
	var result strings.Builder
	result.WriteString(root)
	for _, segment := range segments {
		result.WriteString("['")
		result.WriteString(strings.ReplaceAll(segment, "'", "''"))
		result.WriteString("']")
	}
	return result.String()
}

func anyFlowQueryIntegerPresent(values []*uint64) bool {
	for _, value := range values {
		if value != nil {
			return true
		}
	}
	return false
}

func allFlowQueryIntegersPresent(values []*uint64) bool {
	for _, value := range values {
		if value == nil {
			return false
		}
	}
	return true
}

func flowQueryAge(observed, collected uint64) uint64 {
	if collected >= observed {
		return 0
	}
	return observed - collected
}

func flowQueryIndexContractError(message string) error {
	return fmt.Errorf("decode FLOW.QUERY.INDEXES: %s", message)
}
