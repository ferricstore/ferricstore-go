package ferricstore

import (
	"errors"
	"fmt"
)

func decodeFlowQueryIndexCoverage(value any) (FlowQueryIndexCoverage, error) {
	mapping, err := nativeMap(value)
	if err != nil {
		return FlowQueryIndexCoverage{}, fmt.Errorf("decode FLOW.QUERY.INDEXES index coverage: %w", err)
	}
	complete, err := unsignedResponseInteger(mapping["complete_shards"], "FLOW.QUERY.INDEXES index coverage complete_shards")
	if err != nil {
		return FlowQueryIndexCoverage{}, err
	}
	total, err := positiveFlowQueryUnsigned(mapping["total_shards"], "FLOW.QUERY.INDEXES index coverage total_shards")
	if err != nil || complete > total {
		return FlowQueryIndexCoverage{}, errors.New("decode FLOW.QUERY.INDEXES index coverage: invalid shard counts")
	}
	validation, err := requiredFlowQueryChoice(mapping, "validation", "FLOW.QUERY.INDEXES index coverage", "pending", "passed", "failed")
	if err != nil {
		return FlowQueryIndexCoverage{}, err
	}
	return FlowQueryIndexCoverage{CompleteShards: complete, TotalShards: total, Validation: validation, Raw: mapping}, nil
}

type decodedFlowQueryProgress struct {
	Scope           string
	PhaseCounts     map[string]uint64
	CurrentPhases   []string
	CompletedShards uint64
	TotalShards     uint64
	Raw             map[string]any
}

func decodeFlowQueryProgress(value any, section string, allowedPhases []string) (decodedFlowQueryProgress, error) {
	mapping, err := nativeMap(value)
	if err != nil {
		return decodedFlowQueryProgress{}, fmt.Errorf("decode FLOW.QUERY.INDEXES index %s: %w", section, err)
	}
	scope, err := requiredFlowQueryChoice(mapping, "scope", "FLOW.QUERY.INDEXES index "+section, "catalog_build")
	if err != nil {
		return decodedFlowQueryProgress{}, err
	}
	phaseCounts, err := decodeFlowQueryPhaseCounts(mapping["phase_counts"], section)
	if err != nil {
		return decodedFlowQueryProgress{}, err
	}
	currentPhases, err := decodeFlowQueryPhases(mapping["current_phases"], section, allowedPhases)
	if err != nil {
		return decodedFlowQueryProgress{}, err
	}
	completed, err := unsignedResponseInteger(mapping["completed_shards"], "FLOW.QUERY.INDEXES index "+section+" completed_shards")
	if err != nil {
		return decodedFlowQueryProgress{}, err
	}
	total, err := positiveFlowQueryUnsigned(mapping["total_shards"], "FLOW.QUERY.INDEXES index "+section+" total_shards")
	if err != nil || completed > total {
		return decodedFlowQueryProgress{}, fmt.Errorf("decode FLOW.QUERY.INDEXES index %s: invalid shard counts", section)
	}
	return decodedFlowQueryProgress{
		Scope: scope, PhaseCounts: phaseCounts, CurrentPhases: currentPhases,
		CompletedShards: completed, TotalShards: total, Raw: mapping,
	}, nil
}

func decodeFlowQueryIndexBuild(value any) (FlowQueryIndexBuild, error) {
	progress, err := decodeFlowQueryProgress(value, "build", flowQueryBuildPhases)
	if err != nil {
		return FlowQueryIndexBuild{}, err
	}
	scanned, err := flowQueryIndexCounter(progress.Raw, "scanned_records", "build")
	if err != nil {
		return FlowQueryIndexBuild{}, err
	}
	writtenEntries, err := flowQueryIndexCounter(progress.Raw, "written_entries", "build")
	if err != nil {
		return FlowQueryIndexBuild{}, err
	}
	writtenBytes, err := flowQueryIndexCounter(progress.Raw, "written_bytes", "build")
	if err != nil {
		return FlowQueryIndexBuild{}, err
	}
	return FlowQueryIndexBuild{
		Scope: progress.Scope, PhaseCounts: progress.PhaseCounts, CurrentPhases: progress.CurrentPhases,
		CompletedShards: progress.CompletedShards, TotalShards: progress.TotalShards,
		ScannedRecords: scanned, WrittenEntries: writtenEntries, WrittenBytes: writtenBytes, Raw: progress.Raw,
	}, nil
}

func decodeFlowQueryIndexValidation(value any) (FlowQueryIndexValidation, error) {
	progress, err := decodeFlowQueryProgress(value, "validation", flowQueryValidationPhases)
	if err != nil {
		return FlowQueryIndexValidation{}, err
	}
	status, err := requiredFlowQueryChoice(progress.Raw, "status", "FLOW.QUERY.INDEXES index validation", "pending", "passed", "failed")
	if err != nil {
		return FlowQueryIndexValidation{}, err
	}
	checkedRecords, err := flowQueryIndexCounter(progress.Raw, "checked_records", "validation")
	if err != nil {
		return FlowQueryIndexValidation{}, err
	}
	checkedEntries, err := flowQueryIndexCounter(progress.Raw, "checked_entries", "validation")
	if err != nil {
		return FlowQueryIndexValidation{}, err
	}
	mismatches, err := flowQueryIndexCounter(progress.Raw, "mismatches", "validation")
	if err != nil {
		return FlowQueryIndexValidation{}, err
	}
	failureReason, err := requiredNullableFlowQueryText(progress.Raw, "failure_reason", "FLOW.QUERY.INDEXES index validation", 128)
	if err != nil {
		return FlowQueryIndexValidation{}, err
	}
	validatedAt, err := requiredNullableFlowQueryUnsigned(progress.Raw, "validated_at_ms", "FLOW.QUERY.INDEXES index validation")
	if err != nil {
		return FlowQueryIndexValidation{}, err
	}
	return FlowQueryIndexValidation{
		Scope: progress.Scope, Status: status, PhaseCounts: progress.PhaseCounts,
		CurrentPhases: progress.CurrentPhases, CompletedShards: progress.CompletedShards,
		TotalShards: progress.TotalShards, CheckedRecords: checkedRecords, CheckedEntries: checkedEntries,
		Mismatches: mismatches, FailureReason: failureReason, ValidatedAtMS: validatedAt, Raw: progress.Raw,
	}, nil
}

func decodeFlowQueryIndexRetirement(value any) (FlowQueryIndexRetirement, error) {
	mapping, err := nativeMap(value)
	if err != nil {
		return FlowQueryIndexRetirement{}, fmt.Errorf("decode FLOW.QUERY.INDEXES index retirement: %w", err)
	}
	status, err := requiredFlowQueryChoice(mapping, "status", "FLOW.QUERY.INDEXES index retirement", "not_applicable", "pending", "complete")
	if err != nil {
		return FlowQueryIndexRetirement{}, err
	}
	if status == "not_applicable" {
		return FlowQueryIndexRetirement{Status: status, Raw: mapping}, nil
	}
	phaseCounts, err := decodeFlowQueryPhaseCounts(mapping["phase_counts"], "retirement")
	if err != nil {
		return FlowQueryIndexRetirement{}, err
	}
	currentPhases, err := decodeFlowQueryPhases(mapping["current_phases"], "retirement", flowQueryRetirementPhases)
	if err != nil {
		return FlowQueryIndexRetirement{}, err
	}
	completed, err := unsignedResponseInteger(mapping["completed_shards"], "FLOW.QUERY.INDEXES index retirement completed_shards")
	if err != nil {
		return FlowQueryIndexRetirement{}, err
	}
	total, err := positiveFlowQueryUnsigned(mapping["total_shards"], "FLOW.QUERY.INDEXES index retirement total_shards")
	if err != nil || completed > total {
		return FlowQueryIndexRetirement{}, errors.New("decode FLOW.QUERY.INDEXES index retirement: invalid shard counts")
	}
	deletedEntries, err := flowQueryIndexCounter(mapping, "deleted_entries", "retirement")
	if err != nil {
		return FlowQueryIndexRetirement{}, err
	}
	deletedBytes, err := flowQueryIndexCounter(mapping, "deleted_bytes", "retirement")
	if err != nil {
		return FlowQueryIndexRetirement{}, err
	}
	rewritten, err := flowQueryIndexCounter(mapping, "rewritten_reverse_rows", "retirement")
	if err != nil {
		return FlowQueryIndexRetirement{}, err
	}
	return FlowQueryIndexRetirement{
		Status: status, PhaseCounts: phaseCounts, CurrentPhases: currentPhases,
		CompletedShards: uint64Pointer(completed), TotalShards: uint64Pointer(total),
		DeletedEntries: uint64Pointer(deletedEntries), DeletedBytes: uint64Pointer(deletedBytes),
		RewrittenReverseRows: uint64Pointer(rewritten), Raw: mapping,
	}, nil
}

func decodeFlowQueryIndexStatistics(value any) (FlowQueryIndexStatistics, error) {
	mapping, err := nativeMap(value)
	if err != nil {
		return FlowQueryIndexStatistics{}, fmt.Errorf("decode FLOW.QUERY.INDEXES index statistics: %w", err)
	}
	status, err := requiredFlowQueryChoice(mapping, "status", "FLOW.QUERY.INDEXES index statistics", "fresh", "stale", "future", "mixed", "missing", "unavailable")
	if err != nil {
		return FlowQueryIndexStatistics{}, err
	}
	samples, err := flowQueryIndexCounter(mapping, "samples", "statistics")
	if err != nil {
		return FlowQueryIndexStatistics{}, err
	}
	fresh, err := flowQueryIndexCounter(mapping, "fresh_samples", "statistics")
	if err != nil {
		return FlowQueryIndexStatistics{}, err
	}
	stale, err := flowQueryIndexCounter(mapping, "stale_samples", "statistics")
	if err != nil {
		return FlowQueryIndexStatistics{}, err
	}
	future, err := flowQueryIndexCounter(mapping, "future_samples", "statistics")
	if err != nil || fresh > samples || stale != samples-fresh || future > stale {
		return FlowQueryIndexStatistics{}, errors.New("decode FLOW.QUERY.INDEXES index statistics: inconsistent counters")
	}
	nullable := make([]*uint64, 4)
	for position, field := range []string{"oldest_collected_at_ms", "newest_collected_at_ms", "oldest_age_ms", "newest_age_ms"} {
		nullable[position], err = requiredNullableFlowQueryUnsigned(mapping, field, "FLOW.QUERY.INDEXES index statistics")
		if err != nil {
			return FlowQueryIndexStatistics{}, err
		}
	}
	return FlowQueryIndexStatistics{
		Status: status, Samples: samples, FreshSamples: fresh, StaleSamples: stale, FutureSamples: future,
		OldestCollectedAtMS: nullable[0], NewestCollectedAtMS: nullable[1],
		OldestAgeMS: nullable[2], NewestAgeMS: nullable[3], Raw: mapping,
	}, nil
}

func decodeFlowQueryPhaseCounts(value any, section string) (map[string]uint64, error) {
	mapping, err := nativeMap(value)
	if err != nil || len(mapping) > 16 {
		return nil, fmt.Errorf("decode FLOW.QUERY.INDEXES index %s phase_counts: invalid map", section)
	}
	counts := make(map[string]uint64, len(mapping))
	for phase, rawCount := range mapping {
		if phase == "" || len(phase) > 64 {
			return nil, fmt.Errorf("decode FLOW.QUERY.INDEXES index %s phase_counts: invalid phase", section)
		}
		count, err := unsignedResponseInteger(rawCount, "FLOW.QUERY.INDEXES index "+section+" phase_count")
		if err != nil {
			return nil, err
		}
		counts[phase] = count
	}
	return counts, nil
}

func decodeFlowQueryPhases(value any, section string, allowed []string) ([]string, error) {
	phases, err := decodeUniqueFlowQueryTextArray(value, "FLOW.QUERY.INDEXES index "+section+" current_phases", len(allowed), 64)
	if err != nil {
		return nil, err
	}
	for _, phase := range phases {
		if !containsFlowQueryString(allowed, phase) {
			return nil, fmt.Errorf("decode FLOW.QUERY.INDEXES index %s current_phases: invalid phase", section)
		}
	}
	return phases, nil
}
