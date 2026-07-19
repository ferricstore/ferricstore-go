package ferricstore

import "fmt"

func effectResult(value any, err error) (EffectResult, error) {
	if err != nil {
		return EffectResult{}, err
	}
	m, err := nativeMap(value)
	if err != nil {
		return EffectResult{}, err
	}
	fields := make([]string, 16)
	for index, field := range []string{
		"id", "flow_id", "partition_key", "type", "state", "effect_key", "effect_type", "status",
		"decision", "scope", "external_id", "error", "reason", "operation_digest", "idempotency_key", "policy_hash",
	} {
		fields[index], err = adminString(m, field)
		if err != nil {
			return EffectResult{}, err
		}
	}
	policyVersion, err := governancePolicyVersion(m)
	if err != nil {
		return EffectResult{}, err
	}
	times := make([]int64, 7)
	timeFields := []string{
		"latency_ms", "created_at_ms", "updated_at_ms", "reserved_at_ms",
		"confirmed_at_ms", "failed_at_ms", "compensated_at_ms",
	}
	for index, field := range timeFields {
		times[index], err = adminNonNegativeInt64(m, field)
		if err != nil {
			return EffectResult{}, err
		}
	}
	if _, present := m["reserved_at_ms"]; !present {
		times[3] = times[1]
	}
	if _, present := m["confirmed_at_ms"]; !present && fields[7] == "confirmed" {
		times[4] = times[2]
	}
	if _, present := m["failed_at_ms"]; !present && fields[7] == "failed" {
		times[5] = times[2]
	}
	if _, present := m["compensated_at_ms"]; !present && fields[7] == "compensated" {
		times[6] = times[2]
	}
	return EffectResult{
		ID: fields[0], FlowID: fields[1], PartitionKey: fields[2], FlowType: fields[3], State: fields[4],
		EffectKey: fields[5], EffectType: fields[6], Status: fields[7], Decision: fields[8], Scope: fields[9],
		ExternalID: fields[10], Error: fields[11], Reason: fields[12], OperationDigest: fields[13],
		IdempotencyKey: fields[14], PolicyHash: fields[15], PolicyVersion: policyVersion,
		LatencyMS: times[0], CreatedAtMS: times[1], UpdatedAtMS: times[2], ReservedAtMS: times[3],
		ConfirmedAtMS: times[4], FailedAtMS: times[5], CompensatedAtMS: times[6], Raw: m,
	}, nil
}

func approvalResult(value any, err error) (ApprovalResult, error) {
	if err != nil {
		return ApprovalResult{}, err
	}
	m, err := nativeMap(value)
	if err != nil {
		return ApprovalResult{}, err
	}
	return approvalResultFromMap(m)
}

func approvalResultFromMap(m map[string]any) (ApprovalResult, error) {
	fields := make([]string, 8)
	for index, field := range []string{"id", "flow_id", "scope", "status", "reason", "requested_by", "decision_reason", "policy_hash"} {
		var err error
		fields[index], err = adminString(m, field)
		if err != nil {
			return ApprovalResult{}, err
		}
	}
	approver, err := governanceStringAlias(m, "decided_by", "approver")
	if err != nil {
		return ApprovalResult{}, err
	}
	policyVersion, err := governancePolicyVersion(m)
	if err != nil {
		return ApprovalResult{}, err
	}
	assignees, err := adminStringList(m["assignees"], "approval assignees")
	if err != nil {
		return ApprovalResult{}, err
	}
	times := make([]int64, 3)
	for index, field := range []string{"requested_at_ms", "decided_at_ms", "expires_at_ms"} {
		times[index], err = adminNonNegativeInt64(m, field)
		if err != nil {
			return ApprovalResult{}, err
		}
	}
	return ApprovalResult{
		ID: fields[0], FlowID: fields[1], Scope: fields[2], Status: fields[3], Reason: fields[4],
		RequestedBy: fields[5], Approver: approver, DecisionReason: fields[6], Assignees: assignees,
		PolicyHash: fields[7], PolicyVersion: policyVersion, RequestedAtMS: times[0], DecidedAtMS: times[1],
		ExpiresAtMS: times[2], Raw: m,
	}, nil
}

func governanceOverviewFromMap(m map[string]any) (GovernanceOverview, error) {
	counts, err := optionalNativeMap(m["counts"], "governance counts")
	if err != nil {
		return GovernanceOverview{}, err
	}
	overview := GovernanceOverview{Counts: counts, Limits: []LimitResult{}, Raw: m}
	collections := []struct {
		name  string
		parse func(map[string]any) error
	}{
		{name: "approvals", parse: func(item map[string]any) error {
			result, err := approvalResultFromMap(item)
			overview.Approvals = append(overview.Approvals, result)
			return err
		}},
		{name: "budgets", parse: func(item map[string]any) error {
			result, err := budgetResultFromMap(item)
			overview.Budgets = append(overview.Budgets, result)
			return err
		}},
		{name: "limits", parse: func(item map[string]any) error {
			result, err := limitResultFromMap(item)
			overview.Limits = append(overview.Limits, result)
			return err
		}},
		{name: "circuits", parse: func(item map[string]any) error {
			result, err := circuitResultFromMap(item)
			overview.Circuits = append(overview.Circuits, result)
			return err
		}},
		{name: "effects", parse: func(item map[string]any) error {
			result, err := effectResult(item, nil)
			overview.Effects = append(overview.Effects, result)
			return err
		}},
	}
	for _, collection := range collections {
		items, err := mapList(m[collection.name], nil)
		if err != nil {
			return GovernanceOverview{}, fmt.Errorf("decode governance %s: %w", collection.name, err)
		}
		for _, item := range items {
			if err := collection.parse(item); err != nil {
				return GovernanceOverview{}, fmt.Errorf("decode governance %s: %w", collection.name, err)
			}
		}
	}
	if err := validateGovernanceOverviewCounts(overview); err != nil {
		return GovernanceOverview{}, err
	}
	return overview, nil
}

func validateGovernanceOverviewCounts(overview GovernanceOverview) error {
	pendingApprovals := 0
	for _, approval := range overview.Approvals {
		if approval.Status == "pending" {
			pendingApprovals++
		}
	}
	openCircuits, halfOpenCircuits := 0, 0
	for _, circuit := range overview.Circuits {
		switch circuit.Status {
		case "open":
			openCircuits++
		case "half_open":
			halfOpenCircuits++
		}
	}
	expected := []struct {
		field string
		count int
	}{
		{field: "approvals", count: len(overview.Approvals)},
		{field: "pending_approvals", count: pendingApprovals},
		{field: "budgets", count: len(overview.Budgets)},
		{field: "limits", count: len(overview.Limits)},
		{field: "circuits", count: len(overview.Circuits)},
		{field: "open_circuits", count: openCircuits},
		{field: "half_open_circuits", count: halfOpenCircuits},
	}
	for _, item := range expected {
		raw, present := overview.Counts[item.field]
		if !present || raw == nil {
			continue
		}
		actual, err := responseInt64(raw, nil)
		if err != nil || actual < 0 || actual > maxFlowExactIntegerV080 {
			return fmt.Errorf("governance %s count is invalid", item.field)
		}
		if actual != int64(item.count) {
			return fmt.Errorf(
				"governance %s count %d does not match %d records",
				item.field, actual, item.count,
			)
		}
	}
	return nil
}

func circuitResult(value any, err error) (CircuitBreakerStatus, error) {
	if err != nil {
		return CircuitBreakerStatus{}, err
	}
	m, err := nativeMap(value)
	if err != nil {
		return CircuitBreakerStatus{}, err
	}
	return circuitResultFromMap(m)
}

func circuitResultFromMap(m map[string]any) (CircuitBreakerStatus, error) {
	scope, err := adminString(m, "scope")
	if err != nil {
		return CircuitBreakerStatus{}, err
	}
	status, err := adminString(m, "status")
	if err != nil {
		return CircuitBreakerStatus{}, err
	}
	failures, err := governanceIntAlias(m, "failures", "failure_count")
	if err != nil {
		return CircuitBreakerStatus{}, err
	}
	values := make([]int64, 17)
	for index, field := range []string{
		"failure_threshold", "open_ms", "opened_at_ms", "window_ms", "min_calls", "failure_rate_pct",
		"latency_threshold_ms", "half_open_max_probes", "half_open_success_threshold", "half_open_in_flight",
		"half_open_successes", "half_open_started_at_ms", "last_failure_ms", "last_success_ms", "updated_at_ms",
		"event_count", "retry_after_ms",
	} {
		values[index], err = adminNonNegativeInt64(m, field)
		if err != nil {
			return CircuitBreakerStatus{}, err
		}
	}
	if values[5] > 100 {
		return CircuitBreakerStatus{}, fmt.Errorf("decode admin field failure_rate_pct: must not exceed 100")
	}
	errorClasses, err := adminStringList(m["error_classes"], "circuit error_classes")
	if err != nil {
		return CircuitBreakerStatus{}, err
	}
	events, err := mapList(m["events"], nil)
	if err != nil {
		return CircuitBreakerStatus{}, fmt.Errorf("decode circuit events: %w", err)
	}
	if _, present := m["events"]; present {
		if rawCount, countPresent := m["event_count"]; countPresent && rawCount != nil && values[15] != int64(len(events)) {
			return CircuitBreakerStatus{}, fmt.Errorf("circuit event_count %d does not match %d events", values[15], len(events))
		}
		if _, countPresent := m["event_count"]; !countPresent {
			values[15] = int64(len(events))
		}
	}
	return CircuitBreakerStatus{
		Scope: scope, Status: status, FailureThreshold: values[0], OpenMS: values[1], OpenedAtMS: values[2],
		Failures: failures, FailureCount: failures, WindowMS: values[3], MinCalls: values[4], FailureRatePct: values[5],
		LatencyThresholdMS: values[6], ErrorClasses: errorClasses, HalfOpenMaxProbes: values[7],
		HalfOpenSuccessThreshold: values[8], HalfOpenInFlight: values[9], HalfOpenSuccesses: values[10],
		HalfOpenStartedAtMS: values[11], LastFailureMS: values[12], LastSuccessMS: values[13], UpdatedAtMS: values[14],
		Events: events, EventCount: values[15], RetryAfterMS: values[16], Raw: m,
	}, nil
}

func adminNonNegativeInt64(m map[string]any, field string) (int64, error) {
	value, err := adminInt64(m, field)
	if err != nil {
		return 0, err
	}
	if value < 0 {
		return 0, fmt.Errorf("decode admin field %s: must be non-negative", field)
	}
	if value > maxFlowExactIntegerV080 {
		return 0, fmt.Errorf(
			"decode admin field %s: exceeds FerricStore 0.8 exact integer maximum %d",
			field,
			maxFlowExactIntegerV080,
		)
	}
	return value, nil
}

func governancePolicyVersion(m map[string]any) (string, error) {
	raw, present := m["policy_version"]
	if !present || raw == nil {
		return "", nil
	}
	if value, err := responseString(raw, nil); err == nil {
		if value == "" || len(value) > maxGovernanceFieldBytesV080 {
			return "", fmt.Errorf(
				"decode admin field policy_version: must be a non-empty string of at most %d bytes",
				maxGovernanceFieldBytesV080,
			)
		}
		return value, nil
	}
	value, err := responseInt64(raw, nil)
	if err != nil || value < 0 || value > maxFlowExactIntegerV080 {
		return "", fmt.Errorf(
			"decode admin field policy_version: expected a non-empty string or exact non-negative integer",
		)
	}
	return fmt.Sprintf("%d", value), nil
}

func governanceStringAlias(m map[string]any, canonical, legacy string) (string, error) {
	canonicalValue, err := adminString(m, canonical)
	if err != nil {
		return "", err
	}
	legacyValue, err := adminString(m, legacy)
	if err != nil {
		return "", err
	}
	if canonicalValue != "" && legacyValue != "" && canonicalValue != legacyValue {
		return "", fmt.Errorf("governance response has conflicting %s and %s", canonical, legacy)
	}
	if canonicalValue != "" {
		return canonicalValue, nil
	}
	return legacyValue, nil
}

func governanceIntAlias(m map[string]any, canonical, legacy string) (int64, error) {
	canonicalValue, canonicalPresent, err := presentGovernanceInt(m, canonical)
	if err != nil {
		return 0, err
	}
	legacyValue, legacyPresent, err := presentGovernanceInt(m, legacy)
	if err != nil {
		return 0, err
	}
	if canonicalPresent && legacyPresent && canonicalValue != legacyValue {
		return 0, fmt.Errorf("governance response has conflicting %s and %s", canonical, legacy)
	}
	if canonicalPresent {
		return canonicalValue, nil
	}
	return legacyValue, nil
}

func presentGovernanceInt(m map[string]any, field string) (int64, bool, error) {
	raw, present := m[field]
	if !present || raw == nil {
		return 0, false, nil
	}
	value, err := adminNonNegativeInt64(m, field)
	return value, true, err
}
