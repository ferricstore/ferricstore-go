package ferricstore

import "fmt"

func recordsFromNative(value any, codec Codec) ([]FlowRecord, error) {
	if value == nil {
		return nil, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected native array, got %T", value)
	}
	records := make([]FlowRecord, 0, len(items))
	for _, item := range items {
		record, err := recordFromNative(item, codec)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func recordsOrNil(value any, codec Codec, expected int) ([]FlowRecord, error) {
	if value == nil {
		return nil, fmt.Errorf("expected %d flow batch results, got nil", expected)
	}
	if count, ok := value.(nativeCompactOKCount); ok {
		if int(count) != expected {
			return nil, fmt.Errorf("flow batch returned %d acknowledgements, expected %d", count, expected)
		}
		return nil, nil
	}
	if items, ok := value.([]any); ok && isOK(items) {
		count := len(items)
		if count != expected {
			return nil, fmt.Errorf("flow batch returned %d acknowledgements, expected %d", count, expected)
		}
		return nil, nil
	}
	// FerricStore uses one scalar OK as the command-level acknowledgement for
	// an atomic same-partition batch. Unlike compact/list acknowledgements, it
	// does not encode a per-item count.
	if isOK(value) {
		return nil, nil
	}
	records, err := recordsFromNative(value, codec)
	if err != nil {
		return nil, err
	}
	if len(records) != expected {
		return nil, fmt.Errorf("flow batch returned %d records, expected %d", len(records), expected)
	}
	return records, nil
}

func recordFromNative(value any, codec Codec) (FlowRecord, error) {
	mapping, err := nativeMap(value)
	if err != nil {
		return FlowRecord{}, err
	}
	return recordFromMap(mapping, codec)
}

func recordOrNil(value any, codec Codec) (*FlowRecord, error) {
	if value == nil || isOK(value) {
		return nil, nil
	}
	record, err := recordFromNative(value, codec)
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func recordFromMap(m map[string]any, codec Codec) (FlowRecord, error) {
	id, err := requiredResponseStringField(m, "id", "flow record")
	if err != nil {
		return FlowRecord{}, err
	}
	flowType, err := optionalResponseStringField(m, "type", "flow record")
	if err != nil {
		return FlowRecord{}, err
	}
	state, err := optionalResponseStringField(m, "state", "flow record")
	if err != nil {
		return FlowRecord{}, err
	}
	partitionKey, err := optionalResponseStringField(m, "partition_key", "flow record")
	if err != nil {
		return FlowRecord{}, err
	}
	leaseToken, err := optionalResponseStringField(m, "lease_token", "flow record")
	if err != nil {
		return FlowRecord{}, err
	}
	parentFlowID, err := optionalResponseStringField(m, "parent_flow_id", "flow record")
	if err != nil {
		return FlowRecord{}, err
	}
	rootFlowID, err := optionalResponseStringField(m, "root_flow_id", "flow record")
	if err != nil {
		return FlowRecord{}, err
	}
	correlationID, err := optionalResponseStringField(m, "correlation_id", "flow record")
	if err != nil {
		return FlowRecord{}, err
	}
	runState, err := optionalResponseStringField(m, "run_state", "flow record")
	if err != nil {
		return FlowRecord{}, err
	}
	indexedStateMeta, err := optionalResponseStringField(m, "indexed_state_meta", "flow record")
	if err != nil {
		return FlowRecord{}, err
	}
	payload, err := decodeValue(codec, m["payload"])
	if err != nil {
		return FlowRecord{}, fmt.Errorf("decode flow payload: %w", err)
	}
	values, err := decodeMap(codec, m["values"])
	if err != nil {
		return FlowRecord{}, err
	}
	fencingToken, err := optionalResponseInt64(m, "fencing_token")
	if err != nil {
		return FlowRecord{}, err
	}
	if fencingToken < 0 || fencingToken > maxFlowExactIntegerV080 {
		return FlowRecord{}, fmt.Errorf(
			"decode flow fencing_token: value must be between 0 and %d",
			maxFlowExactIntegerV080,
		)
	}
	version, err := optionalResponseInt64(m, "version")
	if err != nil {
		return FlowRecord{}, err
	}
	if version < 0 || version > maxFlowExactIntegerV080 {
		return FlowRecord{}, fmt.Errorf(
			"decode flow version: value must be between 0 and %d",
			maxFlowExactIntegerV080,
		)
	}
	maxActiveMS, err := optionalResponseInt64(m, "max_active_ms")
	if err != nil {
		return FlowRecord{}, err
	}
	if value, present := m["max_active_ms"]; present && value != nil &&
		(maxActiveMS <= 0 || maxActiveMS > maxFlowActiveMS) {
		return FlowRecord{}, fmt.Errorf(
			"decode flow max_active_ms: value must be between 1 and %d",
			maxFlowActiveMS,
		)
	}
	flowError, err := decodeFlowRecordError(codec, m["error"])
	if err != nil {
		return FlowRecord{}, fmt.Errorf("decode flow error: %w", err)
	}
	failureReason, err := flowFailureReason(flowError)
	if err != nil {
		return FlowRecord{}, err
	}
	attributes, err := optionalNativeMap(m["attributes"], "flow attributes")
	if err != nil {
		return FlowRecord{}, err
	}
	stateMeta, err := optionalNativeMap(m["state_meta"], "flow state_meta")
	if err != nil {
		return FlowRecord{}, err
	}
	valueRefs, err := optionalNativeMap(m["value_refs"], "flow value_refs")
	if err != nil {
		return FlowRecord{}, err
	}
	valueSizes, err := optionalNativeMap(m["value_sizes"], "flow value_sizes")
	if err != nil {
		return FlowRecord{}, err
	}
	valueOmitted, err := optionalNativeMap(m["value_omitted"], "flow value_omitted")
	if err != nil {
		return FlowRecord{}, err
	}
	valueMissing, err := optionalNativeMap(m["value_missing"], "flow value_missing")
	if err != nil {
		return FlowRecord{}, err
	}
	return FlowRecord{
		ID:               id,
		Type:             flowType,
		State:            state,
		PartitionKey:     partitionKey,
		Payload:          payload,
		Error:            flowError,
		FailureReason:    failureReason,
		MaxActiveMS:      maxActiveMS,
		LeaseToken:       leaseToken,
		FencingToken:     fencingToken,
		Version:          version,
		ParentFlowID:     parentFlowID,
		RootFlowID:       rootFlowID,
		CorrelationID:    correlationID,
		RunState:         runState,
		Attributes:       attributes,
		StateMeta:        stateMeta,
		IndexedStateMeta: indexedStateMeta,
		Values:           values,
		ValueRefs:        valueRefs,
		ValueSizes:       valueSizes,
		ValueOmitted:     valueOmitted,
		ValueMissing:     valueMissing,
		Raw:              m,
	}, nil
}

func decodeFlowRecordError(codec Codec, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if mapping, err := nativeMap(value); err == nil {
		return mapping, nil
	}
	return decodeValue(codec, value)
}

func flowFailureReason(value any) (string, error) {
	mapping, err := nativeMap(value)
	if err != nil {
		return "", nil
	}
	return optionalResponseStringValue(mapping["reason"], "flow failure reason")
}

func optionalResponseStringField(mapping map[string]any, key, context string) (string, error) {
	value, found := mapping[key]
	if !found || value == nil {
		return "", nil
	}
	parsed, err := responseString(value, nil)
	if err != nil {
		return "", fmt.Errorf("decode %s %s: %w", context, key, err)
	}
	if parsed == "" {
		return "", fmt.Errorf("decode %s %s: field must be a non-empty string", context, key)
	}
	return parsed, nil
}

func requiredResponseStringField(mapping map[string]any, key, context string) (string, error) {
	value, found := mapping[key]
	if !found || value == nil {
		return "", fmt.Errorf("decode %s %s: missing string field", context, key)
	}
	parsed, err := responseString(value, nil)
	if err != nil {
		return "", fmt.Errorf("decode %s %s: %w", context, key, err)
	}
	if parsed == "" {
		return "", fmt.Errorf("decode %s %s: field must be a non-empty string", context, key)
	}
	return parsed, nil
}

func optionalResponseStringValue(value any, context string) (string, error) {
	if value == nil {
		return "", nil
	}
	parsed, err := responseString(value, nil)
	if err != nil {
		return "", fmt.Errorf("decode %s: %w", context, err)
	}
	return parsed, nil
}
