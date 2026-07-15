package ferricstore

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ErrNil reports a valid protocol null where a scalar result was requested.
// Callers can use errors.Is to distinguish absence from a legitimate zero.
var ErrNil = errors.New("ferricstore: nil response")

func responseInt64(value any, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	switch v := value.(type) {
	case nil:
		return 0, ErrNil
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		if uint64(v) <= math.MaxInt64 {
			return int64(v), nil
		}
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v <= math.MaxInt64 {
			return int64(v), nil
		}
	case string:
		n, parseErr := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if parseErr == nil {
			return n, nil
		}
	case []byte:
		return responseInt64(string(v), nil)
	}
	return 0, fmt.Errorf("expected integer response, got %T (%v)", value, value)
}

func boundedCountResponse(command string, maximum int, value any, err error) (int64, error) {
	count, err := responseInt64(value, err)
	if err != nil {
		return 0, err
	}
	if count < 0 || count > int64(maximum) {
		return 0, fmt.Errorf("%s count %d is outside valid range 0..%d", command, count, maximum)
	}
	return count, nil
}

func requiredOneResponse(command string, value any, err error) (int64, error) {
	count, err := responseInt64(value, err)
	if err != nil {
		return 0, err
	}
	if count != 1 {
		return 0, fmt.Errorf("%s expected success count 1, got %d", command, count)
	}
	return count, nil
}

func responseFloat64(value any, err error) (float64, error) {
	return responseFloat64Policy(value, err, false)
}

func responseFloat64NonFinite(value any, err error) (float64, error) {
	return responseFloat64Policy(value, err, true)
}

func responseFloat64Policy(value any, err error, allowNonFinite bool) (float64, error) {
	if err != nil {
		return 0, err
	}
	var result float64
	switch v := value.(type) {
	case nil:
		return 0, ErrNil
	case float64:
		result = v
	case float32:
		result = float64(v)
	case int:
		result = float64(v)
	case int8:
		result = float64(v)
	case int16:
		result = float64(v)
	case int32:
		result = float64(v)
	case int64:
		result = float64(v)
	case uint:
		result = float64(v)
	case uint8:
		result = float64(v)
	case uint16:
		result = float64(v)
	case uint32:
		result = float64(v)
	case uint64:
		result = float64(v)
	case string:
		parsed, parseErr := parseResponseFloat(v)
		if parseErr != nil {
			return 0, fmt.Errorf("expected float response, got %q", v)
		}
		result = parsed
	case []byte:
		return responseFloat64Policy(string(v), nil, allowNonFinite)
	default:
		return 0, fmt.Errorf("expected float response, got %T", value)
	}
	if !allowNonFinite && (math.IsNaN(result) || math.IsInf(result, 0)) {
		return 0, fmt.Errorf("expected finite float response, got %v", result)
	}
	return result, nil
}

func parseResponseFloat(value string) (float64, error) {
	text := strings.TrimSpace(value)
	switch strings.ToLower(text) {
	case "nan", "+nan", "-nan":
		return math.NaN(), nil
	case "inf", "+inf", "infinity", "+infinity":
		return math.Inf(1), nil
	case "-inf", "-infinity":
		return math.Inf(-1), nil
	default:
		return strconv.ParseFloat(text, 64)
	}
}

func responseBool(value any, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	switch v := value.(type) {
	case nil:
		return false, ErrNil
	case bool:
		return v, nil
	case int:
		if v == 0 || v == 1 {
			return v == 1, nil
		}
	case int8:
		return responseBool(int64(v), nil)
	case int16:
		return responseBool(int64(v), nil)
	case int32:
		return responseBool(int64(v), nil)
	case int64:
		if v == 0 || v == 1 {
			return v == 1, nil
		}
	case uint:
		return responseBool(uint64(v), nil)
	case uint8:
		return responseBool(uint64(v), nil)
	case uint16:
		return responseBool(uint64(v), nil)
	case uint32:
		return responseBool(uint64(v), nil)
	case uint64:
		if v == 0 || v == 1 {
			return v == 1, nil
		}
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "1", "true":
			return true, nil
		case "0", "false":
			return false, nil
		}
	case []byte:
		return responseBool(string(v), nil)
	}
	return false, fmt.Errorf("expected boolean response, got %T (%v)", value, value)
}

func responseString(value any, err error) (string, error) {
	if err != nil {
		return "", err
	}
	switch v := value.(type) {
	case nil:
		return "", ErrNil
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return "", fmt.Errorf("expected string response, got %T", value)
	}
}

func responseOptionalBool(value any, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	if value == nil {
		return false, nil
	}
	return responseBool(value, nil)
}

func responseOK(value any, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	if count, ok := value.(nativeCompactOKCount); ok {
		if count == 1 {
			return true, nil
		}
		return false, fmt.Errorf("expected one OK response, got compact OK count %d", count)
	}
	text, err := responseString(value, err)
	if err != nil {
		return false, err
	}
	if !strings.EqualFold(text, "OK") {
		return false, fmt.Errorf("expected OK response, got %q", text)
	}
	return true, nil
}

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

func recordsOrNil(value any, codec Codec) ([]FlowRecord, error) {
	if value == nil || isOK(value) {
		return nil, nil
	}
	return recordsFromNative(value, codec)
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

func claimedItemsFromNative(value any, codec Codec) ([]ClaimedItem, error) {
	if value == nil {
		return nil, nil
	}
	if claimed, ok := value.([]ClaimedItem); ok {
		return claimed, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected native array, got %T", value)
	}
	claimed := make([]ClaimedItem, 0, len(items))
	for _, item := range items {
		parsed, err := claimedItemFromNative(item, codec)
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, parsed)
	}
	return claimed, nil
}

func claimedItemFromNative(value any, codec Codec) (ClaimedItem, error) {
	if list, ok := value.([]any); ok {
		if len(list) < 4 {
			return ClaimedItem{}, fmt.Errorf("expected claimed item with at least 4 fields")
		}
		fencingToken, err := responseInt64(list[3], nil)
		if err != nil {
			return ClaimedItem{}, fmt.Errorf("decode claimed item fencing_token: %w", err)
		}
		item := ClaimedItem{
			ID:           asString(list[0]),
			PartitionKey: asString(list[1]),
			LeaseToken:   asString(list[2]),
			FencingToken: fencingToken,
			State:        "running",
		}
		if len(list) > 4 {
			if attrs, mapErr := nativeMap(list[4]); mapErr == nil {
				item.Attributes = attrs
			} else {
				item.RunState = asString(list[4])
			}
		}
		if len(list) > 5 {
			attrs, err := optionalNativeMap(list[5], "claimed item attributes")
			if err != nil {
				return ClaimedItem{}, err
			}
			item.Attributes = attrs
		}
		return item, nil
	}
	m, err := nativeMap(value)
	if err != nil {
		return ClaimedItem{}, err
	}
	state := asString(m["state"])
	if state == "" {
		state = "running"
	}
	fencingToken, err := responseInt64(m["fencing_token"], nil)
	if err != nil {
		return ClaimedItem{}, fmt.Errorf("decode claimed item fencing_token: %w", err)
	}
	attributes, err := optionalNativeMap(m["attributes"], "claimed item attributes")
	if err != nil {
		return ClaimedItem{}, err
	}
	payload, err := decodeValue(codec, m["payload"])
	if err != nil {
		return ClaimedItem{}, fmt.Errorf("decode claimed item payload: %w", err)
	}
	return ClaimedItem{
		ID:           asString(m["id"]),
		LeaseToken:   asString(m["lease_token"]),
		FencingToken: fencingToken,
		PartitionKey: asString(m["partition_key"]),
		Type:         asString(m["type"]),
		State:        state,
		RunState:     asString(m["run_state"]),
		Payload:      payload,
		Attributes:   attributes,
	}, nil
}

func recordFromMap(m map[string]any, codec Codec) (FlowRecord, error) {
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
	version, err := optionalResponseInt64(m, "version")
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
		ID:               asString(m["id"]),
		Type:             asString(m["type"]),
		State:            asString(m["state"]),
		PartitionKey:     asString(m["partition_key"]),
		Payload:          payload,
		LeaseToken:       asString(m["lease_token"]),
		FencingToken:     fencingToken,
		Version:          version,
		ParentFlowID:     asString(m["parent_flow_id"]),
		RootFlowID:       asString(m["root_flow_id"]),
		CorrelationID:    asString(m["correlation_id"]),
		RunState:         asString(m["run_state"]),
		Attributes:       attributes,
		StateMeta:        stateMeta,
		IndexedStateMeta: asString(m["indexed_state_meta"]),
		Values:           values,
		ValueRefs:        valueRefs,
		ValueSizes:       valueSizes,
		ValueOmitted:     valueOmitted,
		ValueMissing:     valueMissing,
		Raw:              m,
	}, nil
}

func decodeMap(codec Codec, value any) (map[string]any, error) {
	raw, err := optionalNativeMap(value, "flow values")
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	out := make(map[string]any, len(raw))
	for key, item := range raw {
		decoded, err := decodeValue(codec, item)
		if err != nil {
			return nil, fmt.Errorf("decode flow value %q: %w", key, err)
		}
		out[key] = decoded
	}
	return out, nil
}

func optionalResponseInt64(mapping map[string]any, key string) (int64, error) {
	value, found := mapping[key]
	if !found || value == nil {
		return 0, nil
	}
	parsed, err := responseInt64(value, nil)
	if err != nil {
		return 0, fmt.Errorf("decode flow %s: %w", key, err)
	}
	return parsed, nil
}

func optionalNativeMap(value any, field string) (map[string]any, error) {
	if value == nil {
		return map[string]any{}, nil
	}
	mapping, err := nativeMap(value)
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", field, err)
	}
	return mapping, nil
}

func decodeValue(codec Codec, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	return codec.Decode(value)
}
