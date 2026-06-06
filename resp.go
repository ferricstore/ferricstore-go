package ferricstore

import (
	"fmt"
	"strconv"
	"strings"
)

func recordsFromRESP(value any, codec Codec) ([]FlowRecord, error) {
	if value == nil {
		return nil, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected RESP array, got %T", value)
	}
	records := make([]FlowRecord, 0, len(items))
	for _, item := range items {
		record, err := recordFromRESP(item, codec)
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
	return recordsFromRESP(value, codec)
}

func recordFromRESP(value any, codec Codec) (FlowRecord, error) {
	mapping, err := respMap(value)
	if err != nil {
		return FlowRecord{}, err
	}
	return recordFromMap(mapping, codec), nil
}

func recordOrNil(value any, codec Codec) (*FlowRecord, error) {
	if value == nil || isOK(value) {
		return nil, nil
	}
	record, err := recordFromRESP(value, codec)
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func claimedItemsFromRESP(value any) ([]ClaimedItem, error) {
	if value == nil {
		return nil, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected RESP array, got %T", value)
	}
	claimed := make([]ClaimedItem, 0, len(items))
	for _, item := range items {
		parsed, err := claimedItemFromRESP(item)
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, parsed)
	}
	return claimed, nil
}

func claimedItemFromRESP(value any) (ClaimedItem, error) {
	if list, ok := value.([]any); ok {
		if len(list) < 4 {
			return ClaimedItem{}, fmt.Errorf("expected claimed item with at least 4 fields")
		}
		item := ClaimedItem{
			ID:           asString(list[0]),
			PartitionKey: asString(list[1]),
			LeaseToken:   asString(list[2]),
			FencingToken: asInt64(list[3]),
			State:        "running",
		}
		if len(list) > 4 {
			item.RunState = asString(list[4])
		}
		return item, nil
	}
	m, err := respMap(value)
	if err != nil {
		return ClaimedItem{}, err
	}
	state := asString(m["state"])
	if state == "" {
		state = "running"
	}
	return ClaimedItem{
		ID:           asString(m["id"]),
		LeaseToken:   asString(m["lease_token"]),
		FencingToken: asInt64(m["fencing_token"]),
		PartitionKey: asString(m["partition_key"]),
		Type:         asString(m["type"]),
		State:        state,
		RunState:     asString(m["run_state"]),
		Payload:      m["payload"],
	}, nil
}

func recordFromMap(m map[string]any, codec Codec) FlowRecord {
	payload, _ := decodeValue(codec, m["payload"])
	values := decodeMap(codec, m["values"])
	return FlowRecord{
		ID:            asString(m["id"]),
		Type:          asString(m["type"]),
		State:         asString(m["state"]),
		PartitionKey:  asString(m["partition_key"]),
		Payload:       payload,
		LeaseToken:    asString(m["lease_token"]),
		FencingToken:  asInt64(m["fencing_token"]),
		Version:       asInt64(m["version"]),
		ParentFlowID:  asString(m["parent_flow_id"]),
		RootFlowID:    asString(m["root_flow_id"]),
		CorrelationID: asString(m["correlation_id"]),
		Values:        values,
		ValueRefs:     stringObjectMap(m["value_refs"]),
		Raw:           m,
	}
}

func decodeMap(codec Codec, value any) map[string]any {
	raw := stringObjectMap(value)
	if len(raw) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(raw))
	for key, item := range raw {
		decoded, err := decodeValue(codec, item)
		if err != nil {
			out[key] = item
			continue
		}
		out[key] = decoded
	}
	return out
}

func decodeValue(codec Codec, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	return codec.Decode(value)
}

func respMap(value any) (map[string]any, error) {
	switch v := value.(type) {
	case map[interface{}]interface{}:
		out := make(map[string]any, len(v))
		for key, val := range v {
			out[asString(key)] = normalizeRESP(val)
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, val := range v {
			out[key] = normalizeRESP(val)
		}
		return out, nil
	case []any:
		return pairArrayMap(v)
	default:
		return nil, fmt.Errorf("expected RESP map, got %T", value)
	}
}

func pairArrayMap(items []any) (map[string]any, error) {
	if len(items) == 0 {
		return map[string]any{}, nil
	}
	if allPairs(items) {
		out := make(map[string]any, len(items))
		for _, pair := range items {
			p := pair.([]any)
			out[asString(p[0])] = normalizeRESP(p[1])
		}
		return out, nil
	}
	if len(items)%2 != 0 {
		return nil, fmt.Errorf("odd RESP map array length %d", len(items))
	}
	out := make(map[string]any, len(items)/2)
	for i := 0; i < len(items); i += 2 {
		out[asString(items[i])] = normalizeRESP(items[i+1])
	}
	return out, nil
}

func allPairs(items []any) bool {
	for _, item := range items {
		pair, ok := item.([]any)
		if !ok || len(pair) != 2 {
			return false
		}
	}
	return true
}

func stringObjectMap(value any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	mapping, err := respMap(value)
	if err != nil {
		return map[string]any{}
	}
	return mapping
}

func kvResponse(value any) (map[string]any, error) {
	if value == nil {
		return map[string]any{}, nil
	}
	if mapping, err := respMap(value); err == nil {
		return mapping, nil
	}
	text := asString(value)
	out := make(map[string]any)
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		key, raw, ok := strings.Cut(line, ":")
		if !ok {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			key = fields[0]
			raw = strings.Join(fields[1:], " ")
		}
		out[strings.TrimSpace(key)] = coerceTextValue(strings.TrimSpace(raw))
	}
	return out, nil
}

func coerceTextValue(value string) any {
	if value == "" {
		return ""
	}
	if n, err := strconv.ParseInt(value, 10, 64); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		return f
	}
	lower := strings.ToLower(value)
	if lower == "true" || lower == "yes" || lower == "ok" {
		return true
	}
	if lower == "false" || lower == "no" {
		return false
	}
	return value
}

func normalizeRESP(value any) any {
	switch v := value.(type) {
	case map[interface{}]interface{}:
		out := make(map[string]any, len(v))
		for key, val := range v {
			out[asString(key)] = normalizeRESP(val)
		}
		return out
	case []interface{}:
		out := make([]any, len(v))
		for i, val := range v {
			out[i] = normalizeRESP(val)
		}
		return out
	default:
		return value
	}
}

func asString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	default:
		return fmt.Sprint(v)
	}
}

func asBytes(value any) []byte {
	switch v := value.(type) {
	case nil:
		return nil
	case []byte:
		return v
	case string:
		return []byte(v)
	default:
		return []byte(fmt.Sprint(v))
	}
}

func asInt64(value any) int64 {
	switch v := value.(type) {
	case nil:
		return 0
	case int64:
		return v
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case int16:
		return int64(v)
	case int8:
		return int64(v)
	case uint64:
		return int64(v)
	case uint:
		return int64(v)
	case string:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	case []byte:
		n, _ := strconv.ParseInt(string(v), 10, 64)
		return n
	default:
		n, _ := strconv.ParseInt(fmt.Sprint(v), 10, 64)
		return n
	}
}

func asFloat64(value any) float64 {
	switch v := value.(type) {
	case nil:
		return 0
	case float64:
		return v
	case float32:
		return float64(v)
	case int64:
		return float64(v)
	case int:
		return float64(v)
	case string:
		n, _ := strconv.ParseFloat(v, 64)
		return n
	case []byte:
		n, _ := strconv.ParseFloat(string(v), 64)
		return n
	default:
		n, _ := strconv.ParseFloat(fmt.Sprint(v), 64)
		return n
	}
}

func asBool(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return v == "1" || v == "true" || v == "OK"
	case []byte:
		return asBool(string(v))
	case int64:
		return v != 0
	case int:
		return v != 0
	default:
		return value != nil
	}
}

func isOK(value any) bool {
	switch v := value.(type) {
	case string:
		return v == "OK"
	case []byte:
		return string(v) == "OK"
	default:
		return false
	}
}
