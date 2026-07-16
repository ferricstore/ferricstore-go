package ferricstore

import (
	"bytes"
	"encoding/binary"
	"strings"
)

type flowOptionSet struct {
	values     map[string]any
	seen       map[string]struct{}
	partitions []any
	itemsToken string
}

func parseFlowOptionsUntilItems(args []any) (flowOptionSet, int, bool) {
	opts := flowOptionSet{values: map[string]any{}, seen: map[string]struct{}{}}
	for i := 0; i < len(args); {
		token := strings.ToUpper(asString(args[i]))
		if token == "ITEMS" || token == "ITEMS_EXT" {
			opts.itemsToken = token
			return opts, i + 1, true
		}
		if i+1 >= len(args) {
			return flowOptionSet{}, 0, false
		}
		if _, duplicate := opts.seen[token]; duplicate {
			return flowOptionSet{}, 0, false
		}
		opts.seen[token] = struct{}{}
		opts.values[token] = args[i+1]
		i += 2
	}
	return flowOptionSet{}, 0, false
}

func parseFlowOptions(args []any) (flowOptionSet, bool) {
	opts := flowOptionSet{values: map[string]any{}, seen: map[string]struct{}{}}
	for i := 0; i < len(args); {
		token := strings.ToUpper(asString(args[i]))
		if _, duplicate := opts.seen[token]; duplicate {
			return flowOptionSet{}, false
		}
		opts.seen[token] = struct{}{}
		if token == "PARTITIONS" {
			if i+1 >= len(args) {
				return flowOptionSet{}, false
			}
			count64, err := responseInt64(args[i+1], nil)
			if err != nil {
				return flowOptionSet{}, false
			}
			remaining := len(args) - (i + 2)
			if count64 < 0 || count64 > int64(remaining) {
				return flowOptionSet{}, false
			}
			count := int(count64)
			opts.partitions = append([]any(nil), args[i+2:i+2+count]...)
			i += 2 + count
			continue
		}
		if i+1 >= len(args) {
			return flowOptionSet{}, false
		}
		opts.values[token] = args[i+1]
		i += 2
	}
	return opts, true
}

func (o flowOptionSet) only(keys ...string) bool {
	allowed := map[string]struct{}{}
	for _, key := range keys {
		allowed[key] = struct{}{}
	}
	for key := range o.seen {
		if _, ok := allowed[key]; !ok {
			return false
		}
	}
	return true
}

func (o flowOptionSet) value(key string) any {
	return o.values[key]
}

func (o flowOptionSet) has(key string) bool {
	_, ok := o.seen[key]
	return ok
}

func (o flowOptionSet) stringValue(key string) (string, bool) {
	value, ok := o.values[key]
	if !ok {
		return "", false
	}
	text := asString(value)
	return text, text != ""
}

func (o flowOptionSet) int64Value(key string) (int64, bool) {
	value, ok := o.values[key]
	if !ok {
		return 0, false
	}
	parsed, err := responseInt64(value, nil)
	return parsed, err == nil
}

func (o flowOptionSet) boolValue(key string) (bool, bool) {
	value, ok := o.values[key]
	if !ok || value == nil {
		return false, false
	}
	parsed, err := nativeFlowBool(value)
	return parsed, err == nil
}

func (o flowOptionSet) boolMarker(key string) (byte, bool) {
	value, ok := o.values[key]
	if !ok {
		return 0, true
	}
	if value == nil {
		return 0, false
	}
	parsed, err := nativeFlowBool(value)
	if err != nil {
		return 0, false
	}
	if parsed {
		return 2, true
	}
	return 1, true
}

func nativeFlowBool(value any) (bool, error) {
	switch v := value.(type) {
	case string:
		text := strings.TrimSpace(v)
		switch {
		case text == "1", strings.EqualFold(text, "true"), strings.EqualFold(text, "yes"), strings.EqualFold(text, "on"):
			return true, nil
		case text == "0", strings.EqualFold(text, "false"), strings.EqualFold(text, "no"), strings.EqualFold(text, "off"):
			return false, nil
		default:
			return false, errExpectedBooleanResponse
		}
	case []byte:
		text := bytes.TrimSpace(v)
		switch {
		case len(text) == 1 && text[0] == '1', bytes.EqualFold(text, []byte("true")), bytes.EqualFold(text, []byte("yes")), bytes.EqualFold(text, []byte("on")):
			return true, nil
		case len(text) == 1 && text[0] == '0', bytes.EqualFold(text, []byte("false")), bytes.EqualFold(text, []byte("no")), bytes.EqualFold(text, []byte("off")):
			return false, nil
		default:
			return false, errExpectedBooleanResponse
		}
	default:
		return responseBool(value, nil)
	}
}

func compactClaimReturnMode(value any) (byte, bool) {
	if value == nil {
		return 0, true
	}
	switch strings.ToUpper(asString(value)) {
	case "JOBS_COMPACT":
		return 1, true
	case "JOBS_COMPACT_STATE":
		return 2, true
	case "JOBS_COMPACT_ATTRS", "JOBS_COMPACT_ATTRIBUTES":
		return 3, true
	case "JOBS_COMPACT_STATE_ATTRS", "JOBS_COMPACT_STATE_ATTRIBUTES", "JOBS_COMPACT_WITH_STATE_ATTRS", "JOBS_COMPACT_WITH_STATE_ATTRIBUTES":
		return 4, true
	default:
		return 0, false
	}
}

func compactPartitionValues(opts flowOptionSet) (byte, any, []any, bool) {
	if opts.has("PARTITION") && opts.has("PARTITIONS") {
		return 0, nil, nil, false
	}
	if opts.has("PARTITION") {
		value := opts.value("PARTITION")
		if !isCompactPayloadValue(value) {
			return 0, nil, nil, false
		}
		return 1, value, nil, true
	}
	if opts.has("PARTITIONS") {
		if len(opts.partitions) == 0 {
			return 0, nil, nil, false
		}
		for _, value := range opts.partitions {
			if !isCompactPayloadValue(value) {
				return 0, nil, nil, false
			}
		}
		return 2, nil, opts.partitions, true
	}
	return 0, nil, nil, true
}

func compactBytes(value any) ([]byte, bool) {
	switch v := value.(type) {
	case nil:
		return nil, false
	case []byte:
		return v, true
	case string:
		return []byte(v), true
	default:
		return nil, false
	}
}

func writeCompactBinary(buf *bytes.Buffer, value []byte) {
	writeCompactU32(buf, uint32(len(value)))
	buf.Write(value)
}

func writeCompactOptionalBinary(buf *bytes.Buffer, value []byte) {
	if value == nil {
		writeCompactU32(buf, nativeCompactNilU32)
		return
	}
	writeCompactBinary(buf, value)
}

func writeCompactU32(buf *bytes.Buffer, value uint32) {
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], value)
	buf.Write(raw[:])
}
