package ferricstore

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func metricsMapResponse(value any) (map[string]any, error) {
	var text string
	switch typed := value.(type) {
	case map[interface{}]interface{}, map[string]any, []any:
		return nativeMap(value)
	case string:
		text = typed
	case []byte:
		text = string(typed)
	default:
		return nil, fmt.Errorf("expected Prometheus metrics map or text, got %T", value)
	}

	result := make(map[string]any)
	for lineNumber, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if key, raw, ok := legacyMetricsPair(line); ok {
			result[key] = coerceTextValue(raw)
			continue
		}
		identityEnd, err := prometheusIdentityEnd(line)
		if err != nil {
			return nil, fmt.Errorf("malformed Prometheus sample line %d: %w", lineNumber+1, err)
		}
		identity := line[:identityEnd]
		fields := strings.Fields(line[identityEnd:])
		if len(fields) < 1 || len(fields) > 2 {
			return nil, fmt.Errorf("malformed Prometheus sample line %d", lineNumber+1)
		}
		if _, err := strconv.ParseFloat(fields[0], 64); err != nil {
			return nil, fmt.Errorf("prometheus sample %q has invalid value %q", identity, fields[0])
		}
		if len(fields) == 2 {
			if _, err := strconv.ParseInt(fields[1], 10, 64); err != nil {
				return nil, fmt.Errorf("prometheus sample %q has invalid timestamp %q", identity, fields[1])
			}
		}
		// This legacy map API is inherently lossy for duplicate series. Preserve
		// its established last-sample-wins behavior; MetricsText is lossless.
		result[identity] = coerceTextValue(fields[0])
	}
	if len(result) == 0 && strings.TrimSpace(text) != "" {
		return nil, errors.New("expected at least one Prometheus sample")
	}
	return result, nil
}

func legacyMetricsPair(line string) (string, string, bool) {
	separator := strings.IndexByte(line, ':')
	if separator <= 0 || separator >= len(line)-1 ||
		(line[separator+1] != ' ' && line[separator+1] != '\t') {
		return "", "", false
	}
	key := strings.TrimSpace(line[:separator])
	raw := strings.TrimSpace(line[separator+1:])
	if key == "" || raw == "" || strings.ContainsAny(key, "{} \t\r") {
		return "", "", false
	}
	return key, raw, true
}

func prometheusIdentityEnd(line string) (int, error) {
	inLabels := false
	inQuote := false
	escaped := false
	closedLabels := false
	for index := 0; index < len(line); index++ {
		character := line[index]
		if inQuote {
			if escaped {
				escaped = false
				continue
			}
			if character == '\\' {
				escaped = true
				continue
			}
			if character == '"' {
				inQuote = false
			}
			continue
		}
		switch character {
		case '{':
			if inLabels || closedLabels || index == 0 {
				return 0, errors.New("invalid label braces")
			}
			inLabels = true
		case '}':
			if !inLabels {
				return 0, errors.New("unmatched label brace")
			}
			inLabels = false
			closedLabels = true
		case '"':
			if !inLabels {
				return 0, errors.New("quote outside labels")
			}
			inQuote = true
		case ' ', '\t', '\r':
			if !inLabels {
				if index == 0 {
					return 0, errors.New("empty metric identity")
				}
				return index, nil
			}
		default:
			if closedLabels {
				return 0, errors.New("characters after label set")
			}
		}
	}
	if inLabels || inQuote || escaped {
		return 0, errors.New("unterminated label set")
	}
	return 0, errors.New("sample value is missing")
}
