package ferricstore

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

func clusterReportResponse(value any) (map[string]any, error) {
	var text string
	switch typed := value.(type) {
	case map[interface{}]interface{}, map[string]any, []any:
		return nativeMap(value)
	case string:
		text = typed
	case []byte:
		text = string(typed)
	default:
		return nil, fmt.Errorf("expected cluster report map or text, got %T", value)
	}

	out := make(map[string]any)
	var section map[string]any
	lines := strings.Split(text, "\n")
	for lineNumber, line := range lines {
		line = strings.TrimSuffix(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		indented := line[0] == ' ' || line[0] == '\t'
		key, raw, ok := strings.Cut(strings.TrimSpace(line), ":")
		key = strings.TrimSpace(key)
		raw = strings.TrimSpace(raw)
		if !ok || key == "" {
			return nil, fmt.Errorf("malformed cluster report line %d", lineNumber+1)
		}

		if indented {
			if section == nil {
				return nil, fmt.Errorf("cluster report line %d has no parent section", lineNumber+1)
			}
			if _, exists := section[key]; exists {
				return nil, fmt.Errorf("duplicate cluster report field %q", key)
			}
			section[key] = coerceClusterReportValue(key, raw)
			continue
		}

		section = nil
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("duplicate cluster report field %q", key)
		}
		if raw == "" && nextClusterReportLineIndented(lines, lineNumber+1) {
			section = make(map[string]any)
			out[key] = section
			continue
		}
		out[key] = coerceClusterReportValue(key, raw)
	}
	if len(out) == 0 {
		return nil, errors.New("expected non-empty cluster report")
	}
	return out, nil
}

func nextClusterReportLineIndented(lines []string, start int) bool {
	for _, line := range lines[start:] {
		line = strings.TrimSuffix(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		return line[0] == ' ' || line[0] == '\t'
	}
	return false
}

func coerceClusterReportValue(key, value string) any {
	switch key {
	case "keys", "memory_bytes", "total_keys", "total_memory_bytes":
		if number, err := strconv.ParseInt(value, 10, 64); err == nil {
			return number
		}
	}
	return value
}

func clusterHealthResponse(value any) (map[string]any, error) {
	report, err := clusterReportResponse(value)
	if err != nil {
		return nil, err
	}
	shards, err := clusterShardSections(report)
	if err != nil {
		return nil, err
	}
	for _, shard := range shards {
		if _, err := clusterRequiredText(shard.fields, "role", false); err != nil {
			return nil, fmt.Errorf("CLUSTER.HEALTH %s: %w", shard.name, err)
		}
		if _, err := clusterRequiredText(shard.fields, "status", false); err != nil {
			return nil, fmt.Errorf("CLUSTER.HEALTH %s: %w", shard.name, err)
		}
		if _, err := clusterNonNegativeInt(shard.fields, "keys"); err != nil {
			return nil, fmt.Errorf("CLUSTER.HEALTH %s: %w", shard.name, err)
		}
		if _, err := clusterNonNegativeInt(shard.fields, "memory_bytes"); err != nil {
			return nil, fmt.Errorf("CLUSTER.HEALTH %s: %w", shard.name, err)
		}
	}
	return report, nil
}

func clusterStatsResponse(value any) (map[string]any, error) {
	report, err := clusterReportResponse(value)
	if err != nil {
		return nil, err
	}
	shards, err := clusterShardSections(report)
	if err != nil {
		return nil, err
	}
	var keysSum, memorySum int64
	for _, shard := range shards {
		keys, err := clusterNonNegativeInt(shard.fields, "keys")
		if err != nil {
			return nil, fmt.Errorf("CLUSTER.STATS %s: %w", shard.name, err)
		}
		memory, err := clusterNonNegativeInt(shard.fields, "memory_bytes")
		if err != nil {
			return nil, fmt.Errorf("CLUSTER.STATS %s: %w", shard.name, err)
		}
		if keys > math.MaxInt64-keysSum || memory > math.MaxInt64-memorySum {
			return nil, errors.New("CLUSTER.STATS shard totals overflow int64")
		}
		keysSum += keys
		memorySum += memory
	}
	totalKeys, err := clusterNonNegativeInt(report, "total_keys")
	if err != nil {
		return nil, fmt.Errorf("CLUSTER.STATS: %w", err)
	}
	totalMemory, err := clusterNonNegativeInt(report, "total_memory_bytes")
	if err != nil {
		return nil, fmt.Errorf("CLUSTER.STATS: %w", err)
	}
	if totalKeys != keysSum || totalMemory != memorySum {
		return nil, fmt.Errorf(
			"CLUSTER.STATS totals do not match shards: keys=%d/%d memory_bytes=%d/%d",
			totalKeys, keysSum, totalMemory, memorySum,
		)
	}
	return report, nil
}

func clusterStatusResponse(value any) (map[string]any, error) {
	report, err := clusterReportResponse(value)
	if err != nil {
		return nil, err
	}
	for _, field := range []string{"mode", "replication_mode", "cluster_state", "role", "node", "sync_status"} {
		if _, err := clusterRequiredText(report, field, false); err != nil {
			return nil, fmt.Errorf("CLUSTER.STATUS: %w", err)
		}
	}
	if _, err := clusterRequiredText(report, "connected_nodes", true); err != nil {
		return nil, fmt.Errorf("CLUSTER.STATUS: %w", err)
	}
	shards, err := clusterShardSections(report)
	if err != nil {
		return nil, err
	}
	for _, shard := range shards {
		if _, failed := shard.fields["error"]; failed {
			if _, err := clusterRequiredText(shard.fields, "error", false); err != nil {
				return nil, fmt.Errorf("CLUSTER.STATUS %s: %w", shard.name, err)
			}
			continue
		}
		if _, err := clusterRequiredText(shard.fields, "leader", false); err != nil {
			return nil, fmt.Errorf("CLUSTER.STATUS %s: %w", shard.name, err)
		}
		if _, err := clusterRequiredText(shard.fields, "members", true); err != nil {
			return nil, fmt.Errorf("CLUSTER.STATUS %s: %w", shard.name, err)
		}
	}
	return report, nil
}

type clusterShardSection struct {
	name   string
	fields map[string]any
}

func clusterShardSections(report map[string]any) ([]clusterShardSection, error) {
	names := make([]string, 0, len(report))
	for name := range report {
		if strings.HasPrefix(name, "shard_") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return nil, errors.New("cluster report contains no shard sections")
	}
	shards := make([]clusterShardSection, 0, len(names))
	for _, name := range names {
		index := strings.TrimPrefix(name, "shard_")
		if parsed, err := strconv.ParseUint(index, 10, 31); err != nil || strconv.FormatUint(parsed, 10) != index {
			return nil, fmt.Errorf("cluster report has invalid shard section %q", name)
		}
		fields, ok := report[name].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("cluster report section %q must be a map", name)
		}
		shards = append(shards, clusterShardSection{name: name, fields: fields})
	}
	return shards, nil
}

func clusterRequiredText(fields map[string]any, name string, allowEmpty bool) (string, error) {
	value, exists := fields[name]
	if !exists {
		return "", fmt.Errorf("missing field %q", name)
	}
	text, err := responseString(value, nil)
	if err != nil {
		return "", fmt.Errorf("field %q must be text", name)
	}
	if !allowEmpty && strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("field %q must be non-empty", name)
	}
	return text, nil
}

func clusterNonNegativeInt(fields map[string]any, name string) (int64, error) {
	value, exists := fields[name]
	if !exists {
		return 0, fmt.Errorf("missing field %q", name)
	}
	number, err := responseInt64(value, nil)
	if err != nil || number < 0 {
		return 0, fmt.Errorf("field %q must be a non-negative integer", name)
	}
	return number, nil
}

func hotnessResponse(value any) (map[string]any, error) {
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected FERRICSTORE.HOTNESS array, got %T", value)
	}
	const headerItems = 10
	const prefixItems = 8
	if len(items) < headerItems || (len(items)-headerItems)%prefixItems != 0 {
		return nil, fmt.Errorf("malformed FERRICSTORE.HOTNESS array length %d", len(items))
	}

	headerKeys := [...]string{
		"hot_reads", "cold_reads", "hot_read_pct", "cold_reads_per_second", "top_n",
	}
	for index, key := range headerKeys {
		if err := requireResponseKey(items[index*2], key); err != nil {
			return nil, err
		}
	}

	hotReads, err := nonNegativeResponseInt("hot_reads", items[1])
	if err != nil {
		return nil, err
	}
	coldReads, err := nonNegativeResponseInt("cold_reads", items[3])
	if err != nil {
		return nil, err
	}
	hotReadPct, err := percentageResponse("hot_read_pct", items[5])
	if err != nil {
		return nil, err
	}
	coldReadsPerSecond, err := responseFloat64(items[7], nil)
	if err != nil || coldReadsPerSecond < 0 {
		return nil, fmt.Errorf("FERRICSTORE.HOTNESS cold_reads_per_second must be finite and non-negative")
	}
	topN, err := responseInt64(items[9], nil)
	if err != nil || topN <= 0 {
		return nil, errors.New("FERRICSTORE.HOTNESS top_n must be positive")
	}

	prefixCount := (len(items) - headerItems) / prefixItems
	if int64(prefixCount) > topN {
		return nil, fmt.Errorf("FERRICSTORE.HOTNESS returned %d prefixes for top_n %d", prefixCount, topN)
	}
	prefixes := make([]any, 0, prefixCount)
	seenPrefixes := make(map[string]struct{}, prefixCount)
	for offset := headerItems; offset < len(items); offset += prefixItems {
		entry, err := hotnessPrefixResponse(items[offset : offset+prefixItems])
		if err != nil {
			return nil, err
		}
		prefix := entry["prefix"].(string)
		if _, exists := seenPrefixes[prefix]; exists {
			return nil, fmt.Errorf("duplicate FERRICSTORE.HOTNESS prefix %q", prefix)
		}
		seenPrefixes[prefix] = struct{}{}
		prefixes = append(prefixes, entry)
	}

	return map[string]any{
		"hot_reads":             hotReads,
		"cold_reads":            coldReads,
		"hot_read_pct":          hotReadPct,
		"cold_reads_per_second": coldReadsPerSecond,
		"top_n":                 topN,
		"prefixes":              prefixes,
	}, nil
}

func hotnessPrefixResponse(items []any) (map[string]any, error) {
	keys := [...]string{"prefix", "hot", "cold", "cold_pct"}
	for index, key := range keys {
		if err := requireResponseKey(items[index*2], key); err != nil {
			return nil, err
		}
	}
	prefix, err := responseString(items[1], nil)
	if err != nil {
		return nil, fmt.Errorf("FERRICSTORE.HOTNESS prefix: %w", err)
	}
	hot, err := nonNegativeResponseInt("prefix hot", items[3])
	if err != nil {
		return nil, err
	}
	cold, err := nonNegativeResponseInt("prefix cold", items[5])
	if err != nil {
		return nil, err
	}
	coldPct, err := percentageResponse("prefix cold_pct", items[7])
	if err != nil {
		return nil, err
	}
	return map[string]any{"prefix": prefix, "hot": hot, "cold": cold, "cold_pct": coldPct}, nil
}

func requireResponseKey(value any, expected string) error {
	key, err := responseString(value, nil)
	if err != nil || key != expected {
		return fmt.Errorf("FERRICSTORE.HOTNESS expected field %q", expected)
	}
	return nil
}

func nonNegativeResponseInt(field string, value any) (int64, error) {
	number, err := responseInt64(value, nil)
	if err != nil || number < 0 {
		return 0, fmt.Errorf("FERRICSTORE.HOTNESS %s must be a non-negative integer", field)
	}
	return number, nil
}

func percentageResponse(field string, value any) (float64, error) {
	number, err := responseFloat64(value, nil)
	if err != nil || number < 0 || number > 100 {
		return 0, fmt.Errorf("FERRICSTORE.HOTNESS %s must be between 0 and 100", field)
	}
	return number, nil
}

func validateClusterSlotsResponse(value any, err error) error {
	if err != nil {
		return err
	}
	ranges, ok := value.([]any)
	if !ok || len(ranges) == 0 {
		return fmt.Errorf("expected non-empty CLUSTER.SLOTS array, got %T", value)
	}
	expectedStart := int64(0)
	for index, rawRange := range ranges {
		slotRange, ok := rawRange.([]any)
		if !ok || len(slotRange) != 3 {
			return fmt.Errorf("CLUSTER.SLOTS range %d must contain start, end, and shard", index)
		}
		start, startErr := responseInt64(slotRange[0], nil)
		end, endErr := responseInt64(slotRange[1], nil)
		shard, shardErr := responseInt64(slotRange[2], nil)
		if startErr != nil || endErr != nil || shardErr != nil {
			return fmt.Errorf("CLUSTER.SLOTS range %d must contain integers", index)
		}
		if start != expectedStart || end < start || end >= routeSlotCount || shard < 0 {
			return fmt.Errorf("CLUSTER.SLOTS range %d is invalid", index)
		}
		expectedStart = end + 1
	}
	if expectedStart != routeSlotCount {
		return fmt.Errorf("CLUSTER.SLOTS ranges cover %d of %d slots", expectedStart, routeSlotCount)
	}
	return nil
}

func validateClusterRoleResponse(value any, err error) error {
	role, err := responseString(value, err)
	if err != nil {
		return err
	}
	if strings.TrimSpace(role) == "" {
		return errors.New("CLUSTER.ROLE returned an empty role")
	}
	return nil
}
