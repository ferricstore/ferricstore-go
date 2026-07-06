package ferricstore

import "fmt"

func valueOrNow(value int64) int64 {
	if value != 0 {
		return value
	}
	return nowMS()
}

func mixedPartition(partitionKey string) string {
	if partitionKey == "" {
		return "MIXED"
	}
	return partitionKey
}

func appendClaimedItems(args *[]any, partitionKey string, items []ClaimedItem, command string) error {
	*args = append(*args, "ITEMS")
	mixed := partitionKey == ""
	for _, item := range items {
		if mixed {
			if item.PartitionKey == "" {
				return fmt.Errorf("%s mixed items require partition key", command)
			}
			*args = append(*args, item.ID, item.PartitionKey, item.LeaseToken, item.FencingToken)
		} else {
			*args = append(*args, item.ID, item.LeaseToken, item.FencingToken)
		}
	}
	return nil
}

func appendFencedItems(args *[]any, partitionKey string, items []FencedItem, command string, includeLease bool) error {
	*args = append(*args, "ITEMS")
	mixed := partitionKey == ""
	for _, item := range items {
		if mixed {
			if item.PartitionKey == "" {
				return fmt.Errorf("%s mixed items require partition key", command)
			}
			*args = append(*args, item.ID, item.PartitionKey)
		} else {
			*args = append(*args, item.ID)
		}
		*args = append(*args, item.FencingToken)
		if includeLease {
			*args = append(*args, item.LeaseToken)
		}
	}
	return nil
}

func anyItemPartition(items []CreateItem) bool {
	for _, item := range items {
		if item.PartitionKey != "" {
			return true
		}
	}
	return false
}

func anyCreateItemValues(items []CreateItem) bool {
	for _, item := range items {
		if len(item.Values) > 0 || len(item.ValueRefs) > 0 {
			return true
		}
	}
	return false
}

func anyCreateItemStateMeta(items []CreateItem) bool {
	for _, item := range items {
		if len(item.StateMeta) > 0 {
			return true
		}
	}
	return false
}

func anyChildPartition(items []ChildSpec) bool {
	for _, item := range items {
		if item.PartitionKey != "" {
			return true
		}
	}
	return false
}

func anyChildValues(items []ChildSpec) bool {
	for _, item := range items {
		if len(item.Values) > 0 || len(item.ValueRefs) > 0 {
			return true
		}
	}
	return false
}

func mergeValues(base, item map[string]any) map[string]any {
	merged := map[string]any{}
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range item {
		merged[key] = value
	}
	return merged
}

func mergeRefs(base, item map[string]string) map[string]string {
	merged := map[string]string{}
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range item {
		merged[key] = value
	}
	return merged
}

func runStepsItems(items []RunStepsItem, partitionKey string) []map[string]string {
	out := make([]map[string]string, 0, len(items))
	for _, item := range items {
		entry := map[string]string{"id": item.ID}
		if item.PartitionKey != "" {
			entry["partition_key"] = item.PartitionKey
		} else if partitionKey != "" {
			entry["partition_key"] = partitionKey
		}
		out = append(out, entry)
	}
	return out
}
