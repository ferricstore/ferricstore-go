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

func createManyPartitionMode(partitionKey string, items []CreateItem) (bool, error) {
	if partitionKey != "" {
		for _, item := range items {
			if item.PartitionKey != "" && item.PartitionKey != partitionKey {
				return false, fmt.Errorf("FLOW.CREATE_MANY item partition key does not match batch partition key")
			}
		}
		return false, nil
	}
	mixed := anyItemPartition(items)
	if mixed {
		for _, item := range items {
			if item.PartitionKey == "" {
				return false, fmt.Errorf("mixed create_many items require partition key")
			}
		}
	}
	return mixed, nil
}

func validateClaimedItemPartitions(partitionKey string, items []ClaimedItem, command string) error {
	for _, item := range items {
		if partitionKey == "" && item.PartitionKey == "" {
			return fmt.Errorf("%s mixed items require partition key", command)
		}
		if partitionKey != "" && item.PartitionKey != "" && item.PartitionKey != partitionKey {
			return fmt.Errorf("%s item partition key does not match batch partition key", command)
		}
	}
	return nil
}

func validateFencedItemPartitions(partitionKey string, items []FencedItem, command string) error {
	for _, item := range items {
		if partitionKey == "" && item.PartitionKey == "" {
			return fmt.Errorf("%s mixed items require partition key", command)
		}
		if partitionKey != "" && item.PartitionKey != "" && item.PartitionKey != partitionKey {
			return fmt.Errorf("%s item partition key does not match batch partition key", command)
		}
	}
	return nil
}

func appendClaimedItems(args *[]any, partitionKey string, items []ClaimedItem) {
	*args = append(*args, "ITEMS")
	mixed := partitionKey == ""
	for _, item := range items {
		if mixed {
			*args = append(*args, item.ID, item.PartitionKey, item.LeaseToken, item.FencingToken)
		} else {
			*args = append(*args, item.ID, item.LeaseToken, item.FencingToken)
		}
	}
}

func appendFencedItems(args *[]any, partitionKey string, items []FencedItem, includeLease bool) {
	*args = append(*args, "ITEMS")
	mixed := partitionKey == ""
	for _, item := range items {
		if mixed {
			*args = append(*args, item.ID, item.PartitionKey)
		} else {
			*args = append(*args, item.ID)
		}
		*args = append(*args, item.FencingToken)
		if includeLease {
			*args = append(*args, item.LeaseToken)
		}
	}
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

func anyCreateItemAttributes(items []CreateItem) bool {
	for _, item := range items {
		if len(item.Attributes) > 0 {
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
	if len(base) == 0 {
		return item
	}
	if len(item) == 0 {
		return base
	}
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
	if len(base) == 0 {
		return item
	}
	if len(item) == 0 {
		return base
	}
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
