package ferricstore

import "fmt"

func validateTopologyPipelineScatterResult(
	args []any,
	route RoutingRoute,
	result pipelineItemResult,
) pipelineItemResult {
	if result.err != nil {
		return result
	}
	name, keys, scatter := safeScatterCommand(args)
	if !scatter || len(keys) == 0 {
		return result
	}

	var err error
	if name == "MGET" {
		items, ok := result.value.([]any)
		if !ok || len(items) != len(keys) {
			err = fmt.Errorf("MGET shard returned %T with %d values, expected %d", result.value, len(items), len(keys))
		}
	} else {
		var count int64
		count, err = responseInt64(result.value, nil)
		if err == nil && (count < 0 || count > int64(len(keys))) {
			err = fmt.Errorf("%s shard count %d is outside valid range 0..%d", name, count, len(keys))
		}
	}
	if err != nil {
		result.value = nil
		result.err = topologyKeyWriteFailure(destructiveScatterCommand(name), route, keys, err)
	}
	return result
}
