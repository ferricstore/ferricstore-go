package ferricstore

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
)

func flowRoutingKey(name string, args []any) (any, bool) {
	flowArgs := args[1:]
	if len(flowArgs) == 0 {
		return nil, false
	}
	if name == "FLOW.VALUE.MGET" {
		return singleShardKey(flowValueMGetRoutingRefs(flowArgs))
	}
	if flowScheduleCommands[name] {
		// Schedule storage uses :erlang.phash2/2, which has no stable portable
		// Go implementation. Keep these commands on the control connection and
		// let the server select the owning shard.
		return nil, false
	}
	if flowApprovalIDCommands[name] {
		return flowLogicalPartitionRoutingKey(flowArgs[0])
	}
	if flowGovernanceScopeCommands[name] {
		return flowLogicalPartitionRoutingKey(flowArgs[0])
	}
	switch name {
	case "FLOW.CREATE_MANY", "FLOW.COMPLETE_MANY", "FLOW.TRANSITION_MANY", "FLOW.RETRY_MANY", "FLOW.FAIL_MANY", "FLOW.CANCEL_MANY":
		partition := flowArgs[0]
		if !isRouteKey(partition) {
			return nil, false
		}
		part := strings.ToUpper(asString(partition))
		if part == "AUTO" || part == "MIXED" {
			return nil, false
		}
		return flowLogicalPartitionRoutingKey(partition)
	}
	claim := name == "FLOW.CLAIM_DUE" || name == "FLOW.RECLAIM"
	optionStart := flowOptionStart(name)
	if key, handled := flowPartitionRoutingKey(name, flowArgs, optionStart, claim); handled {
		return key, true
	}
	if flowHasPartitionOption(name, flowArgs, optionStart) {
		return nil, false
	}
	if name == "FLOW.VALUE.PUT" {
		owner, ownerOK := flowNamedOption(name, flowArgs, optionStart, "OWNER_FLOW_ID")
		_, nameOK := flowNamedOption(name, flowArgs, optionStart, "NAME")
		if ownerOK && nameOK {
			return flowAutoIDRoutingKey(owner)
		}
		return nil, false
	}
	if typeScopedFlowCommands[name] {
		return nil, false
	}
	if flowStateIDCommands[name] {
		return flowAutoIDRoutingKey(flowArgs[0])
	}
	return nil, false
}

func flowValueMGetRoutingRefs(args []any) []any {
	if len(args) < 2 {
		return args
	}
	option := len(args) - 2
	token := commandPart(args[option])
	if token != "MAX_BYTES" && token != "MAXBYTES" {
		return args
	}
	maximum, err := topologyInteger(args[option+1], "MAX_BYTES")
	if err != nil || maximum < 0 {
		return args
	}
	return args[:option]
}

func flowPartitionRoutingKey(name string, args []any, start int, claim bool) (any, bool) {
	for idx := start; idx < len(args); idx = nextFlowOption(name, args, idx) {
		token := commandPart(args[idx])
		switch token {
		case "PARTITION":
			if idx+1 < len(args) && isRouteKey(args[idx+1]) {
				if claim {
					return flowClaimPartitionRoutingKey(args[idx+1])
				}
				return flowLogicalPartitionRoutingKey(args[idx+1])
			}
			return nil, false
		case "PARTITIONS":
			if idx+1 >= len(args) {
				return nil, false
			}
			count64 := asInt64(args[idx+1])
			remaining := len(args) - (idx + 2)
			if count64 < 0 || count64 > int64(remaining) {
				return nil, false
			}
			count := int(count64)
			keys := make([]any, 0, count)
			for _, partition := range args[idx+2 : idx+2+count] {
				var key any
				var ok bool
				if claim {
					key, ok = flowClaimPartitionRoutingKey(partition)
				} else {
					key, ok = flowLogicalPartitionRoutingKey(partition)
				}
				if !ok {
					return nil, false
				}
				keys = append(keys, key)
			}
			return singleShardKey(keys)
		}
	}
	return nil, false
}

func flowHasPartitionOption(name string, args []any, start int) bool {
	for idx := start; idx < len(args); idx = nextFlowOption(name, args, idx) {
		token := commandPart(args[idx])
		if token == "PARTITION" || token == "PARTITIONS" {
			return true
		}
	}
	return false
}

func flowNamedOption(name string, args []any, start int, wanted string) (any, bool) {
	for idx := start; idx+1 < len(args); idx = nextFlowOption(name, args, idx) {
		if commandPart(args[idx]) == wanted {
			return args[idx+1], isRouteKey(args[idx+1])
		}
	}
	return nil, false
}

func flowOptionStart(name string) int {
	switch name {
	case "FLOW.SEARCH", "FLOW.APPROVAL.LIST", "FLOW.GOVERNANCE.OVERVIEW", "FLOW.BUDGET.LIST", "FLOW.LIMIT.LIST":
		return 0
	case "FLOW.ATTRIBUTE_VALUES":
		return 2
	case "FLOW.EXTEND_LEASE", "FLOW.COMPLETE", "FLOW.RETRY", "FLOW.FAIL":
		return 2
	case "FLOW.TRANSITION":
		return 3
	case "FLOW.STEP_CONTINUE":
		return 4
	default:
		return 1
	}
}

func nextFlowOption(name string, args []any, index int) int {
	if index >= len(args) {
		return len(args)
	}
	switch commandPart(args[index]) {
	case "ATTRIBUTE", "ATTRIBUTE_MERGE", "VALUE_REF":
		return minInt(len(args), index+3)
	case "STATE_META":
		if name == "FLOW.SEARCH" {
			return minInt(len(args), index+4)
		}
		return minInt(len(args), index+3)
	case "VALUE":
		if flowValueReturnCommands[name] {
			return minInt(len(args), index+2)
		}
		return minInt(len(args), index+3)
	case "ITEMS", "ITEMS_EXT":
		return len(args)
	case "PARTITIONS":
		if index+1 >= len(args) {
			return len(args)
		}
		count, ok := boundedRoutingCount(args[index+1], len(args)-index-2, true)
		if !ok {
			return len(args)
		}
		return index + 2 + count
	default:
		return minInt(len(args), index+2)
	}
}

func flowLogicalPartitionRoutingKey(value any) (any, bool) {
	if !isRouteKey(value) {
		return nil, false
	}
	bytes := asBytes(value)
	text := string(bytes)
	const autoPrefix = "__flow_auto__:"
	if strings.HasPrefix(text, autoPrefix) {
		bucketText := strings.TrimPrefix(text, autoPrefix)
		bucket, err := strconv.Atoi(bucketText)
		if err == nil && bucket >= 0 && bucket < 256 && strconv.Itoa(bucket) == bucketText {
			return fmt.Sprintf("f:{fa:%d}:route", bucket), true
		}
	}
	digest := sha256.Sum256(bytes)
	return "f:{f:" + base64.RawURLEncoding.EncodeToString(digest[:]) + "}:route", true
}

func flowAutoIDRoutingKey(value any) (any, bool) {
	if !isRouteKey(value) {
		return nil, false
	}
	bucket := crc32.ChecksumIEEE(asBytes(value)) & 0xff
	return fmt.Sprintf("f:{fa:%d}:route", bucket), true
}

func flowClaimPartitionRoutingKey(value any) (any, bool) {
	if !isRouteKey(value) {
		return nil, false
	}
	switch commandPart(value) {
	case "AUTO", "ANY":
		return nil, false
	case "GLOBAL":
		return "f:{f}:route", true
	default:
		return flowLogicalPartitionRoutingKey(value)
	}
}
