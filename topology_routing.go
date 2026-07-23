package ferricstore

import (
	"hash/crc32"
	"strings"
)

type topologyRouteData struct {
	command  nativeCommand
	route    RoutingRoute
	snapshot topologyRoutingSnapshot
}

type topologyRouteSlot uint16

func routingKeyForCommand(args []any) (any, bool) {
	if len(args) == 0 {
		return nil, false
	}
	command, err := buildNativeCommand(args)
	if err != nil {
		return nil, false
	}
	return routingKeyForBuiltCommand(args, command)
}

func routingKeyForBuiltCommand(args []any, command nativeCommand) (any, bool) {
	routeArgs := canonicalCommandArgs(args)
	name := commandName(routeArgs)
	if command.opcode < nativeOpCommandExec || name == "CLUSTER.KEYSLOT" || name == "SHARDS" || name == "ROUTE" {
		return nil, false
	}
	// Flow routing has command-specific storage-key transformations and an
	// explicit control-path fallback. Do not let the generic payload heuristic
	// reinterpret a deliberately unrouted Flow ID or scope as a normal key.
	if strings.HasPrefix(name, "FLOW.") {
		return flowRoutingKey(name, routeArgs)
	}
	if key, ok := routingKeyFromArgs(name, routeArgs); ok {
		return key, true
	}
	mapping, ok := command.payload.(map[string]any)
	if !ok {
		return nil, false
	}
	for _, field := range []string{"key", "partition_key", "id", "owner_flow_id", "parent_flow_id", "root_flow_id", "correlation_id", "scope"} {
		value := mapping[field]
		if isRouteKey(value) {
			return value, true
		}
	}
	if keys, ok := mapping["keys"].([]any); ok {
		return singleShardKey(keys)
	}
	if pairs, ok := mapping["pairs"].([]any); ok {
		keys := make([]any, 0, len(pairs))
		for _, pair := range pairs {
			switch p := pair.(type) {
			case []any:
				if len(p) > 0 {
					keys = append(keys, p[0])
				}
			}
		}
		return singleShardKey(keys)
	}
	return nil, false
}

func topologyRequestContext(args []any) (any, bool) {
	if len(args) < 3 || commandPart(args[0]) != "COMMAND_EXEC" {
		return nil, false
	}
	_, requestContext, ok := splitNativeRequestContext(args)
	return requestContext, ok
}

func routingKeyFromArgs(name string, args []any) (any, bool) {
	if strings.HasPrefix(name, "FLOW.") {
		return flowRoutingKey(name, args)
	}
	keys, _, ok := topologyPolicyKeys(name, args)
	if !ok {
		return nil, false
	}
	return singleShardKey(keys)
}

func sameSlotCommandKeys(args []any) ([]any, bool) {
	args = canonicalCommandArgs(args)
	if len(args) == 0 {
		return nil, false
	}
	name := commandName(args)
	keys, policy, ok := topologyPolicyKeys(name, args)
	if !ok || !policy.requireSameSlot {
		return nil, false
	}
	return keys, true
}

func countedReadKeys(args []any) ([]any, bool) {
	if len(args) < 3 {
		return nil, false
	}
	count, ok := boundedRoutingCount(args[1], len(args)-2, false)
	if !ok {
		return nil, false
	}
	return args[2 : 2+count], true
}

func countedStoreKeys(args []any) ([]any, bool) {
	if len(args) < 4 {
		return nil, false
	}
	count, ok := boundedRoutingCount(args[2], len(args)-3, false)
	if !ok {
		return nil, false
	}
	keys := make([]any, 0, count+1)
	keys = append(keys, args[1])
	keys = append(keys, args[3:3+count]...)
	return keys, true
}

func boundedRoutingCount(value any, available int, allowZero bool) (int, bool) {
	count := asInt64(value)
	if count < 0 || (!allowZero && count == 0) || count > int64(available) {
		return 0, false
	}
	return int(count), true
}

func singleShardKey(keys []any) (any, bool) {
	var first any
	firstSlot := 0
	found := false
	for _, key := range keys {
		slot, ok := routingTargetSlot(key)
		if !ok {
			return nil, false
		}
		if !found {
			first, firstSlot, found = key, slot, true
			continue
		}
		if slot != firstSlot {
			return nil, false
		}
	}
	if !found {
		return nil, false
	}
	return first, true
}

func routingTargetSlot(value any) (int, bool) {
	if slot, ok := value.(topologyRouteSlot); ok {
		if int(slot) < routeSlotCount {
			return int(slot), true
		}
		return 0, false
	}
	if !isRouteKey(value) {
		return 0, false
	}
	return routeSlotForKey(value), true
}

func isRouteKey(value any) bool {
	switch value.(type) {
	case string, []byte:
		return true
	default:
		return false
	}
}

func commandName(args []any) string {
	first := commandPart(args[0])
	if first == "" {
		return ""
	}
	if len(args) > 1 && (first == "FLOW" || first == "CLIENT" || first == "CLUSTER") {
		second := commandPart(args[1])
		if second != "" {
			return first + "." + second
		}
	}
	return first
}

func commandPart(value any) string {
	text, ok := commandText(value)
	if !ok {
		return ""
	}
	return strings.ToUpper(text)
}

func routeSlotForKey(key any) int {
	return routeSlotForString(asString(key))
}

func routeSlotForString(text string) int {
	var hashInput string
	if strings.HasPrefix(text, "f:{") {
		hashInput = flowHashTag(text[3:], text)
	} else if strings.HasPrefix(text, "X:f:{") {
		hashInput = flowHashTag(text[5:], text)
	} else {
		hashInput = hashTagOrKey(text)
	}
	return int(routeCRC32(hashInput)) & routeSlotMask
}

func routeCRC32(value string) uint32 {
	crc := ^uint32(0)
	for index := 0; index < len(value); index++ {
		crc = crc32.IEEETable[byte(crc)^value[index]] ^ crc>>8
	}
	return ^crc
}

func hashTagOrKey(key string) string {
	start := strings.IndexByte(key, '{')
	if start < 0 {
		return key
	}
	end := strings.IndexByte(key[start+1:], '}')
	if end <= 0 {
		return key
	}
	return key[start+1 : start+1+end]
}

func flowHashTag(rest string, fallbackKey string) string {
	end := strings.IndexByte(rest, '}')
	if end > 0 {
		return rest[:end]
	}
	return hashTagOrKey(fallbackKey)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var typeScopedFlowCommands = map[string]bool{
	"FLOW.APPROVAL.LIST":       true,
	"FLOW.ATTRIBUTE_VALUES":    true,
	"FLOW.ATTRIBUTES":          true,
	"FLOW.BUDGET.LIST":         true,
	"FLOW.GOVERNANCE.OVERVIEW": true,
	"FLOW.INFO":                true,
	"FLOW.LIMIT.LIST":          true,
	"FLOW.POLICY.GET":          true,
	"FLOW.POLICY.SET":          true,
	"FLOW.RETENTION_CLEANUP":   true,
	"FLOW.SCHEDULE.FIRE_DUE":   true,
	"FLOW.SCHEDULE.LIST":       true,
	"FLOW.STATS":               true,
}

var flowScheduleCommands = map[string]bool{
	"FLOW.SCHEDULE.CREATE":   true,
	"FLOW.SCHEDULE.DELETE":   true,
	"FLOW.SCHEDULE.FIRE":     true,
	"FLOW.SCHEDULE.FIRE_DUE": true,
	"FLOW.SCHEDULE.GET":      true,
	"FLOW.SCHEDULE.LIST":     true,
	"FLOW.SCHEDULE.PAUSE":    true,
	"FLOW.SCHEDULE.RESUME":   true,
}

var flowApprovalIDCommands = map[string]bool{
	"FLOW.APPROVAL.APPROVE": true,
	"FLOW.APPROVAL.GET":     true,
	"FLOW.APPROVAL.REJECT":  true,
	"FLOW.APPROVAL.REQUEST": true,
}

var flowGovernanceScopeCommands = map[string]bool{
	"FLOW.BUDGET.COMMIT":  true,
	"FLOW.BUDGET.GET":     true,
	"FLOW.BUDGET.RELEASE": true,
	"FLOW.BUDGET.RESERVE": true,
	"FLOW.CIRCUIT.CLOSE":  true,
	"FLOW.CIRCUIT.GET":    true,
	"FLOW.CIRCUIT.OPEN":   true,
	"FLOW.LIMIT.GET":      true,
	"FLOW.LIMIT.LEASE":    true,
	"FLOW.LIMIT.RELEASE":  true,
	"FLOW.LIMIT.SPEND":    true,
}

var flowStateIDCommands = map[string]bool{
	"FLOW.CANCEL":            true,
	"FLOW.COMPLETE":          true,
	"FLOW.CREATE":            true,
	"FLOW.EFFECT.COMPENSATE": true,
	"FLOW.EFFECT.CONFIRM":    true,
	"FLOW.EFFECT.FAIL":       true,
	"FLOW.EFFECT.GET":        true,
	"FLOW.EFFECT.RESERVE":    true,
	"FLOW.EXTEND_LEASE":      true,
	"FLOW.FAIL":              true,
	"FLOW.GET":               true,
	"FLOW.GOVERNANCE.LEDGER": true,
	"FLOW.HISTORY":           true,
	"FLOW.RETRY":             true,
	"FLOW.REWIND":            true,
	"FLOW.SIGNAL":            true,
	"FLOW.SPAWN_CHILDREN":    true,
	"FLOW.START_AND_CLAIM":   true,
	"FLOW.STEP_CONTINUE":     true,
	"FLOW.TRANSITION":        true,
}

var flowValueReturnCommands = map[string]bool{
	"FLOW.CLAIM_DUE": true,
	"FLOW.GET":       true,
	"FLOW.RECLAIM":   true,
}
