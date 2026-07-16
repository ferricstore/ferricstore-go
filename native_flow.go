package ferricstore

import (
	"math"
	"strings"
)

const (
	nativeCompactFlowCreateManyRequest          = 0x90
	nativeCompactFlowCreateManyPartitionRequest = 0x96
	nativeCompactFlowCreateManyMixedRequest     = 0x9E
	nativeCompactFlowClaimDueRequest            = 0x91
	nativeCompactFlowCompleteManyRequest        = 0x92
	nativeCompactFlowCompleteManyOKRequest      = 0x93
	nativeCompactNilU32                         = math.MaxUint32
	nativeFlowClaimDefaultReclaimRatio          = int64(25)
	nativeFlowClaimDefaultReclaimExpired        = byte(1)
)

func buildFlowNativeCommand(name string, args []any) (nativeCommand, bool, error) {
	switch name {
	case "FLOW.POLICY.SET":
		return buildFlowPolicySetNative(args)
	case "FLOW.POLICY.GET":
		return buildFlowAdminNative(name, nativeOpFlowPolicyGet, args, "type")
	case "FLOW.SPAWN_CHILDREN":
		return buildFlowSpawnChildrenNative(args)
	}
	if hasFlowCommandOnlyOption(name, args) {
		return nativeCommand{}, false, nil
	}
	switch name {
	case "FLOW.CREATE_MANY":
		return buildFlowCreateManyNative(args)
	case "FLOW.CLAIM_DUE":
		return buildFlowClaimDueNative(args)
	case "FLOW.COMPLETE_MANY":
		return buildFlowCompleteManyNative(args)
	case "FLOW.START_AND_CLAIM":
		return buildFlowAdminNative(name, nativeOpFlowStartAndClaim, args, "id")
	case "FLOW.STEP_CONTINUE":
		return buildFlowAdminNative(name, nativeOpFlowStepContinue, args, "id", "lease_token", "from_state", "to_state")
	case "FLOW.RUN_STEPS_MANY":
		return buildFlowAdminNative(name, nativeOpFlowRunStepsMany, args)
	case "FLOW.SCHEDULE.CREATE":
		return buildFlowScheduleCreateNative(args)
	case "FLOW.SCHEDULE.GET":
		return buildFlowScheduleIDNative(name, nativeOpFlowScheduleGet, args)
	case "FLOW.SCHEDULE.FIRE":
		return buildFlowScheduleIDNative(name, nativeOpFlowScheduleFire, args)
	case "FLOW.SCHEDULE.PAUSE":
		return buildFlowScheduleIDNative(name, nativeOpFlowSchedulePause, args)
	case "FLOW.SCHEDULE.RESUME":
		return buildFlowScheduleIDNative(name, nativeOpFlowScheduleResume, args)
	case "FLOW.SCHEDULE.DELETE":
		return buildFlowScheduleIDNative(name, nativeOpFlowScheduleDelete, args)
	case "FLOW.SCHEDULE.FIRE_DUE":
		return buildFlowScheduleOptionsNative(name, nativeOpFlowScheduleFireDue, args, false)
	case "FLOW.SCHEDULE.LIST":
		return buildFlowScheduleOptionsNative(name, nativeOpFlowScheduleList, args, false)
	case "FLOW.EFFECT.RESERVE":
		return buildFlowAdminNative(name, nativeOpFlowEffectReserve, args, "id")
	case "FLOW.EFFECT.CONFIRM":
		return buildFlowAdminNative(name, nativeOpFlowEffectConfirm, args, "id")
	case "FLOW.EFFECT.FAIL":
		return buildFlowAdminNative(name, nativeOpFlowEffectFail, args, "id")
	case "FLOW.EFFECT.COMPENSATE":
		return buildFlowAdminNative(name, nativeOpFlowEffectCompensate, args, "id")
	case "FLOW.EFFECT.GET":
		return buildFlowAdminNative(name, nativeOpFlowEffectGet, args, "id")
	case "FLOW.GOVERNANCE.LEDGER":
		return buildFlowAdminNative(name, nativeOpFlowGovernanceLedger, args, "id")
	case "FLOW.APPROVAL.REQUEST":
		return buildFlowAdminNative(name, nativeOpFlowApprovalRequest, args, "id")
	case "FLOW.APPROVAL.APPROVE":
		return buildFlowAdminNative(name, nativeOpFlowApprovalApprove, args, "id")
	case "FLOW.APPROVAL.REJECT":
		return buildFlowAdminNative(name, nativeOpFlowApprovalReject, args, "id")
	case "FLOW.APPROVAL.GET":
		return buildFlowAdminNative(name, nativeOpFlowApprovalGet, args, "id")
	case "FLOW.APPROVAL.LIST":
		return buildFlowAdminNative(name, nativeOpFlowApprovalList, args)
	case "FLOW.GOVERNANCE.OVERVIEW":
		return buildFlowAdminNative(name, nativeOpFlowGovernanceOverview, args)
	case "FLOW.CIRCUIT.OPEN":
		return buildFlowAdminNative(name, nativeOpFlowCircuitOpen, args, "scope")
	case "FLOW.CIRCUIT.CLOSE":
		return buildFlowAdminNative(name, nativeOpFlowCircuitClose, args, "scope")
	case "FLOW.CIRCUIT.GET":
		return buildFlowAdminNative(name, nativeOpFlowCircuitGet, args, "scope")
	case "FLOW.BUDGET.RESERVE":
		return buildFlowAdminNative(name, nativeOpFlowBudgetReserve, args, "scope")
	case "FLOW.BUDGET.COMMIT":
		return buildFlowAdminNative(name, nativeOpFlowBudgetCommit, args, "scope")
	case "FLOW.BUDGET.RELEASE":
		return buildFlowAdminNative(name, nativeOpFlowBudgetRelease, args, "scope")
	case "FLOW.BUDGET.GET":
		return buildFlowAdminNative(name, nativeOpFlowBudgetGet, args, "scope")
	case "FLOW.BUDGET.LIST":
		return buildFlowAdminNative(name, nativeOpFlowBudgetList, args)
	case "FLOW.LIMIT.LEASE":
		return buildFlowAdminNative(name, nativeOpFlowLimitLease, args, "scope")
	case "FLOW.LIMIT.SPEND":
		return buildFlowAdminNative(name, nativeOpFlowLimitSpend, args, "scope")
	case "FLOW.LIMIT.RELEASE":
		return buildFlowAdminNative(name, nativeOpFlowLimitRelease, args, "scope")
	case "FLOW.LIMIT.GET":
		return buildFlowAdminNative(name, nativeOpFlowLimitGet, args, "scope")
	case "FLOW.LIMIT.LIST":
		return buildFlowAdminNative(name, nativeOpFlowLimitList, args)
	default:
		return nativeCommand{}, false, nil
	}
}

func hasFlowCommandOnlyOption(name string, args []any) bool {
	for _, arg := range args {
		token := strings.ToUpper(asString(arg))
		if token == "INDEXED_STATE_META" {
			return true
		}
		if name != "FLOW.SEARCH" && token == "STATE_META" {
			return true
		}
	}
	return false
}

func buildFlowCreateManyNative(args []any) (nativeCommand, bool, error) {
	if len(args) < 2 {
		return nativeCommand{}, false, nil
	}
	if _, token, ok := flowItemMarker(args[1:]); ok && token == "ITEMS_EXT" {
		return buildFlowCreateManyExtendedNative(args)
	}
	wirePartition := asString(args[0])
	opts, itemStart, ok := parseFlowOptionsUntilItems(args[1:])
	if !ok || opts.itemsToken != "ITEMS" {
		return nativeCommand{}, false, nil
	}
	itemArgs := args[1+itemStart:]
	mixed := wirePartition == "MIXED"
	width := 2
	if mixed {
		width = 3
	}
	if len(itemArgs) == 0 || len(itemArgs)%width != 0 {
		return nativeCommand{}, false, nil
	}
	flowType, ok := opts.stringValue("TYPE")
	if !ok {
		return nativeCommand{}, false, nil
	}
	state, ok := opts.stringValue("STATE")
	if !ok {
		return nativeCommand{}, false, nil
	}
	now, ok := opts.int64Value("NOW")
	if !ok {
		return nativeCommand{}, false, nil
	}
	runAt, ok := opts.int64Value("RUN_AT")
	if !ok {
		runAt = now
	}
	independent, ok := opts.boolMarker("INDEPENDENT")
	if !ok {
		return nativeCommand{}, false, nil
	}
	if !opts.only("TYPE", "STATE", "NOW", "RUN_AT", "INDEPENDENT") {
		return nativeCommand{}, false, nil
	}

	var kind byte
	switch {
	case mixed:
		kind = nativeCompactFlowCreateManyMixedRequest
	case wirePartition == "" || wirePartition == "AUTO" || strings.EqualFold(wirePartition, "none"):
		kind = nativeCompactFlowCreateManyRequest
	default:
		kind = nativeCompactFlowCreateManyPartitionRequest
	}
	for i := 0; i < len(itemArgs); i += width {
		if !isCompactPayloadValue(itemArgs[i]) {
			return nativeCommand{}, false, nil
		}
		if mixed {
			if !isCompactPayloadValue(itemArgs[i+1]) {
				return nativeCommand{}, false, nil
			}
		}
		if !isCompactPayloadValue(itemArgs[i+width-1]) {
			return nativeCommand{}, false, nil
		}
	}
	return nativeCommand{
		name:   "FLOW.CREATE_MANY",
		opcode: nativeOpFlowCreateMany,
		laneID: 1,
		payload: nativeFlowCreateManyPayload{
			kind: kind, flowType: flowType, state: state, partition: wirePartition,
			now: now, runAt: runAt, independent: independent,
			itemArgs: itemArgs, itemWidth: width, mixed: mixed,
		},
		flags: nativeFlagCustomPayload,
	}, true, nil
}

func buildFlowClaimDueNative(args []any) (nativeCommand, bool, error) {
	if len(args) < 1 {
		return nativeCommand{}, false, nil
	}
	flowType := args[0]
	if !isCompactPayloadValue(flowType) {
		return nativeCommand{}, false, nil
	}
	opts, ok := parseFlowOptions(args[1:])
	if !ok {
		return nativeCommand{}, false, nil
	}
	if !opts.only("STATE", "WORKER", "LEASE_MS", "LIMIT", "NOW", "PARTITION", "PARTITIONS", "RETURN", "BLOCK", "RECLAIM_EXPIRED", "RECLAIM_RATIO", "PRIORITY") {
		return nativeCommand{}, false, nil
	}
	if opts.has("NOW") {
		// The compact wire format has no explicit cutoff. Falling back preserves
		// the caller's exact NOW semantics instead of substituting server time.
		return nativeCommand{}, false, nil
	}
	worker := opts.value("WORKER")
	if !opts.has("WORKER") || !isCompactPayloadValue(worker) {
		return nativeCommand{}, false, nil
	}
	leaseMS, ok := opts.int64Value("LEASE_MS")
	if !ok {
		return nativeCommand{}, false, nil
	}
	limit, ok := opts.int64Value("LIMIT")
	if !ok {
		return nativeCommand{}, false, nil
	}
	blockMS := int64(-1)
	if opts.has("BLOCK") {
		blockMS, ok = opts.int64Value("BLOCK")
		if !ok {
			return nativeCommand{}, false, nil
		}
	}
	reclaimRatio := nativeFlowClaimDefaultReclaimRatio
	if opts.has("RECLAIM_RATIO") {
		reclaimRatio, ok = opts.int64Value("RECLAIM_RATIO")
		if !ok {
			return nativeCommand{}, false, nil
		}
	}
	priority := int64(math.MinInt64)
	if opts.has("PRIORITY") {
		priority, ok = opts.int64Value("PRIORITY")
		if !ok {
			return nativeCommand{}, false, nil
		}
	}
	returnMode, ok := compactClaimReturnMode(opts.value("RETURN"))
	if !ok {
		return nativeCommand{}, false, nil
	}
	reclaimExpired := nativeFlowClaimDefaultReclaimExpired
	if opts.has("RECLAIM_EXPIRED") {
		value, valid := opts.boolValue("RECLAIM_EXPIRED")
		if !valid {
			return nativeCommand{}, false, nil
		}
		if !value {
			reclaimExpired = 0
		}
	}
	partitionMode, partition, partitions, ok := compactPartitionValues(opts)
	if !ok {
		return nativeCommand{}, false, nil
	}
	var state any
	if opts.has("STATE") {
		state = opts.value("STATE")
		if !isCompactPayloadValue(state) {
			return nativeCommand{}, false, nil
		}
	}
	return nativeCommand{
		name:   "FLOW.CLAIM_DUE",
		opcode: nativeOpFlowClaimDue,
		laneID: 1,
		payload: nativeFlowClaimDuePayload{
			flowType: flowType, state: state, worker: worker, leaseMS: leaseMS, limit: limit,
			blockMS: blockMS, reclaimExpired: reclaimExpired, reclaimRatio: reclaimRatio,
			priority: priority, returnMode: returnMode, partitionMode: partitionMode,
			partition: partition, partitions: partitions,
		},
		flags: nativeFlagCustomPayload,
	}, true, nil
}

func buildFlowCompleteManyNative(args []any) (nativeCommand, bool, error) {
	if len(args) < 2 {
		return nativeCommand{}, false, nil
	}
	wirePartition := asString(args[0])
	opts, itemStart, ok := parseFlowOptionsUntilItems(args[1:])
	if !ok || opts.itemsToken != "ITEMS" {
		return nativeCommand{}, false, nil
	}
	if !opts.only("NOW", "INDEPENDENT") {
		return nativeCommand{}, false, nil
	}
	now, ok := opts.int64Value("NOW")
	if !ok {
		return nativeCommand{}, false, nil
	}
	itemArgs := args[1+itemStart:]
	mixed := wirePartition == "MIXED"
	width := 3
	if mixed {
		width = 4
	}
	if len(itemArgs) == 0 || len(itemArgs)%width != 0 {
		return nativeCommand{}, false, nil
	}
	independent, ok := opts.boolMarker("INDEPENDENT")
	if !ok {
		return nativeCommand{}, false, nil
	}
	for i := 0; i < len(itemArgs); i += width {
		if !isCompactPayloadValue(itemArgs[i]) {
			return nativeCommand{}, false, nil
		}
		offset := 1
		if mixed {
			if !isCompactPayloadValue(itemArgs[i+1]) {
				return nativeCommand{}, false, nil
			}
			offset = 2
		}
		if !isCompactPayloadValue(itemArgs[i+offset]) {
			return nativeCommand{}, false, nil
		}
		_, err := responseInt64(itemArgs[i+offset+1], nil)
		if err != nil {
			return nativeCommand{}, false, nil
		}
	}
	var partition any
	if !mixed {
		partition = wirePartition
	}
	return nativeCommand{
		name:   "FLOW.COMPLETE_MANY",
		opcode: nativeOpFlowCompleteMany,
		laneID: 1,
		payload: nativeFlowCompleteManyPayload{
			partition: partition, now: now, independent: independent,
			itemArgs: itemArgs, itemWidth: width, mixed: mixed,
		},
		flags: nativeFlagCustomPayload,
	}, true, nil
}
