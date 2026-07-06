package ferricstore

import (
	"bytes"
	"encoding/binary"
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
)

func buildFlowNativeCommand(name string, args []any) (nativeCommand, bool, error) {
	switch name {
	case "FLOW.POLICY.SET":
		return buildFlowPolicySetNative(args)
	case "FLOW.POLICY.GET":
		return buildFlowAdminNative(name, nativeOpFlowPolicyGet, args, "type")
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
	independent := opts.boolMarker("INDEPENDENT")
	if !opts.only("TYPE", "STATE", "NOW", "RUN_AT", "INDEPENDENT") {
		return nativeCommand{}, false, nil
	}

	var buf bytes.Buffer
	switch {
	case mixed:
		buf.WriteByte(nativeCompactFlowCreateManyMixedRequest)
	case wirePartition == "" || wirePartition == "AUTO" || strings.EqualFold(wirePartition, "none"):
		buf.WriteByte(nativeCompactFlowCreateManyRequest)
	default:
		buf.WriteByte(nativeCompactFlowCreateManyPartitionRequest)
	}
	writeCompactBinary(&buf, []byte(flowType))
	writeCompactBinary(&buf, []byte(state))
	if buf.Bytes()[0] == nativeCompactFlowCreateManyPartitionRequest {
		writeCompactOptionalBinary(&buf, []byte(wirePartition))
	}
	writeCompactInt64(&buf, now)
	writeCompactInt64(&buf, runAt)
	buf.WriteByte(independent)
	buf.WriteByte(0)
	writeCompactU32(&buf, uint32(len(itemArgs)/width))
	for i := 0; i < len(itemArgs); i += width {
		id, ok := compactBytes(itemArgs[i])
		if !ok {
			return nativeCommand{}, false, nil
		}
		writeCompactBinary(&buf, id)
		if mixed {
			partition, ok := compactBytes(itemArgs[i+1])
			if !ok {
				return nativeCommand{}, false, nil
			}
			writeCompactBinary(&buf, partition)
		}
		payload, ok := compactBytes(itemArgs[i+width-1])
		if !ok {
			return nativeCommand{}, false, nil
		}
		writeCompactBinary(&buf, payload)
	}
	return nativeCommand{
		name:    "FLOW.CREATE_MANY",
		opcode:  nativeOpFlowCreateMany,
		laneID:  1,
		payload: buf.Bytes(),
		flags:   nativeFlagCustomPayload,
	}, true, nil
}

func buildFlowClaimDueNative(args []any) (nativeCommand, bool, error) {
	if len(args) < 1 {
		return nativeCommand{}, false, nil
	}
	flowType, ok := compactBytes(args[0])
	if !ok {
		return nativeCommand{}, false, nil
	}
	opts, ok := parseFlowOptions(args[1:])
	if !ok {
		return nativeCommand{}, false, nil
	}
	if !opts.only("STATE", "WORKER", "LEASE_MS", "LIMIT", "NOW", "PARTITION", "PARTITIONS", "RETURN", "BLOCK", "RECLAIM_EXPIRED", "RECLAIM_RATIO", "PRIORITY") {
		return nativeCommand{}, false, nil
	}
	worker, ok := opts.bytesValue("WORKER")
	if !ok {
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
	if now, hasNow := opts.int64Value("NOW"); hasNow && absInt64(now-nowMS()) > 10_000 {
		return nativeCommand{}, false, nil
	}
	blockMS, ok := opts.int64Value("BLOCK")
	if !ok {
		blockMS = -1
	}
	reclaimRatio, ok := opts.int64Value("RECLAIM_RATIO")
	if !ok {
		reclaimRatio = 0
	}
	priority, ok := opts.int64Value("PRIORITY")
	if !ok {
		priority = math.MinInt64
	}
	returnMode, ok := compactClaimReturnMode(opts.value("RETURN"))
	if !ok {
		return nativeCommand{}, false, nil
	}
	reclaimExpired := byte(0)
	if opts.boolValue("RECLAIM_EXPIRED") {
		reclaimExpired = 1
	}
	partitionMode, partitionBody, ok := compactPartitionBody(opts)
	if !ok {
		return nativeCommand{}, false, nil
	}

	var buf bytes.Buffer
	buf.WriteByte(nativeCompactFlowClaimDueRequest)
	writeCompactBinary(&buf, flowType)
	if state, ok := opts.bytesValue("STATE"); ok {
		writeCompactOptionalBinary(&buf, state)
	} else {
		writeCompactOptionalBinary(&buf, nil)
	}
	writeCompactBinary(&buf, worker)
	writeCompactInt64(&buf, leaseMS)
	writeCompactInt64(&buf, limit)
	writeCompactInt64(&buf, blockMS)
	buf.WriteByte(reclaimExpired)
	writeCompactInt64(&buf, reclaimRatio)
	writeCompactInt64(&buf, priority)
	buf.WriteByte(returnMode)
	buf.WriteByte(partitionMode)
	buf.Write(partitionBody)
	return nativeCommand{
		name:    "FLOW.CLAIM_DUE",
		opcode:  nativeOpFlowClaimDue,
		laneID:  1,
		payload: buf.Bytes(),
		flags:   nativeFlagCustomPayload,
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
	var buf bytes.Buffer
	buf.WriteByte(nativeCompactFlowCompleteManyOKRequest)
	if mixed {
		writeCompactOptionalBinary(&buf, nil)
	} else {
		writeCompactOptionalBinary(&buf, []byte(wirePartition))
	}
	writeCompactInt64(&buf, now)
	buf.WriteByte(opts.boolMarker("INDEPENDENT"))
	writeCompactU32(&buf, uint32(len(itemArgs)/width))
	for i := 0; i < len(itemArgs); i += width {
		id, ok := compactBytes(itemArgs[i])
		if !ok {
			return nativeCommand{}, false, nil
		}
		writeCompactBinary(&buf, id)
		offset := 1
		if mixed {
			partition, ok := compactBytes(itemArgs[i+1])
			if !ok {
				return nativeCommand{}, false, nil
			}
			writeCompactOptionalBinary(&buf, partition)
			offset = 2
		} else {
			writeCompactOptionalBinary(&buf, nil)
		}
		lease, ok := compactBytes(itemArgs[i+offset])
		if !ok {
			return nativeCommand{}, false, nil
		}
		writeCompactBinary(&buf, lease)
		fencing := asInt64(itemArgs[i+offset+1])
		writeCompactInt64(&buf, fencing)
	}
	return nativeCommand{
		name:    "FLOW.COMPLETE_MANY",
		opcode:  nativeOpFlowCompleteMany,
		laneID:  1,
		payload: buf.Bytes(),
		flags:   nativeFlagCustomPayload,
	}, true, nil
}

type flowOptionSet struct {
	values     map[string]any
	partitions []any
	itemsToken string
}

func parseFlowOptionsUntilItems(args []any) (flowOptionSet, int, bool) {
	opts := flowOptionSet{values: map[string]any{}}
	for i := 0; i < len(args); {
		token := strings.ToUpper(asString(args[i]))
		if token == "ITEMS" || token == "ITEMS_EXT" {
			opts.itemsToken = token
			return opts, i + 1, true
		}
		if i+1 >= len(args) {
			return flowOptionSet{}, 0, false
		}
		opts.values[token] = args[i+1]
		i += 2
	}
	return flowOptionSet{}, 0, false
}

func parseFlowOptions(args []any) (flowOptionSet, bool) {
	opts := flowOptionSet{values: map[string]any{}}
	for i := 0; i < len(args); {
		token := strings.ToUpper(asString(args[i]))
		if token == "PARTITIONS" {
			if i+1 >= len(args) {
				return flowOptionSet{}, false
			}
			count := int(asInt64(args[i+1]))
			if count < 0 || i+2+count > len(args) {
				return flowOptionSet{}, false
			}
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
	for key := range o.values {
		if _, ok := allowed[key]; !ok {
			return false
		}
	}
	return true
}

func (o flowOptionSet) value(key string) any {
	return o.values[key]
}

func (o flowOptionSet) stringValue(key string) (string, bool) {
	value, ok := o.values[key]
	if !ok {
		return "", false
	}
	text := asString(value)
	return text, text != ""
}

func (o flowOptionSet) bytesValue(key string) ([]byte, bool) {
	value, ok := o.values[key]
	if !ok {
		return nil, false
	}
	return compactBytes(value)
}

func (o flowOptionSet) int64Value(key string) (int64, bool) {
	value, ok := o.values[key]
	if !ok {
		return 0, false
	}
	return asInt64(value), true
}

func (o flowOptionSet) boolValue(key string) bool {
	value, ok := o.values[key]
	return ok && asBool(value)
}

func (o flowOptionSet) boolMarker(key string) byte {
	value, ok := o.values[key]
	if !ok {
		return 0
	}
	if asBool(value) {
		return 2
	}
	return 1
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

func compactPartitionBody(opts flowOptionSet) (byte, []byte, bool) {
	if value, ok := opts.bytesValue("PARTITION"); ok {
		var buf bytes.Buffer
		writeCompactBinary(&buf, value)
		return 1, buf.Bytes(), true
	}
	if len(opts.partitions) > 0 {
		var buf bytes.Buffer
		writeCompactU32(&buf, uint32(len(opts.partitions)))
		for _, item := range opts.partitions {
			value, ok := compactBytes(item)
			if !ok {
				return 0, nil, false
			}
			writeCompactBinary(&buf, value)
		}
		return 2, buf.Bytes(), true
	}
	return 0, nil, true
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
		text := asString(value)
		if text == "" {
			return nil, false
		}
		return []byte(text), true
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

func writeCompactInt64(buf *bytes.Buffer, value int64) {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], uint64(value))
	buf.Write(raw[:])
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
