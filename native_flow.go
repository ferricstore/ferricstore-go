package ferricstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
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
	case "FLOW.CREATE_MANY":
		return buildFlowCreateManyNative(args)
	case "FLOW.CLAIM_DUE":
		return buildFlowClaimDueNative(args)
	case "FLOW.COMPLETE_MANY":
		return buildFlowCompleteManyNative(args)
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

func buildFlowAdminNative(name string, opcode uint16, args []any, leadingFields ...string) (nativeCommand, bool, error) {
	payload := map[string]any{}
	if len(args) < len(leadingFields) {
		return nativeCommand{}, true, errors.New(name + " missing required arguments")
	}
	for idx, field := range leadingFields {
		payload[field] = args[idx]
	}
	if err := appendFlowAdminOptions(payload, args[len(leadingFields):]); err != nil {
		return nativeCommand{}, true, err
	}
	return nativeCommand{name: name, opcode: opcode, laneID: 1, payload: payload}, true, nil
}

func appendFlowAdminOptions(payload map[string]any, args []any) error {
	if len(args)%2 != 0 {
		return errors.New("FLOW admin options must be key/value pairs")
	}
	for idx := 0; idx < len(args); idx += 2 {
		key, ok := flowAdminNativeField(asString(args[idx]))
		if !ok {
			continue
		}
		payload[key] = flowAdminNativeValue(key, args[idx+1])
	}
	return nil
}

func flowAdminNativeField(token string) (string, bool) {
	switch strings.ToUpper(token) {
	case "PARTITION":
		return "partition_key", true
	case "LEASE_TOKEN":
		return "lease_token", true
	case "FENCING", "FENCING_TOKEN":
		return "fencing_token", true
	case "EFFECT_KEY":
		return "effect_key", true
	case "EFFECT_TYPE":
		return "effect_type", true
	case "OPERATION_DIGEST":
		return "operation_digest", true
	case "IDEMPOTENCY_KEY":
		return "idempotency_key", true
	case "GOVERNANCE_SCOPE":
		return "governance_scope", true
	case "EXTERNAL_ID":
		return "external_id", true
	case "ERROR":
		return "error", true
	case "REASON":
		return "reason", true
	case "LATENCY_MS":
		return "latency_ms", true
	case "FLOW_ID":
		return "flow_id", true
	case "SCOPE":
		return "scope", true
	case "REQUESTED_BY":
		return "requested_by", true
	case "ASSIGNEES":
		return "assignees", true
	case "POLICY_HASH":
		return "policy_hash", true
	case "POLICY_VERSION":
		return "policy_version", true
	case "TIMEOUT_MS":
		return "timeout_ms", true
	case "EXPIRES_AT_MS":
		return "expires_at_ms", true
	case "APPROVER":
		return "approver", true
	case "STATUS":
		return "status", true
	case "OPEN_MS":
		return "open_ms", true
	case "FAILURE_THRESHOLD":
		return "failure_threshold", true
	case "AMOUNT":
		return "amount", true
	case "LIMIT":
		return "limit", true
	case "WINDOW_MS":
		return "window_ms", true
	case "RESERVATION_ID":
		return "reservation_id", true
	case "ACTUAL_AMOUNT":
		return "actual_amount", true
	case "USAGE":
		return "usage", true
	case "SHARD_ID":
		return "shard_id", true
	case "TTL_MS":
		return "ttl_ms", true
	case "NOW":
		return "now_ms", true
	case "STATE":
		return "state", true
	case "COUNT":
		return "count", true
	case "FROM_MS":
		return "from_ms", true
	case "TO_MS":
		return "to_ms", true
	case "REV":
		return "rev", true
	case "CONSISTENT_PROJECTION":
		return "consistent_projection", true
	case "DEADLINE_MS":
		return "deadline_ms", true
	default:
		return scheduleNativeField(token)
	}
}

func flowAdminNativeValue(key string, value any) any {
	switch key {
	case "rev", "consistent_projection":
		return asBool(value)
	default:
		return value
	}
}

func buildFlowScheduleCreateNative(args []any) (nativeCommand, bool, error) {
	if len(args) < 1 {
		return nativeCommand{}, true, errors.New("FLOW.SCHEDULE.CREATE requires id")
	}
	payload := map[string]any{"id": args[0]}
	if err := appendScheduleOptions(payload, args[1:]); err != nil {
		return nativeCommand{}, true, err
	}
	return nativeCommand{name: "FLOW.SCHEDULE.CREATE", opcode: nativeOpFlowScheduleCreate, laneID: 1, payload: payload}, true, nil
}

func buildFlowScheduleIDNative(name string, opcode uint16, args []any) (nativeCommand, bool, error) {
	if len(args) < 1 {
		return nativeCommand{}, true, errors.New(name + " requires id")
	}
	payload := map[string]any{"id": args[0]}
	if err := appendScheduleOptions(payload, args[1:]); err != nil {
		return nativeCommand{}, true, err
	}
	return nativeCommand{name: name, opcode: opcode, laneID: 1, payload: payload}, true, nil
}

func buildFlowScheduleOptionsNative(name string, opcode uint16, args []any, requireID bool) (nativeCommand, bool, error) {
	payload := map[string]any{}
	if requireID {
		if len(args) < 1 {
			return nativeCommand{}, true, errors.New(name + " requires id")
		}
		payload["id"] = args[0]
		args = args[1:]
	}
	if err := appendScheduleOptions(payload, args); err != nil {
		return nativeCommand{}, true, err
	}
	return nativeCommand{name: name, opcode: opcode, laneID: 1, payload: payload}, true, nil
}

func appendScheduleOptions(payload map[string]any, args []any) error {
	if len(args)%2 != 0 {
		return errors.New("FLOW.SCHEDULE options must be key/value pairs")
	}
	for idx := 0; idx < len(args); idx += 2 {
		key, ok := scheduleNativeField(asString(args[idx]))
		if !ok {
			continue
		}
		payload[key] = scheduleNativeValue(key, args[idx+1])
	}
	return nil
}

func scheduleNativeValue(key string, value any) any {
	switch key {
	case "overwrite":
		return asBool(value)
	default:
		return value
	}
}

func scheduleNativeField(token string) (string, bool) {
	switch strings.ToUpper(token) {
	case "KIND":
		return "kind", true
	case "TARGET":
		return "target", true
	case "AT_MS":
		return "at_ms", true
	case "DELAY_MS":
		return "delay_ms", true
	case "START_AT_MS":
		return "start_at_ms", true
	case "EVERY_MS":
		return "every_ms", true
	case "CRON":
		return "cron", true
	case "TIMEZONE":
		return "timezone", true
	case "NOW":
		return "now_ms", true
	case "OVERWRITE":
		return "overwrite", true
	case "OVERLAP_POLICY":
		return "overlap_policy", true
	case "OVERLAP_RETRY_MS":
		return "overlap_retry_ms", true
	case "MAX_FIRES":
		return "max_fires", true
	case "END_AT_MS":
		return "end_at_ms", true
	case "WORKER":
		return "worker", true
	case "LIMIT":
		return "limit", true
	case "LEASE_MS":
		return "lease_ms", true
	case "BLOCK", "BLOCK_MS":
		return "block_ms", true
	case "STATE":
		return "state", true
	case "TARGET_TYPE":
		return "target_type", true
	case "FROM_MS":
		return "from_ms", true
	case "TO_MS":
		return "to_ms", true
	case "COUNT":
		return "count", true
	case "DEADLINE_MS":
		return "deadline_ms", true
	default:
		return "", false
	}
}

func (c *Client) tryCreateManyNativeCompact(ctx context.Context, opt CreateManyOptions, state string, now, runAt int64, mixed bool, wirePartition string) ([]FlowRecord, bool, error) {
	native, ok := c.exec.(*NativeExecutor)
	if !ok || !createManyCompactEligible(opt) {
		return nil, false, nil
	}
	buf := bytes.NewBuffer(make([]byte, 0, createManyCompactCapacity(opt, state, wirePartition, mixed)))
	switch {
	case mixed:
		buf.WriteByte(nativeCompactFlowCreateManyMixedRequest)
	case wirePartition == "" || wirePartition == "AUTO":
		buf.WriteByte(nativeCompactFlowCreateManyRequest)
	default:
		buf.WriteByte(nativeCompactFlowCreateManyPartitionRequest)
	}
	writeCompactString(buf, opt.Type)
	writeCompactString(buf, state)
	if buf.Bytes()[0] == nativeCompactFlowCreateManyPartitionRequest {
		writeCompactOptionalString(buf, wirePartition)
	}
	writeCompactInt64(buf, now)
	writeCompactInt64(buf, runAt)
	buf.WriteByte(boolPtrMarker(opt.Independent))
	buf.WriteByte(0)
	writeCompactU32(buf, uint32(len(opt.Items)))
	for _, item := range opt.Items {
		writeCompactString(buf, item.ID)
		if mixed {
			if item.PartitionKey == "" {
				return nil, true, errors.New("mixed create_many items require partition key")
			}
			writeCompactString(buf, item.PartitionKey)
		}
		encoded, err := c.encode(item.Payload)
		if err != nil {
			return nil, true, err
		}
		payload, ok := compactBytes(encoded)
		if !ok {
			return nil, false, nil
		}
		writeCompactBinary(buf, payload)
	}
	value, err := native.request(ctx, nativeOpFlowCreateMany, 1, buf.Bytes(), nativeFlagCustomPayload)
	if err != nil {
		return nil, true, err
	}
	records, err := recordsOrNil(value, c.codec)
	return records, true, err
}

func (c *Client) tryClaimDueNativeCompact(ctx context.Context, opt ClaimDueOptions, leaseMS int64, limit int) (any, bool, error) {
	native, ok := c.exec.(*NativeExecutor)
	if !ok || !claimDueCompactEligible(opt) {
		return nil, false, nil
	}
	if opt.NowMS != 0 && absInt64(opt.NowMS-nowMS()) > 10_000 {
		return nil, false, nil
	}
	blockMS := int64(-1)
	if opt.BlockMS != nil {
		blockMS = *opt.BlockMS
	}
	reclaimRatio := int64(0)
	if opt.ReclaimRatio != nil {
		reclaimRatio = *opt.ReclaimRatio
	}
	priority := int64(math.MinInt64)
	if opt.Priority != nil {
		priority = *opt.Priority
	}
	reclaimExpired := byte(0)
	if opt.ReclaimExpired != nil && *opt.ReclaimExpired {
		reclaimExpired = 1
	}
	returnMode := byte(1)
	if opt.IncludeState {
		if boolDefault(opt.IncludeAttributes, true) {
			returnMode = 4
		} else {
			returnMode = 2
		}
	} else if boolDefault(opt.IncludeAttributes, true) {
		returnMode = 3
	}

	buf := bytes.NewBuffer(make([]byte, 0, claimDueCompactCapacity(opt)))
	buf.WriteByte(nativeCompactFlowClaimDueRequest)
	writeCompactString(buf, opt.Type)
	if opt.State != "" {
		writeCompactOptionalString(buf, opt.State)
	} else {
		writeCompactOptionalBinary(buf, nil)
	}
	writeCompactString(buf, opt.Worker)
	writeCompactInt64(buf, leaseMS)
	writeCompactInt64(buf, int64(limit))
	writeCompactInt64(buf, blockMS)
	buf.WriteByte(reclaimExpired)
	writeCompactInt64(buf, reclaimRatio)
	writeCompactInt64(buf, priority)
	buf.WriteByte(returnMode)
	writeClaimPartitions(buf, opt.PartitionKey, opt.PartitionKeys)
	value, err := native.request(ctx, nativeOpFlowClaimDue, 1, buf.Bytes(), nativeFlagCustomPayload)
	return value, true, err
}

func (c *Client) tryCompleteManyNativeCompact(ctx context.Context, opt CompleteManyOptions, now int64) ([]FlowRecord, bool, error) {
	native, ok := c.exec.(*NativeExecutor)
	if !ok || !completeManyCompactEligible(opt) {
		return nil, false, nil
	}
	mixed := opt.PartitionKey == ""
	buf := bytes.NewBuffer(make([]byte, 0, completeManyCompactCapacity(opt)))
	buf.WriteByte(nativeCompactFlowCompleteManyOKRequest)
	if mixed {
		writeCompactOptionalBinary(buf, nil)
	} else {
		writeCompactOptionalBinary(buf, []byte(opt.PartitionKey))
	}
	writeCompactInt64(buf, now)
	buf.WriteByte(boolPtrMarker(opt.Independent))
	writeCompactU32(buf, uint32(len(opt.Items)))
	for _, item := range opt.Items {
		writeCompactString(buf, item.ID)
		if mixed {
			if item.PartitionKey == "" {
				return nil, true, errors.New("FLOW.COMPLETE_MANY mixed items require partition key")
			}
			writeCompactOptionalString(buf, item.PartitionKey)
		} else {
			writeCompactOptionalBinary(buf, nil)
		}
		writeCompactString(buf, item.LeaseToken)
		writeCompactInt64(buf, item.FencingToken)
	}
	value, err := native.request(ctx, nativeOpFlowCompleteMany, 1, buf.Bytes(), nativeFlagCustomPayload)
	if err != nil {
		return nil, true, err
	}
	records, err := recordsOrNil(value, c.codec)
	return records, true, err
}

func createManyCompactEligible(opt CreateManyOptions) bool {
	return opt.Priority == nil &&
		opt.Idempotent == nil &&
		opt.RetentionTTLMS == nil &&
		len(opt.Values) == 0 &&
		len(opt.ValueRefs) == 0 &&
		len(opt.Attributes) == 0 &&
		!anyCreateItemValues(opt.Items)
}

func claimDueCompactEligible(opt ClaimDueOptions) bool {
	return opt.JobOnly &&
		len(opt.States) == 0 &&
		opt.Payload == nil &&
		opt.PayloadMaxBytes == nil &&
		len(opt.Values) == 0 &&
		opt.ValueMaxBytes == nil
}

func completeManyCompactEligible(opt CompleteManyOptions) bool {
	return opt.Result == nil &&
		opt.Payload == nil &&
		opt.TTLMS == nil &&
		namedValuesEmpty(opt.NamedValues)
}

func namedValuesEmpty(values NamedValues) bool {
	return len(values.Values) == 0 &&
		len(values.ValueRefs) == 0 &&
		len(values.DropValues) == 0 &&
		len(values.OverrideValues) == 0 &&
		len(values.AttributesMerge) == 0 &&
		len(values.AttributesDelete) == 0
}

func boolPtrMarker(value *bool) byte {
	if value == nil {
		return 0
	}
	if *value {
		return 2
	}
	return 1
}

func writeClaimPartitions(buf *bytes.Buffer, partitionKey string, partitionKeys []string) {
	switch {
	case partitionKey != "":
		buf.WriteByte(1)
		writeCompactString(buf, partitionKey)
	case len(partitionKeys) > 0:
		buf.WriteByte(2)
		writeCompactU32(buf, uint32(len(partitionKeys)))
		for _, partition := range partitionKeys {
			writeCompactString(buf, partition)
		}
	default:
		buf.WriteByte(0)
	}
}

func writeCompactString(buf *bytes.Buffer, value string) {
	writeCompactU32(buf, uint32(len(value)))
	buf.WriteString(value)
}

func writeCompactOptionalString(buf *bytes.Buffer, value string) {
	writeCompactString(buf, value)
}

func createManyCompactCapacity(opt CreateManyOptions, state, wirePartition string, mixed bool) int {
	size := 64 + len(opt.Type) + len(state) + len(wirePartition)
	for _, item := range opt.Items {
		size += 12 + len(item.ID) + compactPayloadSize(item.Payload)
		if mixed {
			size += 4 + len(item.PartitionKey)
		}
	}
	return size
}

func claimDueCompactCapacity(opt ClaimDueOptions) int {
	size := 96 + len(opt.Type) + len(opt.State) + len(opt.Worker) + len(opt.PartitionKey)
	for _, partition := range opt.PartitionKeys {
		size += 4 + len(partition)
	}
	return size
}

func completeManyCompactCapacity(opt CompleteManyOptions) int {
	size := 48 + len(opt.PartitionKey)
	mixed := opt.PartitionKey == ""
	for _, item := range opt.Items {
		size += 24 + len(item.ID) + len(item.LeaseToken)
		if mixed {
			size += 4 + len(item.PartitionKey)
		}
	}
	return size
}

func compactPayloadSize(value any) int {
	switch v := value.(type) {
	case nil:
		return 4
	case []byte:
		return 4 + len(v)
	case string:
		return 4 + len(v)
	default:
		return 64
	}
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
