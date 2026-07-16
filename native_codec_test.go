package ferricstore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestDecodeNativeValueRejectsExcessiveNesting(t *testing.T) {
	encoded := []byte{3, 0, 0, 0, 0, 0, 0, 0, 1}
	for i := 0; i < 80; i++ {
		encoded = append([]byte{5, 0, 0, 0, 1}, encoded...)
	}
	if _, _, err := decodeNativeValue(encoded); err == nil {
		t.Fatal("expected deeply nested native value to be rejected")
	}
}

func TestDecodeNativeValueUsesAggregateItemBudget(t *testing.T) {
	encoded, err := encodeNativeValue([]any{int64(1), int64(2)})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := decodeNativeValueWithLimits(encoded, 64, 2); err == nil {
		t.Fatal("expected aggregate native item budget to include nested values")
	}
}

func TestNativeValueCodecRoundTrip(t *testing.T) {
	input := map[string]any{
		"command": []byte("PING"),
		"args": []any{
			[]byte("hello"),
			int64(42),
			true,
			nil,
			[]any{[]byte("nested")},
			map[string]any{"field": []byte("value")},
		},
	}
	encoded, err := encodeNativeValue(input)
	if err != nil {
		t.Fatal(err)
	}
	decoded, rest, err := decodeNativeValue(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 0 {
		t.Fatalf("unexpected trailing bytes: %d", len(rest))
	}
	if !reflect.DeepEqual(decoded, input) {
		t.Fatalf("decoded mismatch:\nwant %#v\ngot  %#v", input, decoded)
	}
}

func TestNativeCompactResponseDecoders(t *testing.T) {
	okValue, err := decodeNativeCompactOKList([]byte{nativeCompactOKList, 0, 0, 0, 1})
	if err != nil {
		t.Fatal(err)
	}
	if asString(okValue) != "OK" {
		t.Fatalf("unexpected OK value: %#v", okValue)
	}
	if !isOK([]any{[]byte("ok")}) {
		t.Fatalf("expected lowercase compact OK list to be accepted")
	}
	manyOKValue, err := decodeNativeCompactOKList([]byte{nativeCompactOKList, 0, 0, 1, 244})
	if err != nil {
		t.Fatal(err)
	}
	if asString(manyOKValue) != "OK" {
		t.Fatalf("unexpected many OK value: %#v", manyOKValue)
	}

	getBody := append([]byte{nativeCompactKVGet, 1, 0, 0, 0, 5}, []byte("value")...)
	getValue, err := decodeNativeCompactKVGet(getBody)
	if err != nil {
		t.Fatal(err)
	}
	if asString(getValue) != "value" {
		t.Fatalf("unexpected GET value: %#v", getValue)
	}

	var mget bytes.Buffer
	mget.WriteByte(nativeCompactKVMGet)
	_ = binary.Write(&mget, binary.BigEndian, uint32(2))
	mget.WriteByte(1)
	_ = binary.Write(&mget, binary.BigEndian, uint32(1))
	mget.WriteByte('a')
	mget.WriteByte(0)
	mgetValue, err := decodeNativeCompactKVMGet(mget.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(mgetValue) != 2 || asString(mgetValue[0]) != "a" || mgetValue[1] != nil {
		t.Fatalf("unexpected MGET value: %#v", mgetValue)
	}

	var claim bytes.Buffer
	claim.WriteByte(nativeCompactFlowClaimJobs)
	_ = binary.Write(&claim, binary.BigEndian, uint32(1))
	writeCompactBinary(&claim, []byte("flow-1"))
	writeCompactOptionalBinary(&claim, []byte("partition-1"))
	writeCompactBinary(&claim, []byte("lease-1"))
	_ = binary.Write(&claim, binary.BigEndian, uint64(7))
	attrs, err := encodeNativeValue(map[string]any{"tenant": []byte("acme")})
	if err != nil {
		t.Fatal(err)
	}
	claim.Write(attrs)
	claimValue, err := decodeNativeCompactClaimJobs(claim.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := claimedItemsFromNative(claimValue, RawCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("unexpected claim count: %#v", claimed)
	}
	if claimed[0].ID != "flow-1" || claimed[0].PartitionKey != "partition-1" || claimed[0].LeaseToken != "lease-1" || claimed[0].FencingToken != 7 {
		t.Fatalf("unexpected compact claim item: %#v", claimed[0])
	}
	if asString(claimed[0].Attributes["tenant"]) != "acme" {
		t.Fatalf("unexpected compact claim attrs: %#v", claimed[0].Attributes)
	}
}

func TestNativeCompactClaimJobsDecodesStateWithoutAttributes(t *testing.T) {
	var claim bytes.Buffer
	claim.WriteByte(nativeCompactFlowClaimJobs)
	_ = binary.Write(&claim, binary.BigEndian, uint32(1))
	writeCompactBinary(&claim, []byte("flow-1"))
	writeCompactOptionalBinary(&claim, []byte("partition-1"))
	writeCompactBinary(&claim, []byte("lease-1"))
	_ = binary.Write(&claim, binary.BigEndian, uint64(7))
	writeCompactOptionalBinary(&claim, []byte("retrying"))

	value, err := decodeNativeCompactClaimJobs(claim.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(value) != 1 {
		t.Fatalf("unexpected compact claim count: %#v", value)
	}
	if value[0].RunState != "retrying" || value[0].Attributes != nil {
		t.Fatalf("unexpected state-only compact claim: %#v", value[0])
	}
}

func TestNativeCompactClaimJobsRejectsFencingOverflow(t *testing.T) {
	var claim bytes.Buffer
	claim.WriteByte(nativeCompactFlowClaimJobs)
	_ = binary.Write(&claim, binary.BigEndian, uint32(1))
	writeCompactBinary(&claim, []byte("flow-1"))
	writeCompactOptionalBinary(&claim, nil)
	writeCompactBinary(&claim, []byte("lease-1"))
	_ = binary.Write(&claim, binary.BigEndian, uint64(math.MaxInt64)+1)

	if _, err := decodeNativeCompactClaimJobs(claim.Bytes()); err == nil {
		t.Fatal("compact claim accepted a fencing token above int64")
	}
}

func TestNativeFlowCompactCommandBuilders(t *testing.T) {
	claim, err := buildNativeCommand([]any{
		"FLOW.CLAIM_DUE",
		"email",
		"STATE", "queued",
		"WORKER", "worker-1",
		"LEASE_MS", int64(30_000),
		"LIMIT", int64(500),
		"PARTITIONS", int64(2), "p1", "p2",
		"RETURN", "JOBS_COMPACT_ATTRS",
	})
	if err != nil {
		t.Fatal(err)
	}
	if claim.opcode != nativeOpFlowClaimDue || claim.flags != nativeFlagCustomPayload {
		t.Fatalf("unexpected compact claim command: %#v", claim)
	}
	if body := encodeNativeCustomPayloadForTest(t, claim.payload); len(body) == 0 || body[0] != nativeCompactFlowClaimDueRequest {
		t.Fatalf("unexpected compact claim payload: %#v", claim.payload)
	}

	create, err := buildNativeCommand([]any{
		"FLOW.CREATE_MANY",
		"MIXED",
		"TYPE", "email",
		"STATE", "queued",
		"NOW", int64(1),
		"RUN_AT", int64(1),
		"INDEPENDENT", true,
		"ITEMS",
		"flow-1", "p1", []byte("payload"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if create.opcode != nativeOpFlowCreateMany || create.flags != nativeFlagCustomPayload {
		t.Fatalf("unexpected compact create command: %#v", create)
	}
	if body := encodeNativeCustomPayloadForTest(t, create.payload); len(body) == 0 || body[0] != nativeCompactFlowCreateManyMixedRequest {
		t.Fatalf("unexpected compact create payload: %#v", create.payload)
	}

	complete, err := buildNativeCommand([]any{
		"FLOW.COMPLETE_MANY",
		"MIXED",
		"NOW", int64(1),
		"INDEPENDENT", true,
		"ITEMS",
		"flow-1", "p1", "lease-1", int64(7),
	})
	if err != nil {
		t.Fatal(err)
	}
	if complete.opcode != nativeOpFlowCompleteMany || complete.flags != nativeFlagCustomPayload {
		t.Fatalf("unexpected compact complete command: %#v", complete)
	}
	if body := encodeNativeCustomPayloadForTest(t, complete.payload); len(body) == 0 || body[0] != nativeCompactFlowCompleteManyOKRequest {
		t.Fatalf("unexpected compact complete payload: %#v", complete.payload)
	}
}

func TestNativeFlowClaimDueWithExplicitNowFallsBackToCommandExec(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.CLAIM_DUE",
		"email",
		"WORKER", "worker-1",
		"LEASE_MS", int64(30_000),
		"LIMIT", int64(1),
		"NOW", nowMS(),
		"RETURN", "JOBS_COMPACT",
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpCommandExec || command.flags != 0 {
		t.Fatalf("explicit NOW must preserve exact server command semantics, got %#v", command)
	}
}

func TestNativeFlowClaimDueCompactPreservesDefaultsAndExplicitZeros(t *testing.T) {
	base := []any{
		"FLOW.CLAIM_DUE",
		"email",
		"WORKER", "worker-1",
		"LEASE_MS", int64(30_000),
		"LIMIT", int64(1),
		"RETURN", "JOBS_COMPACT",
	}
	tests := []struct {
		name           string
		options        []any
		reclaimExpired byte
		reclaimRatio   int64
	}{
		{name: "omitted defaults", reclaimExpired: 1, reclaimRatio: 25},
		{name: "explicit zero values", options: []any{"RECLAIM_EXPIRED", false, "RECLAIM_RATIO", int64(0)}, reclaimExpired: 0, reclaimRatio: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append(append([]any(nil), base...), tt.options...)
			command, err := buildNativeCommand(args)
			if err != nil {
				t.Fatal(err)
			}
			expired, ratio := decodeCompactClaimReclaimOptions(t, command.payload)
			if expired != tt.reclaimExpired || ratio != tt.reclaimRatio {
				t.Fatalf("compact reclaim options = (%d, %d), want (%d, %d)", expired, ratio, tt.reclaimExpired, tt.reclaimRatio)
			}
		})
	}
}

func TestClaimJobsTypedCompactUsesServerOmissionDefaults(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	frames := make(chan nativeFrame, 1)
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			serverDone <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			serverDone <- err
			return
		}
		request, err := readNativeRequestFrame(reader)
		if err != nil {
			serverDone <- err
			return
		}
		frames <- request
		serverDone <- writeNativeTestResponse(writer, request, nativeStatusOK, nil)
	}()

	client := NewClient(listener.Addr().String(), WithNativeOptions(
		WithNativeTimeout(time.Second),
		WithNativeHeartbeat(0, 0),
		WithNativeReconnect(0),
	))
	defer func() { _ = client.Close() }()
	if _, err := client.ClaimJobs(context.Background(), ClaimDueOptions{Type: "email", Worker: "worker-1"}); err != nil {
		t.Fatal(err)
	}
	frame := <-frames
	if frame.opcode != nativeOpFlowClaimDue || frame.flags != nativeFlagCustomPayload {
		t.Fatalf("ClaimJobs did not use compact Flow request: %#v", frame)
	}
	expired, ratio := decodeCompactClaimReclaimOptions(t, frame.body)
	if expired != 1 || ratio != 25 {
		t.Fatalf("typed compact reclaim options = (%d, %d), want (1, 25)", expired, ratio)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func decodeCompactClaimReclaimOptions(t *testing.T, raw any) (byte, int64) {
	t.Helper()
	payload, ok := raw.([]byte)
	if !ok {
		payload = encodeNativeCustomPayloadForTest(t, raw)
	}
	if len(payload) == 0 || payload[0] != nativeCompactFlowClaimDueRequest {
		t.Fatalf("unexpected compact claim payload: %#v", raw)
	}
	offset := 1
	var err error
	for _, optional := range []bool{false, true, false} {
		var next int
		if optional {
			_, next, err = readNativeCompactOptionalBinary(payload, offset)
		} else {
			_, next, err = readNativeCompactBinary(payload, offset)
		}
		if err != nil {
			t.Fatal(err)
		}
		offset = next
	}
	offset += 8 + 8 + 8 // lease, limit, block
	if len(payload) < offset+1+8 {
		t.Fatalf("compact claim payload is truncated: %x", payload)
	}
	return payload[offset], int64(binary.BigEndian.Uint64(payload[offset+1 : offset+9]))
}

func TestCreateManyCompactRejectsPerItemAttributes(t *testing.T) {
	if createManyCompactEligible(CreateManyOptions{
		Items: []CreateItem{{ID: "flow-1", Attributes: map[string]any{"tenant": "acme"}}},
	}) {
		t.Fatal("compact create_many would discard per-item attributes")
	}
}

func TestNativeFlowClaimDueWithMalformedNumberFallsBackToCommandExec(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.CLAIM_DUE",
		"email",
		"WORKER", "worker-1",
		"LEASE_MS", int64(30_000),
		"LIMIT", "not-a-number",
		"RETURN", "JOBS_COMPACT",
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpCommandExec || command.flags != 0 {
		t.Fatalf("malformed numeric option must reach server validation, got %#v", command)
	}
}

func TestNativeBlockingMillisecondsBudgetAvoidsDurationOverflow(t *testing.T) {
	budget := nativeBlockingMillisecondsBudget(math.MaxInt64)
	if !budget.disableDefault || budget.extension != 0 {
		t.Fatalf("overflowing blocking duration must disable the default timeout, got %#v", budget)
	}
}

func TestNativeFastPathsFallBackForUnsupportedArguments(t *testing.T) {
	tests := []struct {
		name string
		args []any
	}{
		{name: "ping extra argument", args: []any{"PING", "one", "two"}},
		{name: "get extra argument", args: []any{"GET", "key", "unexpected"}},
		{
			name: "future schedule option",
			args: []any{
				"FLOW.SCHEDULE.CREATE", "schedule-1",
				"KIND", "once",
				"FUTURE_OPTION", "preserve-me",
			},
		},
		{
			name: "nil schedule boolean",
			args: []any{"FLOW.SCHEDULE.CREATE", "schedule-1", "OVERWRITE", nil},
		},
		{
			name: "nil compact claim boolean",
			args: []any{
				"FLOW.CLAIM_DUE", "email",
				"WORKER", "worker-1",
				"LEASE_MS", int64(30_000),
				"LIMIT", int64(1),
				"RECLAIM_EXPIRED", nil,
				"RETURN", "JOBS_COMPACT",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			command, err := buildNativeCommand(tt.args)
			if err != nil {
				t.Fatal(err)
			}
			if command.opcode != nativeOpCommandExec || command.flags != 0 {
				t.Fatalf("unsupported fast-path input must use COMMAND_EXEC, got %#v", command)
			}
			payload := command.payload.(map[string]any)
			if payload["command"] != strings.ToUpper(asString(tt.args[0])) {
				t.Fatalf("unexpected fallback payload: %#v", payload)
			}
		})
	}
}

func TestNativeFlowCompleteManyFallsBackWhenResultIsPresent(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.COMPLETE_MANY",
		"MIXED",
		"NOW", int64(1),
		"INDEPENDENT", true,
		"RESULT", []byte("ok"),
		"ITEMS",
		"flow-1", "p1", "lease-1", int64(7),
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpCommandExec || command.flags != 0 {
		t.Fatalf("expected generic fallback for result-bearing complete_many, got %#v", command)
	}
}

func TestNativeFlowStateMetaAndStepBuilders(t *testing.T) {
	start, err := buildNativeCommand([]any{
		"FLOW.START_AND_CLAIM", "f1",
		"TYPE", "order",
		"INITIAL_STATE", "reserve",
		"WORKER", "worker-1",
		"LEASE_MS", int64(30_000),
		"PAYLOAD", []byte("payload"),
		"PARTITION", "tenant:1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if start.opcode != nativeOpFlowStartAndClaim || start.laneID != 1 {
		t.Fatalf("unexpected start_and_claim routing: %#v", start)
	}
	startPayload := start.payload.(map[string]any)
	if startPayload["id"] != "f1" || startPayload["type"] != "order" || startPayload["initial_state"] != "reserve" {
		t.Fatalf("unexpected start_and_claim payload: %#v", startPayload)
	}

	step, err := buildNativeCommand([]any{
		"FLOW.STEP_CONTINUE", "f1", "lease-1", "reserve", "charge",
		"FENCING", int64(7),
		"LEASE_MS", int64(45_000),
		"PAYLOAD", []byte("next"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if step.opcode != nativeOpFlowStepContinue || step.laneID != 1 {
		t.Fatalf("unexpected step_continue routing: %#v", step)
	}
	stepPayload := step.payload.(map[string]any)
	if stepPayload["id"] != "f1" || stepPayload["lease_token"] != "lease-1" || stepPayload["from_state"] != "reserve" || stepPayload["to_state"] != "charge" {
		t.Fatalf("unexpected step_continue payload: %#v", stepPayload)
	}
}

func TestNativeFlowStateMetaFallsBackToCommandExec(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.START_AND_CLAIM", "f1",
		"TYPE", "order",
		"INITIAL_STATE", "reserve",
		"WORKER", "worker-1",
		"LEASE_MS", int64(30_000),
		"STATE_META", "version", int64(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpCommandExec || command.laneID != 1 {
		t.Fatalf("expected command_exec fallback for state_meta mutation, got %#v", command)
	}
	payload := command.payload.(map[string]any)
	if payload["command"] != "FLOW.START_AND_CLAIM" {
		t.Fatalf("unexpected command_exec payload: %#v", payload)
	}
}

func TestNativeFlowRunStepsManyBuilder(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.RUN_STEPS_MANY",
		"TYPE", "order",
		"STATES", []string{"reserve", "charge", "email"},
		"WORKER", "worker-1",
		"LEASE_MS", int64(30_000),
		"NOW", int64(123),
		"RESULT", []byte("ok"),
		"ITEMS", []map[string]string{{"id": "f1", "partition_key": "p1"}, {"id": "f2"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpFlowRunStepsMany || command.laneID != 1 {
		t.Fatalf("unexpected run_steps_many routing: %#v", command)
	}
	payload := command.payload.(map[string]any)
	if payload["type"] != "order" || payload["worker"] != "worker-1" || !reflect.DeepEqual(payload["states"], []string{"reserve", "charge", "email"}) {
		t.Fatalf("unexpected run_steps_many payload: %#v", payload)
	}
	if _, ok := payload["items"].([]map[string]string); !ok {
		t.Fatalf("unexpected run_steps_many items payload: %#v", payload["items"])
	}
}

func TestNativeFlowSearchFallsBackToCommandExec(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.SEARCH",
		"TYPE", "order",
		"STATE", "completed",
		"PARTITION", "tenant:1",
		"COUNT", 10,
		"REV", true,
		"CONSISTENT_PROJECTION", true,
		"ATTRIBUTE", "tenant", "acme",
		"STATE_META", "completed", "version", 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpCommandExec || command.laneID != 1 {
		t.Fatalf("expected command_exec fallback for flow search, got %#v", command)
	}
	payload := command.payload.(map[string]any)
	if payload["command"] != "FLOW.SEARCH" {
		t.Fatalf("unexpected flow search command_exec payload: %#v", payload)
	}
	wantArgs := []any{
		"TYPE", "order",
		"STATE", "completed",
		"PARTITION", "tenant:1",
		"COUNT", 10,
		"REV", true,
		"CONSISTENT_PROJECTION", true,
		"ATTRIBUTE", "tenant", "acme",
		"STATE_META", "completed", "version", 3,
	}
	if !reflect.DeepEqual(payload["args"], wantArgs) {
		t.Fatalf("unexpected flow search command_exec args: %#v", payload)
	}
}

func TestNativeFlowPolicySetBuilderIncludesIndexes(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.POLICY.SET", "order",
		"INDEXED_ATTRIBUTES", []string{"tenant", "region"},
		"INDEXED_STATE_META", "version",
		"MAX_RETRIES", 5,
		"BACKOFF", "exponential",
		"BASE_MS", 100,
		"STATE", "queued",
		"MODE", "FIFO",
		"MAX_RETRIES", 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpFlowPolicySet || command.laneID != 1 {
		t.Fatalf("unexpected policy set routing: %#v", command)
	}
	payload := command.payload.(map[string]any)
	if payload["type"] != "order" || payload["indexed_state_meta"] != "version" {
		t.Fatalf("unexpected policy set payload: %#v", payload)
	}
	if !reflect.DeepEqual(payload["indexed_attributes"], []string{"tenant", "region"}) {
		t.Fatalf("unexpected indexed attributes: %#v", payload)
	}
	retry := payload["retry"].(map[string]any)
	if retry["max_retries"] != 5 {
		t.Fatalf("unexpected retry payload: %#v", retry)
	}
	backoff := retry["backoff"].(map[string]any)
	if backoff["kind"] != "exponential" || backoff["base_ms"] != 100 {
		t.Fatalf("unexpected backoff payload: %#v", retry)
	}
	states := payload["states"].(map[string]any)
	queued := states["queued"].(map[string]any)
	if queued["mode"] != "fifo" {
		t.Fatalf("unexpected state mode payload: %#v", queued)
	}
	queuedRetry := queued["retry"].(map[string]any)
	if queuedRetry["max_retries"] != 2 {
		t.Fatalf("unexpected state retry payload: %#v", queued)
	}
}

func TestNativeFlowPolicySetBuilderRejectsStateLevelIndexes(t *testing.T) {
	tests := []struct {
		name string
		args []any
		want string
	}{
		{
			name: "indexed attributes",
			args: []any{
				"FLOW.POLICY.SET", "order",
				"STATE", "queued",
				"INDEXED_ATTRIBUTES", []string{"tenant"},
			},
			want: "ERR flow indexed_attributes is type-level only",
		},
		{
			name: "indexed state meta",
			args: []any{
				"FLOW.POLICY.SET", "order",
				"STATE", "queued",
				"INDEXED_STATE_META", "version",
			},
			want: "ERR flow indexed_state_meta is type-level only",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildNativeCommand(tt.args)
			if err == nil {
				t.Fatal("expected state-level index option to fail")
			}
			if err.Error() != tt.want {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
