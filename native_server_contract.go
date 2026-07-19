package ferricstore

import (
	"errors"
	"fmt"
	"math"
)

const (
	nativeDefaultResponseBytes = 64 * 1024 * 1024
	nativeMaxResponseCodecs    = 32
	nativeMaxResponseOpcodes   = 1024
)

type nativeCompactCodec uint8

const (
	nativeCodecFlowClaimJobs nativeCompactCodec = iota + 1
	nativeCodecFlowRecord
	nativeCodecFlowRecordList
	nativeCodecKVGet
	nativeCodecKVMGet
	nativeCodecOKList
	nativeCodecPipeline
)

type nativeResponseCodecs struct {
	byOpcode   map[uint16]nativeCompactCodec
	negotiated bool
}

type nativeHelloContract struct {
	maxRequestFrameBytes int
	maxResponseBytes     int
	maxPipelineCommands  int
	maxDataLanes         uint32
	connectionCredits    int
	laneCredits          int
	laneQueue            int
	responseCodecs       nativeResponseCodecs
	events               map[string]struct{}
	authRequired         bool
}

func defaultNativeHelloContract(configuredMaxResponseBytes int) nativeHelloContract {
	if configuredMaxResponseBytes <= 0 {
		configuredMaxResponseBytes = nativeDefaultResponseBytes
	}
	return nativeHelloContract{
		maxRequestFrameBytes: nativeDefaultRequestFrameBytes,
		maxResponseBytes:     configuredMaxResponseBytes,
		maxPipelineCommands:  nativeDefaultPipelineCommands,
		maxDataLanes:         1,
		connectionCredits:    nativeDefaultConnectionCredits,
		laneCredits:          nativeDefaultLaneCredits,
		laneQueue:            nativeDefaultLaneQueue,
		responseCodecs: nativeResponseCodecs{
			byOpcode: make(map[uint16]nativeCompactCodec), negotiated: true,
		},
	}
}

func constrainNativeContractForAuthentication(contract nativeHelloContract, authenticated bool) nativeHelloContract {
	if !authenticated && (contract.maxRequestFrameBytes <= 0 || contract.maxRequestFrameBytes > nativeUnauthenticatedFrameBytes) {
		contract.maxRequestFrameBytes = nativeUnauthenticatedFrameBytes
	}
	return contract
}

func (e *NativeExecutor) applyHelloContractLocked(contract nativeHelloContract) {
	configuredLanes := e.opts.ProtocolLanes
	if configuredLanes == 0 {
		configuredLanes = nativeDefaultProtocolLanes
	}
	if contract.maxDataLanes == 0 || contract.maxDataLanes > configuredLanes {
		contract.maxDataLanes = configuredLanes
	}
	e.maxRequestFrameBytes = contract.maxRequestFrameBytes
	e.maxResponseBytes = contract.maxResponseBytes
	e.maxPipelineCommands = contract.maxPipelineCommands
	e.maxDataLanes = contract.maxDataLanes
	e.responseCodecs = contract.responseCodecs
	e.nextLane.Store(0)
	e.flow = newNativeFlowController(
		contract.connectionCredits,
		contract.laneCredits,
		contract.laneQueue,
		e.opts.MaxQueuedRequests,
	)
}

func parseNativeHelloContract(value any, configuredMaxResponseBytes int) (nativeHelloContract, error) {
	contract := defaultNativeHelloContract(configuredMaxResponseBytes)
	hello, err := nativeMap(value)
	if err != nil {
		return nativeHelloContract{}, fmt.Errorf("ferricstore native HELLO response must be a map: %w", err)
	}
	rawProtocol, exists := hello["protocol"]
	if !exists || asString(rawProtocol) != "ferricstore-native" {
		return nativeHelloContract{}, fmt.Errorf("ferricstore native HELLO advertised unsupported protocol %q", asString(rawProtocol))
	}
	rawVersion, exists := hello["version"]
	version, versionErr := topologyInteger(rawVersion, "HELLO version")
	if !exists || versionErr != nil || version != int64(nativeRequestVersion) {
		return nativeHelloContract{}, fmt.Errorf("ferricstore native HELLO advertised unsupported protocol version %v", rawVersion)
	}
	if raw, exists := hello["auth_required"]; exists {
		required, ok := raw.(bool)
		if !ok {
			return nativeHelloContract{}, errors.New("ferricstore native HELLO auth_required must be boolean")
		}
		contract.authRequired = required
	}

	rawCapabilities, exists := hello["capabilities"]
	if !exists {
		return nativeHelloContract{}, errors.New("ferricstore native HELLO missing capabilities")
	}
	capabilities, err := nativeMap(rawCapabilities)
	if err != nil {
		return nativeHelloContract{}, errors.New("ferricstore native HELLO capabilities must be a map")
	}
	if err := validateNativeProtocolVersions(capabilities["protocol_versions"]); err != nil {
		return nativeHelloContract{}, err
	}
	limits, err := requiredNativeCapabilityMap(capabilities, "limits")
	if err != nil {
		return nativeHelloContract{}, err
	}
	serverResponseBytes, err := requiredPositiveNativeCapability(limits, "max_response_bytes")
	if err != nil {
		return nativeHelloContract{}, err
	}
	contract.maxResponseBytes = min(contract.maxResponseBytes, boundedNativePositiveInt(serverResponseBytes))
	if raw, ok := optionalNativeCapabilityInteger(limits, "max_frame_bytes", true); ok {
		contract.maxRequestFrameBytes = boundedNativeFrameBytes(raw)
	}
	if raw, ok := optionalNativeCapabilityInteger(limits, "max_pipeline_commands", false); ok && raw <= math.MaxInt {
		contract.maxPipelineCommands = int(raw)
	}
	if raw, ok := optionalNativeCapabilityInteger(limits, "max_lane_queue", false); ok && raw <= math.MaxInt {
		contract.laneQueue = int(raw)
	}
	if multiplexing, mapErr := optionalNativeCapabilityMap(capabilities, "multiplexing"); mapErr != nil {
		return nativeHelloContract{}, mapErr
	} else if raw, ok := optionalNativeCapabilityInteger(multiplexing, "max_lanes_per_connection", true); ok && raw <= math.MaxUint32 {
		contract.maxDataLanes = uint32(raw)
	}
	if flowControl, mapErr := optionalNativeCapabilityMap(capabilities, "flow_control"); mapErr != nil {
		return nativeHelloContract{}, mapErr
	} else {
		if raw, ok := optionalNativeCapabilityInteger(flowControl, "max_inflight_per_connection", false); ok && raw <= math.MaxInt {
			contract.connectionCredits = int(raw)
		}
		if raw, ok := optionalNativeCapabilityInteger(flowControl, "max_inflight_per_lane", false); ok && raw <= math.MaxInt {
			contract.laneCredits = int(raw)
		}
	}
	responseCodecs, err := requiredNativeCapabilityMap(capabilities, "response_codecs")
	if err != nil {
		return nativeHelloContract{}, err
	}
	contract.responseCodecs, err = parseNativeResponseCodecs(responseCodecs)
	if err != nil {
		return nativeHelloContract{}, fmt.Errorf("ferricstore native HELLO response codecs: %w", err)
	}
	contract.events, err = parseNativeEventCapabilities(capabilities["events"])
	if err != nil {
		return nativeHelloContract{}, err
	}
	return contract, nil
}

func parseNativeEventCapabilities(value any) (map[string]struct{}, error) {
	if value == nil {
		return nil, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, errors.New("ferricstore native HELLO events capability must be an array")
	}
	if len(items) > 64 {
		return nil, errors.New("ferricstore native HELLO advertises too many events")
	}
	events := make(map[string]struct{}, len(items))
	for _, item := range items {
		name, ok := commandText(item)
		if !ok || name == "" {
			return nil, errors.New("ferricstore native HELLO event names must be non-empty strings")
		}
		events[name] = struct{}{}
	}
	return events, nil
}

func (c nativeHelloContract) supportsEvents(names []string) bool {
	for _, name := range names {
		if _, supported := c.events[name]; !supported {
			return false
		}
	}
	return len(names) > 0
}

func parseNativeResponseCodecs(value any) (nativeResponseCodecs, error) {
	responseCodecs, err := nativeMap(value)
	if err != nil {
		return nativeResponseCodecs{}, errors.New("expected a map")
	}
	typed, ok := responseCodecs["typed_value"].(bool)
	if !ok || !typed {
		return nativeResponseCodecs{}, errors.New("typed_value must be true")
	}
	table, err := requiredNativeCapabilityMap(responseCodecs, "compact_response_opcodes")
	if err != nil {
		return nativeResponseCodecs{}, err
	}
	if len(table) > nativeMaxResponseCodecs {
		return nativeResponseCodecs{}, fmt.Errorf("too many compact response codecs: %d", len(table))
	}
	out := nativeResponseCodecs{byOpcode: make(map[uint16]nativeCompactCodec), negotiated: true}
	advertised := make(map[uint16]string)
	count := 0
	for name, rawOpcodes := range table {
		codec, supported := nativeCompactCodecByName(name)
		opcodes, ok := rawOpcodes.([]any)
		if !ok {
			return nativeResponseCodecs{}, fmt.Errorf("compact response codec %q opcodes must be an array", name)
		}
		for _, rawOpcode := range opcodes {
			opcode, err := topologyInteger(rawOpcode, "compact response opcode")
			if err != nil || opcode < 0 || opcode > math.MaxUint16 {
				return nativeResponseCodecs{}, fmt.Errorf("compact response codec %q contains invalid opcode %v", name, rawOpcode)
			}
			count++
			if count > nativeMaxResponseOpcodes {
				return nativeResponseCodecs{}, errors.New("too many compact response opcodes")
			}
			key := uint16(opcode)
			if previous, duplicate := advertised[key]; duplicate {
				return nativeResponseCodecs{}, fmt.Errorf(
					"compact response opcode %#x is advertised by both %q and %q", key, previous, name,
				)
			}
			advertised[key] = name
			if supported {
				out.byOpcode[key] = codec
			}
		}
	}
	return out, nil
}

func nativeCompactCodecByName(name string) (nativeCompactCodec, bool) {
	switch name {
	case "flow_claim_jobs_v1":
		return nativeCodecFlowClaimJobs, true
	case "flow_record_v1":
		return nativeCodecFlowRecord, true
	case "flow_record_list_v1":
		return nativeCodecFlowRecordList, true
	case "kv_get_v1":
		return nativeCodecKVGet, true
	case "kv_mget_v1":
		return nativeCodecKVMGet, true
	case "ok_list_v1":
		return nativeCodecOKList, true
	case "pipeline_v1":
		return nativeCodecPipeline, true
	default:
		return 0, false
	}
}

func validateNativeProtocolVersions(value any) error {
	versions, ok := value.([]any)
	if !ok {
		return errors.New("ferricstore native HELLO protocol_versions must be an array")
	}
	for _, raw := range versions {
		version, err := topologyInteger(raw, "protocol version")
		if err == nil && version == int64(nativeRequestVersion) {
			return nil
		}
	}
	return errors.New("ferricstore native HELLO does not support protocol version 1")
}

func requiredNativeCapabilityMap(mapping map[string]any, key string) (map[string]any, error) {
	value, exists := mapping[key]
	if !exists {
		return nil, fmt.Errorf("missing %s capability", key)
	}
	result, err := nativeMap(value)
	if err != nil {
		return nil, fmt.Errorf("%s capability must be a map", key)
	}
	return result, nil
}

func optionalNativeCapabilityMap(mapping map[string]any, key string) (map[string]any, error) {
	value, exists := mapping[key]
	if !exists || value == nil {
		return nil, nil
	}
	result, err := nativeMap(value)
	if err != nil {
		return nil, fmt.Errorf("%s capability must be a map", key)
	}
	return result, nil
}

func requiredPositiveNativeCapability(mapping map[string]any, key string) (int64, error) {
	value, exists := mapping[key]
	if !exists {
		return 0, fmt.Errorf("missing %s capability", key)
	}
	number, err := topologyInteger(value, key)
	if err != nil || number <= 0 {
		return 0, fmt.Errorf("%s capability must be a positive integer", key)
	}
	return number, nil
}

func optionalNativeCapabilityInteger(mapping map[string]any, key string, positive bool) (int64, bool) {
	if mapping == nil {
		return 0, false
	}
	number, err := topologyInteger(mapping[key], key)
	return number, err == nil && ((!positive && number >= 0) || (positive && number > 0))
}

func boundedNativePositiveInt(value int64) int {
	if value > math.MaxInt {
		return math.MaxInt
	}
	return int(value)
}
