package ferricstore

func nativeHelloPayload(clientName string, options ...any) map[string]any {
	name := nativeClientName(clientName)
	codecs := []any{"flow_query_result_v1"}
	if len(options) > 0 && nativeOptionsAdvertiseCodec(options[0], "pubsub_batch_v1", nativeOpEvent) {
		codecs = append(codecs, "pubsub_batch_v1")
	}
	return map[string]any{
		"client_name":             name,
		"driver_name":             name,
		"compression":             "none",
		"compact_flow_responses":  false,
		"compact_response_codecs": codecs,
	}
}

func nativeOptionsAdvertiseCodec(value any, codec string, opcode uint16) bool {
	options, err := nativeMap(value)
	if err != nil {
		return false
	}
	responseCodecs, err := nativeMap(options["response_codecs"])
	if err != nil {
		return false
	}
	opcodes, err := nativeMap(responseCodecs["compact_response_opcodes"])
	if err != nil {
		return false
	}
	items, ok := opcodes[codec].([]any)
	if !ok {
		return false
	}
	for _, item := range items {
		value, err := topologyInteger(item, "OPTIONS compact response opcode")
		if err == nil && value == int64(opcode) {
			return true
		}
	}
	return false
}
