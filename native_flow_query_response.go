package ferricstore

// ownedNativeFlowQueryResponse marks a map whose ownership was transferred by
// the built-in native decoder. Typed query decoding may normalize this map in
// place; custom Executor responses deliberately do not receive this marker.
type ownedNativeFlowQueryResponse map[string]any

// ownedNativeFlowQueryResult marks a result that was validated and decoded
// directly from the negotiated compact wire representation.
type ownedNativeFlowQueryResult struct {
	result *FlowQueryResult
}

func flowQueryResponseMap(value any) (map[string]any, error) {
	if owned, ok := value.(ownedNativeFlowQueryResponse); ok {
		return map[string]any(owned), nil
	}
	return nativeMap(value)
}

func nativeResponseModeForPayload(payload any) nativeResponseDecodeMode {
	query, ok := payload.(nativeFlowQueryPayload)
	if !ok || hasFlowExplainPrefix(query.query) {
		return nativeResponseDecodeGeneric
	}
	return nativeResponseDecodeFlowQuery
}
