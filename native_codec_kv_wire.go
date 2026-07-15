package ferricstore

func writeNativeKeyCommandPayload(
	buf *nativeEncodeBuffer,
	payload nativeKeyCommandPayload,
	state *nativeEncodeState,
	depth int,
) error {
	if err := ensureNativeEncodeContainerBudget("map", 2, state.remaining); err != nil {
		return err
	}
	if err := writeNativeContainerHeader(buf, 6, 2); err != nil {
		return err
	}
	if err := writeNativeMapKey(buf, "command"); err != nil {
		return err
	}
	if err := writeNativeValue(buf, payload.command, state, depth+1); err != nil {
		return err
	}
	if err := writeNativeMapKey(buf, "args"); err != nil {
		return err
	}
	leave, err := state.enter(payload.keys, depth+1)
	if err != nil {
		return err
	}
	defer leave()
	if err := ensureNativeEncodeContainerBudget("array", len(payload.keys), state.remaining); err != nil {
		return err
	}
	if err := writeNativeContainerHeader(buf, 5, len(payload.keys)); err != nil {
		return err
	}
	for _, key := range payload.keys {
		if err := writeNativeValue(buf, key, state, depth+2); err != nil {
			return err
		}
	}
	return nil
}
