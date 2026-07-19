package ferricstore

func (c *Client) createManyItemMap(item CreateItem, shared CreateManyOptions) (map[string]any, error) {
	payload, err := c.encode(item.Payload)
	if err != nil {
		return nil, err
	}
	out := map[string]any{"id": item.ID, "payload": payload}
	if item.PartitionKey != "" {
		out["partition_key"] = item.PartitionKey
	}
	if err := c.putEncodedFlowValues(out, mergeValues(shared.Values, item.Values)); err != nil {
		return nil, err
	}
	if refs := mergeRefs(shared.ValueRefs, item.ValueRefs); len(refs) > 0 {
		out["value_refs"] = refs
	}
	if attributes := canonicalFlowMetadataMap(item.Attributes); len(attributes) > 0 {
		out["attributes"] = attributes
	}
	if stateMeta := canonicalFlowMetadataMap(item.StateMeta); len(stateMeta) > 0 {
		out["state_meta"] = stateMeta
	}
	maxActiveMS, err := canonicalFlowMaxActiveMS(item.MaxActiveMS)
	if err != nil {
		return nil, err
	}
	if maxActiveMS != nil {
		out["max_active_ms"] = maxActiveMS
	}
	return out, nil
}

func canonicalFlowMetadataMap(values map[string]any) map[string]any {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]any, len(values))
	for name, value := range values {
		out[canonicalFlowMetadataKey(name)] = value
	}
	return out
}

func (c *Client) childItemMap(child ChildSpec, shared SpawnChildrenOptions) (map[string]any, error) {
	payload, err := c.encode(child.Payload)
	if err != nil {
		return nil, err
	}
	out := map[string]any{"id": child.ID, "type": child.Type, "payload": payload}
	partition := child.PartitionKey
	if partition == "" {
		partition = shared.PartitionKey
	}
	if partition != "" {
		out["partition_key"] = partition
	}
	if err := c.putEncodedFlowValues(out, mergeValues(shared.Values, child.Values)); err != nil {
		return nil, err
	}
	if refs := mergeRefs(shared.ValueRefs, child.ValueRefs); len(refs) > 0 {
		out["value_refs"] = refs
	}
	if attributes := canonicalFlowMetadataMap(child.Attributes); len(attributes) > 0 {
		out["attributes"] = attributes
	}
	if stateMeta := canonicalFlowMetadataMap(child.StateMeta); len(stateMeta) > 0 {
		out["state_meta"] = stateMeta
	}
	maxActiveMS, err := canonicalFlowMaxActiveMS(child.MaxActiveMS)
	if err != nil {
		return nil, err
	}
	if maxActiveMS != nil {
		out["max_active_ms"] = maxActiveMS
	}
	return out, nil
}

func (c *Client) putEncodedFlowValues(out map[string]any, values map[string]any) error {
	if len(values) == 0 {
		return nil
	}
	encoded := make(map[string]any, len(values))
	if keys := deterministicMapKeysForCodec(values, c.codec); keys != nil {
		for _, name := range keys {
			value, err := c.encode(values[name])
			if err != nil {
				return err
			}
			encoded[name] = value
		}
	} else {
		for name, raw := range values {
			value, err := c.encode(raw)
			if err != nil {
				return err
			}
			encoded[name] = value
		}
	}
	out["values"] = encoded
	return nil
}
