package ferricstore

import "errors"

// encodeScheduleTarget copies only when target values need codec encoding, so
// the common metadata-only schedule path keeps its existing allocation cost.
func (c *Client) encodeScheduleTarget(target map[string]any) (map[string]any, error) {
	payload, hasPayload := target["payload"]
	rawValues, hasValues := target["values"]
	if !hasPayload && !hasValues {
		return target, nil
	}
	encodedTarget := make(map[string]any, len(target))
	for key, value := range target {
		encodedTarget[key] = value
	}
	if hasPayload && payload != nil {
		encoded, err := c.encode(payload)
		if err != nil {
			return nil, err
		}
		encodedTarget["payload"] = encoded
	}
	if hasValues && rawValues != nil {
		values, ok := rawValues.(map[string]any)
		if !ok {
			return nil, errors.New("target values must be a string-keyed mapping")
		}
		encodedValues := make(map[string]any, len(values))
		for name, value := range values {
			encoded, err := c.encode(value)
			if err != nil {
				return nil, err
			}
			encodedValues[name] = encoded
		}
		encodedTarget["values"] = encodedValues
	}
	return encodedTarget, nil
}
