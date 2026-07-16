package ferricstore

import "fmt"

func normalizeAdminResult(value any, err error) (any, error) {
	if err != nil {
		return nil, err
	}
	return normalizeAdminResponse(value)
}

func normalizedAdminMap(value any) (map[string]any, error) {
	normalized, err := normalizeAdminResponse(value)
	if err != nil {
		return nil, err
	}
	return nativeMap(normalized)
}

type adminNormalizeBudget struct {
	remaining int
}

func normalizeAdminResponse(value any) (any, error) {
	budget := adminNormalizeBudget{remaining: nativeMaxContainerItems}
	return normalizeAdminResponseDepth(value, &budget, 0)
}

func normalizeAdminResponseDepth(value any, budget *adminNormalizeBudget, depth int) (any, error) {
	if depth > nativeMaxDecodeDepth {
		return nil, fmt.Errorf("admin response exceeds maximum nesting depth %d", nativeMaxDecodeDepth)
	}
	switch v := value.(type) {
	case []byte:
		return string(v), nil
	case []any:
		if err := budget.consume(len(v)); err != nil {
			return nil, err
		}
		out := make([]any, len(v))
		for index, item := range v {
			normalized, err := normalizeAdminResponseDepth(item, budget, depth+1)
			if err != nil {
				return nil, fmt.Errorf("admin response item %d: %w", index, err)
			}
			out[index] = normalized
		}
		return out, nil
	case map[string]any:
		if err := budget.consume(len(v)); err != nil {
			return nil, err
		}
		out := make(map[string]any, len(v))
		for key, item := range v {
			normalized, err := normalizeAdminResponseDepth(item, budget, depth+1)
			if err != nil {
				return nil, fmt.Errorf("admin response field %q: %w", key, err)
			}
			out[key] = normalized
		}
		return out, nil
	case map[interface{}]interface{}:
		if err := budget.consume(len(v)); err != nil {
			return nil, err
		}
		out := make(map[string]any, len(v))
		for key, item := range v {
			name, err := nativeMapKey(key)
			if err != nil {
				return nil, fmt.Errorf("admin response map key: %w", err)
			}
			if _, exists := out[name]; exists {
				return nil, fmt.Errorf("duplicate admin response map key %q", name)
			}
			normalized, err := normalizeAdminResponseDepth(item, budget, depth+1)
			if err != nil {
				return nil, fmt.Errorf("admin response field %q: %w", name, err)
			}
			out[name] = normalized
		}
		return out, nil
	default:
		return value, nil
	}
}

func (budget *adminNormalizeBudget) consume(count int) error {
	if count < 0 || count > budget.remaining {
		return fmt.Errorf("admin response exceeds maximum item count %d", nativeMaxContainerItems)
	}
	budget.remaining -= count
	return nil
}

func adminArrayResponse(value any) ([]any, error) {
	value, err := normalizeAdminResponse(value)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected admin array response, got %T", value)
	}
	return append([]any(nil), items...), nil
}
