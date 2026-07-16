package ferricstore

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

func normalizeRequestContext(value any) (map[string]any, error) {
	mapping, err := requestContextMap(value)
	if err != nil {
		return nil, err
	}
	fields := make(map[string]any, len(mapping))
	for key, item := range mapping {
		name := strings.ToLower(strings.TrimSpace(key))
		switch name {
		case "subject", "tenant", "scopes":
		default:
			return nil, fmt.Errorf("request context contains unsupported field %q", key)
		}
		if _, exists := fields[name]; exists {
			return nil, fmt.Errorf("request context contains duplicate field %q", name)
		}
		fields[name] = item
	}

	out := make(map[string]any, len(fields))
	for _, name := range []string{"subject", "tenant"} {
		value, exists := fields[name]
		if !exists {
			continue
		}
		text, ok := value.(string)
		if !ok || strings.TrimSpace(text) == "" {
			return nil, fmt.Errorf("request context %s must be a non-empty string", name)
		}
		out[name] = strings.TrimSpace(text)
	}
	if value, exists := fields["scopes"]; exists {
		scopes, err := normalizeRequestContextScopes(value)
		if err != nil {
			return nil, err
		}
		out["scopes"] = scopes
	}
	return out, nil
}

func requestContextMap(value any) (map[string]any, error) {
	switch v := value.(type) {
	case *RequestContext:
		if v == nil {
			return nil, errors.New("request context must not be nil")
		}
		return requestContextFields(*v), nil
	case RequestContext:
		return requestContextFields(v), nil
	case map[string]any:
		return v, nil
	case map[interface{}]interface{}:
		out := make(map[string]any, len(v))
		for key, item := range v {
			name, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("request context field name must be a string, got %T", key)
			}
			out[name] = item
		}
		return out, nil
	default:
		return nil, fmt.Errorf("request context must be an object, got %T", value)
	}
}

func requestContextFields(value RequestContext) map[string]any {
	out := map[string]any{}
	if value.Subject != "" {
		out["subject"] = value.Subject
	}
	if value.Tenant != "" {
		out["tenant"] = value.Tenant
	}
	if value.Scopes != nil {
		out["scopes"] = value.Scopes
	}
	return out
}

func normalizeRequestContextScopes(value any) ([]string, error) {
	var values []string
	switch v := value.(type) {
	case string:
		values = strings.Fields(v)
	case []string:
		values = v
	case []any:
		values = make([]string, 0, len(v))
		for _, item := range v {
			text, ok := item.(string)
			if !ok {
				return nil, errors.New("request context scopes must contain only non-empty strings")
			}
			values = append(values, text)
		}
	default:
		rv := reflect.ValueOf(value)
		if !rv.IsValid() || (rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array) || rv.Type().Elem().Kind() != reflect.String {
			return nil, errors.New("request context scopes must be a string or an array of strings")
		}
		values = make([]string, 0, rv.Len())
		for idx := 0; idx < rv.Len(); idx++ {
			values = append(values, rv.Index(idx).String())
		}
	}
	return uniqueRequestContextScopes(values)
}

func uniqueRequestContextScopes(values []string) ([]string, error) {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, errors.New("request context scopes must contain only non-empty strings")
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out, nil
}
