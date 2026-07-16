package ferricstore

import (
	"errors"
	"math"
	"reflect"
	"strings"
)

const (
	maxFlowAttributes          = 16
	maxFlowAttributeKeyBytes   = 64
	maxFlowAttributeValueBytes = 256
	maxFlowAttributeTotalBytes = 2_048
	maxFlowAttributeListValues = 16
	maxFlowStateMetaEntries    = 16
	maxFlowStateMetaStates     = 64
	maxFlowStateMetaStateBytes = 64
	maxFlowStateMetaTotalBytes = 16_384
)

func validateFlowMetadata(attributes, attributesMerge map[string]any, attributesDelete []string, stateMeta map[string]any) error {
	if err := validateFlowAttributes(attributes); err != nil {
		return err
	}
	if err := validateFlowAttributes(attributesMerge); err != nil {
		return err
	}
	for _, name := range attributesDelete {
		if _, err := validateFlowMetadataKey("attribute", name); err != nil {
			return err
		}
	}
	return validateFlowStateMeta(stateMeta)
}

func validateFlowAttributes(attributes map[string]any) error {
	if len(attributes) > maxFlowAttributes {
		return errors.New("too many flow attributes")
	}
	total := 0
	var normalized map[string]struct{}
	if len(attributes) > 1 {
		normalized = make(map[string]struct{}, len(attributes))
	}
	for name, value := range attributes {
		key, err := validateFlowMetadataKey("attribute", name)
		if err != nil {
			return err
		}
		if _, exists := normalized[key]; exists {
			return errors.New("flow attribute key is duplicated after normalization")
		}
		if normalized != nil {
			normalized[key] = struct{}{}
		}
		size, err := flowAttributeValueSize(value)
		if err != nil {
			return err
		}
		total += len(key) + size
	}
	if total > maxFlowAttributeTotalBytes {
		return errors.New("flow attributes are too large")
	}
	return nil
}

func validateFlowStateMeta(stateMeta map[string]any) error {
	_, err := flowStateMetaEntrySize(stateMeta)
	return err
}

func flowStateMetaEntrySize(stateMeta map[string]any) (int, error) {
	if len(stateMeta) > maxFlowStateMetaEntries {
		return 0, errors.New("too many flow state_meta entries")
	}
	total := 0
	var normalized map[string]struct{}
	if len(stateMeta) > 1 {
		normalized = make(map[string]struct{}, len(stateMeta))
	}
	for name, value := range stateMeta {
		key, err := validateFlowMetadataKey("state_meta", name)
		if err != nil {
			return 0, err
		}
		if _, exists := normalized[key]; exists {
			return 0, errors.New("flow state_meta key is duplicated after normalization")
		}
		if normalized != nil {
			normalized[key] = struct{}{}
		}
		size, err := flowMetadataScalarSize(value, "state_meta")
		if err != nil {
			return 0, err
		}
		total += len(key) + size
	}
	return total, nil
}

func validateFlowMetadataKey(kind, name string) (string, error) {
	name = canonicalFlowMetadataKey(name)
	switch {
	case name == "":
		return "", errors.New("flow " + kind + " key must not be empty")
	case len(name) > maxFlowAttributeKeyBytes:
		return "", errors.New("flow " + kind + " key is too large")
	case strings.HasPrefix(name, "__"):
		return "", errors.New("flow " + kind + " key is reserved")
	default:
		return name, nil
	}
}

func canonicalFlowMetadataKey(name string) string { return strings.TrimSpace(name) }

func flowAttributeValueSize(value any) (int, error) {
	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return 0, errors.New("flow attribute value must be scalar or a string list")
	}
	if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() != reflect.Uint8 {
		if rv.Len() == 0 {
			return 0, errors.New("flow attribute list must not be empty")
		}
		if rv.Len() > maxFlowAttributeListValues {
			return 0, errors.New("flow attribute list is too large")
		}
		total := 0
		for index := 0; index < rv.Len(); index++ {
			item := rv.Index(index)
			if item.Kind() == reflect.Interface && !item.IsNil() {
				item = item.Elem()
			}
			if !item.IsValid() || (item.Kind() != reflect.String && !isByteSliceValue(item)) {
				return 0, errors.New("flow attribute list values must be strings")
			}
			size := metadataTextSize(item)
			if size > maxFlowAttributeValueBytes {
				return 0, errors.New("flow attribute value is too large")
			}
			total += size
		}
		return total, nil
	}
	return flowMetadataScalarSize(value, "attribute")
}

func flowMetadataScalarSize(value any, kind string) (int, error) {
	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return 0, errors.New("flow " + kind + " value must be scalar")
	}
	switch rv.Kind() {
	case reflect.String:
		if rv.Len() > maxFlowAttributeValueBytes {
			return 0, errors.New("flow " + kind + " value is too large")
		}
		return rv.Len(), nil
	case reflect.Slice:
		if !isByteSliceValue(rv) {
			return 0, errors.New("flow " + kind + " value must be scalar")
		}
		if rv.Len() > maxFlowAttributeValueBytes {
			return 0, errors.New("flow " + kind + " value is too large")
		}
		return rv.Len(), nil
	case reflect.Bool:
		return 1, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return 8, nil
	case reflect.Float32, reflect.Float64:
		value := rv.Convert(reflect.TypeOf(float64(0))).Float()
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return 0, errors.New("flow " + kind + " value must be finite")
		}
		return 8, nil
	default:
		return 0, errors.New("flow " + kind + " value must be scalar")
	}
}

func isByteSliceValue(value reflect.Value) bool {
	return value.Kind() == reflect.Slice && value.Type().Elem().Kind() == reflect.Uint8
}

func metadataTextSize(value reflect.Value) int {
	if value.Kind() == reflect.String {
		return value.Len()
	}
	return value.Len()
}
