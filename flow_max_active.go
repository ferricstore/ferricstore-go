package ferricstore

import (
	"fmt"
	"reflect"
	"strings"
)

const (
	// FlowMaxActiveInfinity explicitly disables the active-runtime deadline.
	FlowMaxActiveInfinity       = "infinity"
	maxFlowActiveMS       int64 = 31_536_000_000
)

func canonicalFlowMaxActiveMS(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if text, ok := value.(string); ok {
		if strings.EqualFold(text, FlowMaxActiveInfinity) {
			return FlowMaxActiveInfinity, nil
		}
		return nil, flowMaxActiveRangeError()
	}
	typeValue := reflect.ValueOf(value)
	if !typeValue.IsValid() {
		return nil, nil
	}
	if typeValue.Kind() == reflect.String {
		if strings.EqualFold(typeValue.String(), FlowMaxActiveInfinity) {
			return FlowMaxActiveInfinity, nil
		}
		return nil, flowMaxActiveRangeError()
	}
	var milliseconds int64
	switch typeValue.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		milliseconds = typeValue.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		unsigned := typeValue.Uint()
		if unsigned > uint64(maxFlowActiveMS) {
			return nil, flowMaxActiveRangeError()
		}
		milliseconds = int64(unsigned)
	default:
		return nil, flowMaxActiveRangeError()
	}
	if milliseconds <= 0 || milliseconds > maxFlowActiveMS {
		return nil, flowMaxActiveRangeError()
	}
	return milliseconds, nil
}

func flowMaxActiveRangeError() error {
	return fmt.Errorf(
		"flow max_active_ms must be between 1 and %d or infinity",
		maxFlowActiveMS,
	)
}

func appendFlowMaxActiveMS(args *[]any, value any) error {
	canonical, err := canonicalFlowMaxActiveMS(value)
	if err != nil {
		return err
	}
	if canonical != nil {
		*args = append(*args, "MAX_ACTIVE_MS", canonical)
	}
	return nil
}

func anyCreateItemMaxActive(items []CreateItem) bool {
	for _, item := range items {
		if item.MaxActiveMS != nil {
			return true
		}
	}
	return false
}

func anyChildMaxActive(children []ChildSpec) bool {
	for _, child := range children {
		if child.MaxActiveMS != nil {
			return true
		}
	}
	return false
}

func anyChildAttributes(children []ChildSpec) bool {
	for _, child := range children {
		if len(child.Attributes) > 0 {
			return true
		}
	}
	return false
}

func anyChildStateMeta(children []ChildSpec) bool {
	for _, child := range children {
		if len(child.StateMeta) > 0 {
			return true
		}
	}
	return false
}
