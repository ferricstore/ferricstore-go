package ferricstore

import (
	"errors"
	"fmt"
	"math"
	"reflect"
)

const (
	maxGovernanceDimensionBytesV080     = 65_535
	maxGovernanceFieldBytesV080         = 262_144
	maxGovernanceReservationIDBytesV080 = 256
	maxGovernanceUsageBytesV080         = 262_144
	maxGovernanceUsageDepthV080         = 64
	maxGovernanceUsageNodesV080         = 4_096
	maxApprovalAssigneesV080            = 1_000
	maxCircuitErrorClassesV080          = 1_000
	maxCircuitErrorClassBytesV080       = 256
)

func validateGovernanceRequiredText(name, value string, maximum int) error {
	if err := validateRequiredText(name, value); err != nil {
		return err
	}
	return validateGovernanceTextSize(name, value, maximum)
}

func validateGovernanceOptionalText(name, value string, maximum int) error {
	if value == "" {
		return nil
	}
	return validateGovernanceTextSize(name, value, maximum)
}

func validateGovernanceTextSize(name, value string, maximum int) error {
	if len(value) > maximum {
		return fmt.Errorf("%s is too large (maximum %d bytes)", name, maximum)
	}
	return nil
}

func validateGovernanceScope(name, value string) error {
	return validateGovernanceRequiredText(name, value, maxGovernanceDimensionBytesV080)
}

func validateOptionalGovernanceExactNonNegative(name string, value *int64) error {
	return validateOptionalFlowExactNonNegative(name, value)
}

func validateOptionalGovernanceExactPositive(name string, value *int64) error {
	return validateOptionalFlowExactPositive(name, value)
}

func validateGovernanceUsage(usage map[string]any) error {
	if usage == nil {
		return nil
	}
	state := governanceUsageValidation{remaining: maxGovernanceUsageNodesV080}
	result, err := state.value(reflect.ValueOf(usage), 0)
	if err != nil {
		return fmt.Errorf("usage must be a bounded portable term: %w", err)
	}
	// :erlang.external_size/1 includes the ETF version byte once for the root.
	if result.externalSize+1 > maxGovernanceUsageBytesV080 {
		return fmt.Errorf(
			"usage must be a bounded portable term: external size exceeds maximum %d bytes",
			maxGovernanceUsageBytesV080,
		)
	}
	return nil
}

type governanceUsageResult struct {
	externalSize int
	byteValue    bool
}

type governanceUsageVisit struct {
	kind     reflect.Kind
	typeID   reflect.Type
	pointer  uintptr
	length   int
	capacity int
}

type governanceUsageValidation struct {
	remaining  int
	visits     [maxGovernanceUsageDepthV080 + 2]governanceUsageVisit
	visitCount int
}

func (state *governanceUsageValidation) value(value reflect.Value, depth int) (governanceUsageResult, error) {
	for value.IsValid() && value.Kind() == reflect.Interface {
		if value.IsNil() {
			return state.scalar(5, false)
		}
		value = value.Elem()
	}
	if depth > maxGovernanceUsageDepthV080 {
		return governanceUsageResult{}, fmt.Errorf("nesting exceeds maximum %d", maxGovernanceUsageDepthV080)
	}
	if !value.IsValid() {
		return state.scalar(5, false)
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return state.scalar(5, false)
		}
		if err := state.enter(value); err != nil {
			return governanceUsageResult{}, err
		}
		defer state.leave()
		if value.CanInterface() {
			if stringer, ok := value.Interface().(fmt.Stringer); ok {
				return state.scalar(5+len(stringer.String()), false)
			}
		}
		return state.value(value.Elem(), depth)
	}

	switch value.Kind() {
	case reflect.Map:
		if err := state.enter(value); err != nil {
			return governanceUsageResult{}, err
		}
		defer state.leave()
		if err := state.consumeNode(); err != nil {
			return governanceUsageResult{}, err
		}
		externalSize := 5
		iterator := value.MapRange()
		for iterator.Next() {
			key := unwrapGovernanceUsageInterface(iterator.Key())
			if !key.IsValid() || key.Kind() != reflect.String {
				return governanceUsageResult{}, errors.New("contains a non-string map key")
			}
			keyResult, err := state.value(key, depth+1)
			if err != nil {
				return governanceUsageResult{}, err
			}
			itemResult, err := state.value(iterator.Value(), depth+1)
			if err != nil {
				return governanceUsageResult{}, err
			}
			externalSize += keyResult.externalSize + itemResult.externalSize
		}
		return governanceUsageResult{externalSize: externalSize}, nil
	case reflect.Slice, reflect.Array:
		if value.Kind() == reflect.Slice && value.Type().Elem().Kind() == reflect.Uint8 {
			return state.scalar(5+value.Len(), false)
		}
		if depth+value.Len() > maxGovernanceUsageDepthV080 {
			return governanceUsageResult{}, fmt.Errorf("list nesting exceeds maximum %d", maxGovernanceUsageDepthV080)
		}
		if value.Kind() == reflect.Slice {
			if err := state.enter(value); err != nil {
				return governanceUsageResult{}, err
			}
			defer state.leave()
		}
		if value.Len() == 0 {
			return state.scalar(1, false)
		}
		externalSize := 6 // LIST_EXT header plus NIL_EXT tail.
		byteList := true
		for index := 0; index < value.Len(); index++ {
			if err := state.consumeNode(); err != nil { // list cons cell
				return governanceUsageResult{}, err
			}
			item, err := state.value(value.Index(index), depth+index+1)
			if err != nil {
				return governanceUsageResult{}, err
			}
			externalSize += item.externalSize
			byteList = byteList && item.byteValue
		}
		if err := state.consumeNode(); err != nil { // empty list tail
			return governanceUsageResult{}, err
		}
		if byteList {
			return governanceUsageResult{externalSize: 3 + value.Len()}, nil
		}
		return governanceUsageResult{externalSize: externalSize}, nil
	case reflect.Bool:
		if value.Bool() {
			return state.scalar(6, false)
		}
		return state.scalar(7, false)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		integer := value.Int()
		return state.scalar(governanceETFIntegerSize(integer), integer >= 0 && integer <= 255)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		integer := value.Uint()
		if integer > math.MaxInt64 {
			return governanceUsageResult{}, errors.New("contains an integer that overflows int64")
		}
		return state.scalar(governanceETFIntegerSize(int64(integer)), integer <= 255)
	case reflect.Float32, reflect.Float64:
		return state.scalar(9, false)
	case reflect.String:
		return state.scalar(5+value.Len(), false)
	default:
		if value.CanInterface() {
			if stringer, ok := value.Interface().(fmt.Stringer); ok {
				return state.scalar(5+len(stringer.String()), false)
			}
		}
		return governanceUsageResult{}, errors.New("contains an unsupported value")
	}
}

func (state *governanceUsageValidation) scalar(externalSize int, byteValue bool) (governanceUsageResult, error) {
	if err := state.consumeNode(); err != nil {
		return governanceUsageResult{}, err
	}
	return governanceUsageResult{externalSize: externalSize, byteValue: byteValue}, nil
}

func (state *governanceUsageValidation) consumeNode() error {
	state.remaining--
	if state.remaining < 0 {
		return fmt.Errorf("node count exceeds maximum %d", maxGovernanceUsageNodesV080)
	}
	return nil
}

func (state *governanceUsageValidation) enter(value reflect.Value) error {
	visit := governanceUsageVisit{
		kind: value.Kind(), typeID: value.Type(), pointer: value.Pointer(),
	}
	if value.Kind() == reflect.Slice {
		visit.length, visit.capacity = value.Len(), value.Cap()
	}
	for index := 0; index < state.visitCount; index++ {
		if state.visits[index] == visit {
			return errors.New("contains a reference cycle")
		}
	}
	if state.visitCount == len(state.visits) {
		return fmt.Errorf("nesting exceeds maximum %d", maxGovernanceUsageDepthV080)
	}
	state.visits[state.visitCount] = visit
	state.visitCount++
	return nil
}

func (state *governanceUsageValidation) leave() {
	state.visitCount--
	state.visits[state.visitCount] = governanceUsageVisit{}
}

func unwrapGovernanceUsageInterface(value reflect.Value) reflect.Value {
	for value.IsValid() && value.Kind() == reflect.Interface {
		if value.IsNil() {
			return reflect.Value{}
		}
		value = value.Elem()
	}
	return value
}

func governanceETFIntegerSize(value int64) int {
	if value >= 0 && value <= 255 {
		return 2
	}
	if value >= math.MinInt32 && value <= math.MaxInt32 {
		return 5
	}
	magnitude := uint64(value)
	if value < 0 {
		magnitude = uint64(-(value + 1)) + 1
	}
	bytes := 1
	for magnitude >>= 8; magnitude != 0; magnitude >>= 8 {
		bytes++
	}
	return 3 + bytes
}
