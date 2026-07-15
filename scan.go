package ferricstore

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
)

func normalizeScanCursor(cursor any, collection bool) (any, error) {
	switch value := cursor.(type) {
	case string:
		if collection && value != "0" && !strings.HasPrefix(value, "~") {
			return nil, errors.New("collection scan cursor must be 0 or a server cursor token")
		}
		return value, nil
	case []byte:
		if collection && string(value) != "0" && !strings.HasPrefix(string(value), "~") {
			return nil, errors.New("collection scan cursor must be 0 or a server cursor token")
		}
		return value, nil
	case nil:
		return nil, errors.New("scan cursor must not be nil")
	}

	value := reflect.ValueOf(cursor)
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		cursorValue := value.Int()
		if cursorValue < 0 || (collection && cursorValue != 0) {
			return nil, errors.New("scan cursor must be non-negative; collection scans accept only numeric cursor 0")
		}
		return cursorValue, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		cursorValue := value.Uint()
		if cursorValue > math.MaxInt64 || (collection && cursorValue != 0) {
			return nil, errors.New("scan cursor is out of range; collection scans accept only numeric cursor 0")
		}
		return int64(cursorValue), nil
	default:
		return nil, fmt.Errorf("scan cursor must be an integer, string, or byte slice, got %T", cursor)
	}
}

func validateScanCount(count *int) error {
	if count != nil && *count <= 0 {
		return errors.New("scan count must be positive")
	}
	return nil
}
