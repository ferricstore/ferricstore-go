package ferricstore

import (
	"fmt"
	"strconv"
	"strings"
)

func asString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	case nativeCompactOKCount:
		return "OK"
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	default:
		return fmt.Sprint(v)
	}
}

func asBytes(value any) []byte {
	switch v := value.(type) {
	case nil:
		return nil
	case []byte:
		return v
	case string:
		return []byte(v)
	default:
		return []byte(fmt.Sprint(v))
	}
}

func asInt64(value any) int64 {
	switch v := value.(type) {
	case nil:
		return 0
	case int64:
		return v
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case int16:
		return int64(v)
	case int8:
		return int64(v)
	case uint64:
		return int64(v)
	case uint:
		return int64(v)
	case string:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	case []byte:
		n, _ := strconv.ParseInt(string(v), 10, 64)
		return n
	default:
		n, _ := strconv.ParseInt(fmt.Sprint(v), 10, 64)
		return n
	}
}

func asFloat64(value any) float64 {
	switch v := value.(type) {
	case nil:
		return 0
	case float64:
		return v
	case float32:
		return float64(v)
	case int64:
		return float64(v)
	case int:

		return float64(v)
	case string:
		n, _ := strconv.ParseFloat(v, 64)
		return n
	case []byte:
		n, _ := strconv.ParseFloat(string(v), 64)
		return n
	default:
		n, _ := strconv.ParseFloat(fmt.Sprint(v), 64)
		return n
	}
}

func asBool(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return v == "1" || v == "true" || v == "OK"
	case []byte:
		return asBool(string(v))
	case int64:
		return v != 0
	case int:
		return v != 0
	default:
		return value != nil
	}
}

func isOK(value any) bool {
	switch v := value.(type) {
	case string:
		return strings.EqualFold(v, "OK")
	case []byte:
		return strings.EqualFold(string(v), "OK")
	case nativeCompactOKCount:
		return v > 0
	case []any:
		if len(v) == 0 {
			return false
		}
		for _, item := range v {
			if !isOK(item) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
