package ferricstore

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const maxBitmapOffset int64 = 4_294_967_295

func validateNonNegativeCount(command string, count *int) error {
	if count != nil && *count < 0 {
		return fmt.Errorf("%s count must be non-negative", command)
	}
	return nil
}

func validateStreamRead(command string, count *int, blockMS *int64, streams []StreamRef) error {
	if len(streams) == 0 {
		return fmt.Errorf("%s requires at least one stream", command)
	}
	if err := validateNonNegativeCount(command, count); err != nil {
		return err
	}
	if blockMS != nil && *blockMS < 0 {
		return fmt.Errorf("%s block timeout must be non-negative", command)
	}
	for index, stream := range streams {
		if !validStreamReadID(command, stream.ID) {
			return fmt.Errorf("%s stream %d has an invalid ID %q", command, index, stream.ID)
		}
	}
	return nil
}

func validStreamReadID(command, id string) bool {
	if id == "0" {
		return true
	}
	if command == "XREAD" && id == "$" {
		return true
	}
	if command == "XREADGROUP" && id == ">" {
		return true
	}
	_, _, ok := parseStreamIDText(id)
	return ok
}

func validateStreamTrim(threshold string, limit *int) error {
	if _, err := strconv.ParseUint(threshold, 10, 64); err != nil {
		return errors.New("XTRIM MAXLEN threshold must be a non-negative integer")
	}
	if limit != nil {
		return errors.New("XTRIM LIMIT is not supported by this server protocol")
	}
	return nil
}

func validateBitmapOffset(offset int64) error {
	if offset < 0 || offset > maxBitmapOffset {
		return fmt.Errorf("bitmap offset must be between 0 and %d", maxBitmapOffset)
	}
	return nil
}

func validateBitValue(bit int) error {
	if bit != 0 && bit != 1 {
		return errors.New("bitmap bit value must be 0 or 1")
	}
	return nil
}

func bitmapPositionResponse(value any, err error) (int64, error) {
	position, err := responseInt64(value, err)
	if err != nil {
		return 0, err
	}
	if position < -1 {
		return 0, fmt.Errorf("BITPOS returned invalid position %d", position)
	}
	return position, nil
}

func validateGeoCoordinate(longitude, latitude float64) error {
	if math.IsNaN(longitude) || math.IsInf(longitude, 0) || longitude < -180 || longitude > 180 {
		return errors.New("geo longitude must be finite and between -180 and 180")
	}
	if math.IsNaN(latitude) || math.IsInf(latitude, 0) || latitude < -85.05112878 || latitude > 85.05112878 {
		return errors.New("geo latitude must be finite and between -85.05112878 and 85.05112878")
	}
	return nil
}

func validateGeoUnit(unit string, allowEmpty bool) error {
	if allowEmpty && unit == "" {
		return nil
	}
	switch strings.ToUpper(unit) {
	case "M", "KM", "FT", "MI":
		return nil
	default:
		return fmt.Errorf("unsupported geo unit %q", unit)
	}
}

func validatePositiveFinite(command, field string, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 {
		return fmt.Errorf("%s %s must be finite and positive", command, field)
	}
	return nil
}

func validateUnitInterval(command, field string, value float64, exclusive bool) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Errorf("%s %s must be finite", command, field)
	}
	if exclusive && (value <= 0 || value >= 1) {
		return fmt.Errorf("%s %s must be between 0 and 1 exclusive", command, field)
	}
	if !exclusive && (value < 0 || value > 1) {
		return fmt.Errorf("%s %s must be between 0 and 1", command, field)
	}
	return nil
}

func validatePositiveInt64(command, field string, value int64) error {
	if value <= 0 {
		return fmt.Errorf("%s %s must be positive", command, field)
	}
	return nil
}

func validateFiniteValues(command string, values []float64) error {
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("%s values must be finite", command)
		}
	}
	return nil
}

func validateQuantiles(command string, values []float64) error {
	for _, value := range values {
		if err := validateUnitInterval(command, "quantile", value, false); err != nil {
			return err
		}
	}
	return nil
}

func nonNegativeIntArrayExact(value any, err error, expected int, command string) ([]int64, error) {
	items, err := intArrayExact(value, err, expected, command)
	if err != nil {
		return nil, err
	}
	for index, item := range items {
		if item < 0 {
			return nil, fmt.Errorf("%s result %d is negative", command, index)
		}
	}
	return items, nil
}
