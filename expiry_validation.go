package ferricstore

import (
	"fmt"
	"math"
)

const (
	maxRelativeExpiryMillisV080 = int64(math.MaxInt64 - 281_474_976_710_655)
	maxRelativeExpirySecsV080   = maxRelativeExpiryMillisV080 / 1000
	maxAbsoluteExpirySecsV080   = int64(math.MaxInt64 / 1000)
)

func validateExpiryOptionBounds(
	command string,
	exSeconds, pxMilliseconds, exatSeconds, pxatMillis *int64,
) error {
	if err := validateExpiryOptionBound(command, "EX", exSeconds, maxRelativeExpirySecsV080); err != nil {
		return err
	}
	if err := validateExpiryOptionBound(command, "PX", pxMilliseconds, maxRelativeExpiryMillisV080); err != nil {
		return err
	}
	if err := validateExpiryOptionBound(command, "EXAT", exatSeconds, maxAbsoluteExpirySecsV080); err != nil {
		return err
	}
	return validateExpiryOptionBound(command, "PXAT", pxatMillis, math.MaxInt64)
}

func validateExpiryOptionBound(command, option string, value *int64, maximum int64) error {
	if value != nil && *value > maximum {
		return fmt.Errorf("%s %s expiration exceeds FerricStore 0.8 integer range", command, option)
	}
	return nil
}

func validateRelativeExpiryValue(command string, value, maximum int64) error {
	if value > maximum {
		return fmt.Errorf("%s expiration exceeds FerricStore 0.8 integer range", command)
	}
	return nil
}
