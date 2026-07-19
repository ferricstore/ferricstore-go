package ferricstore

import (
	"fmt"
	"math"
)

const maxBlockingTimeoutMS int64 = 0xFFFF_FFFF

func validateOptionalBlockingTimeoutMS(command string, blockMS *int64) error {
	if blockMS != nil && (*blockMS < 0 || *blockMS > maxBlockingTimeoutMS) {
		return fmt.Errorf("%s block timeout must be between 0 and %d milliseconds", command, maxBlockingTimeoutMS)
	}
	return nil
}

func validateBlockingTimeoutSeconds(command string, timeoutSeconds int64) error {
	const maximum = maxBlockingTimeoutMS / 1_000
	if timeoutSeconds < 0 || timeoutSeconds > maximum {
		return fmt.Errorf("%s timeout must be between 0 and %d seconds", command, maximum)
	}
	return nil
}

func validateBlockingTimeoutSecondsFloat(command string, timeoutSeconds float64) error {
	maximum := float64(maxBlockingTimeoutMS) / 1_000
	if math.IsNaN(timeoutSeconds) || math.IsInf(timeoutSeconds, 0) || timeoutSeconds < 0 || timeoutSeconds > maximum {
		return fmt.Errorf("%s timeout must be finite and between 0 and %.3f seconds", command, maximum)
	}
	return nil
}
