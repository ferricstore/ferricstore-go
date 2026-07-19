package ferricstore

import "fmt"

const maxNativeWindowMSV080 = maxRelativeExpiryMillisV080 / 2

func validateNativeTTLMSV080(command string, ttlMS int64, allowZero bool) error {
	minimum := int64(1)
	if allowZero {
		minimum = 0
	}
	if ttlMS < minimum || ttlMS > maxRelativeExpiryMillisV080 {
		return fmt.Errorf("%s ttl must be between %d and %d milliseconds", command, minimum, int64(maxRelativeExpiryMillisV080))
	}
	return nil
}

func validateNativeTTLSecondsV080(command string, seconds int64) error {
	if seconds <= 0 || seconds > maxRelativeExpirySecsV080 {
		return fmt.Errorf("%s expiration must be between 1 and %d seconds", command, int64(maxRelativeExpirySecsV080))
	}
	return nil
}

func validateNativeWindowMSV080(windowMS int64) error {
	if windowMS <= 0 || windowMS > maxNativeWindowMSV080 {
		return fmt.Errorf("RATELIMIT.ADD window must be between 1 and %d milliseconds", int64(maxNativeWindowMSV080))
	}
	return nil
}
