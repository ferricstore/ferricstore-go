package ferricstore

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
)

const (
	maxProbabilisticBatchItemsV080 = 10_000
	maxBloomBitsV080               = 8_589_934_592
	maxBloomHashesV080             = 1_024
	maxCuckooCapacityV080          = 1_073_741_824
	maxCMSDepthV080                = 1_024
	maxCMSCountersV080             = 16_777_216
	maxCMSMergeSourcesV080         = 128
	maxTopKKV080                   = 100_000
	maxTopKCountersV080            = 1_048_576
	maxTopKElementBytesV080        = 252
	maxTDigestCompressionV080      = 1_000
	maxTDigestMergeSourcesV080     = 10_000
)

func validateProbabilisticBatch(command string, count int) error {
	if count > maxProbabilisticBatchItemsV080 {
		return fmt.Errorf("%s batch exceeds FerricStore 0.8 maximum of %d items", command, maxProbabilisticBatchItemsV080)
	}
	return nil
}

func validateBloomSizingV080(errorRate float64, capacity int64) error {
	bitsPerItem := -bloomSizingLog(errorRate) / (math.Ln2 * math.Ln2)
	maxCapacity := math.Floor(float64(maxBloomBitsV080) / bitsPerItem)
	if math.IsNaN(maxCapacity) || float64(capacity) > maxCapacity {
		return fmt.Errorf("BF.RESERVE computed bits exceed FerricStore 0.8 maximum of %d", int64(maxBloomBitsV080))
	}
	numBits := math.Ceil(float64(capacity) * bitsPerItem)
	numHashes := math.Round(numBits / float64(capacity) * math.Ln2)
	if numBits > maxBloomBitsV080 || numHashes > maxBloomHashesV080 {
		return errorsBloomSizingV080(numBits, numHashes)
	}
	return nil
}

// bloomSizingLog keeps Bloom limit validation stable on architectures whose
// optimized math.Log implementation loses precision for subnormal inputs.
// Frexp extracts the subnormal exponent exactly; math.Log then only sees a
// normal mantissa in [0.5, 1), matching the server's sizing calculation.
func bloomSizingLog(value float64) float64 {
	if math.Float64bits(value)&0x7ff0000000000000 != 0 {
		return math.Log(value)
	}
	mantissa, exponent := math.Frexp(value)
	return math.Log(mantissa) + float64(exponent)*math.Ln2
}

func errorsBloomSizingV080(numBits, numHashes float64) error {
	if numBits > maxBloomBitsV080 {
		return fmt.Errorf("BF.RESERVE computed bits exceed FerricStore 0.8 maximum of %d", int64(maxBloomBitsV080))
	}
	return fmt.Errorf("BF.RESERVE hash count exceeds FerricStore 0.8 maximum of %d", int64(maxBloomHashesV080))
}

func validateCuckooCapacityV080(capacity int64) error {
	if capacity > maxCuckooCapacityV080 {
		return fmt.Errorf("CF.RESERVE capacity exceeds FerricStore 0.8 maximum of %d", int64(maxCuckooCapacityV080))
	}
	return nil
}

func validateCMSDimensionsV080(width, depth int64) error {
	if depth > maxCMSDepthV080 {
		return fmt.Errorf("CMS depth exceeds FerricStore 0.8 maximum of %d", int64(maxCMSDepthV080))
	}
	if width > maxCMSCountersV080/depth {
		return fmt.Errorf("CMS counter count exceeds FerricStore 0.8 maximum of %d", int64(maxCMSCountersV080))
	}
	return nil
}

func validateCMSProbabilityV080(errorRate, probability float64) error {
	depth := int64(math.Ceil(-math.Log(probability)))
	if depth <= 0 || depth > maxCMSDepthV080 {
		return fmt.Errorf("CMS.INITBYPROB dimensions exceed FerricStore 0.8 limits")
	}
	maxWidth := int64(maxCMSCountersV080) / depth
	if errorRate < math.E/float64(maxWidth) {
		return fmt.Errorf("CMS counter count exceeds FerricStore 0.8 maximum of %d", int64(maxCMSCountersV080))
	}
	width := math.Ceil(math.E / errorRate)
	if math.IsInf(width, 0) || width > float64(maxWidth) {
		return fmt.Errorf("CMS counter count exceeds FerricStore 0.8 maximum of %d", int64(maxCMSCountersV080))
	}
	return nil
}

func validateCMSMergeSourceCountV080(count int) error {
	if count > maxCMSMergeSourcesV080 {
		return fmt.Errorf("CMS.MERGE source count exceeds FerricStore 0.8 maximum of %d", maxCMSMergeSourcesV080)
	}
	return nil
}

func validateTopKReserveV080(k, width, depth int64) error {
	if k > maxTopKKV080 {
		return fmt.Errorf("TOPK.RESERVE k exceeds FerricStore 0.8 maximum of %d", int64(maxTopKKV080))
	}
	if width > maxTopKCountersV080/depth {
		return fmt.Errorf("TOPK.RESERVE counter count exceeds FerricStore 0.8 maximum of %d", int64(maxTopKCountersV080))
	}
	return nil
}

func validateTDigestCompressionV080(command string, compression int64) error {
	if compression > maxTDigestCompressionV080 {
		return fmt.Errorf("%s compression exceeds FerricStore 0.8 maximum of %d", command, int64(maxTDigestCompressionV080))
	}
	return nil
}

func validateTDigestMergeSourceCountV080(count int) error {
	if count > maxTDigestMergeSourcesV080 {
		return fmt.Errorf("TDIGEST.MERGE source count exceeds FerricStore 0.8 maximum of %d", maxTDigestMergeSourcesV080)
	}
	return nil
}

func validateEncodedByteLimit(command, field string, value any, limit int) error {
	length, err := encodedCommandArgLength(value)
	if err != nil {
		return fmt.Errorf("%s %s: %w", command, field, err)
	}
	if length > limit {
		return fmt.Errorf("%s %s exceeds FerricStore 0.8 maximum of %d bytes", command, field, limit)
	}
	return nil
}

func encodedCommandArgLength(value any) (int, error) {
	switch typed := value.(type) {
	case string:
		return len(typed), nil
	case []byte:
		return len(typed), nil
	case nil:
		return 0, fmt.Errorf("encoded value must not be nil")
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.String, reflect.Slice:
		if reflected.Kind() == reflect.String {
			return reflected.Len(), nil
		}
		if reflected.Type().Elem().Kind() == reflect.Uint8 {
			return reflected.Len(), nil
		}
	case reflect.Bool:
		if reflected.Bool() {
			return 4, nil
		}
		return 5, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return len(strconv.FormatInt(reflected.Int(), 10)), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return len(strconv.FormatUint(reflected.Uint(), 10)), nil
	case reflect.Float32, reflect.Float64:
		return len(strconv.FormatFloat(reflected.Float(), 'g', -1, reflected.Type().Bits())), nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return 0, fmt.Errorf("encoded value is not a supported command argument: %w", err)
	}
	return len(encoded), nil
}
