package ferricstore

import "fmt"

type probabilisticInfoValueKind uint8

const (
	infoPositiveInteger probabilisticInfoValueKind = iota
	infoNonNegativeInteger
	infoUnitFloat
	infoExclusiveUnitFloat
	infoNonNegativeFloat
)

type probabilisticInfoField struct {
	name string
	kind probabilisticInfoValueKind
}

var bloomInfoSchema = [...]probabilisticInfoField{
	{"Capacity", infoPositiveInteger},
	{"Size", infoNonNegativeInteger},
	{"Number of filters", infoPositiveInteger},
	{"Number of items inserted", infoNonNegativeInteger},
	{"Expansion rate", infoNonNegativeInteger},
	{"Error rate", infoExclusiveUnitFloat},
	{"Number of hash functions", infoPositiveInteger},
	{"Number of bits", infoPositiveInteger},
}

var cuckooInfoSchema = [...]probabilisticInfoField{
	{"Size", infoPositiveInteger},
	{"Number of buckets", infoPositiveInteger},
	{"Number of filters", infoPositiveInteger},
	{"Number of items inserted", infoNonNegativeInteger},
	{"Number of items deleted", infoNonNegativeInteger},
	{"Bucket size", infoPositiveInteger},
	{"Fingerprint size", infoPositiveInteger},
	{"Max iterations", infoPositiveInteger},
	{"Expansion rate", infoNonNegativeInteger},
}

var cmsInfoSchema = [...]probabilisticInfoField{
	{"width", infoPositiveInteger},
	{"depth", infoPositiveInteger},
	{"count", infoNonNegativeInteger},
}

var topKInfoSchema = [...]probabilisticInfoField{
	{"k", infoPositiveInteger},
	{"width", infoPositiveInteger},
	{"depth", infoPositiveInteger},
	{"decay", infoUnitFloat},
}

var tDigestInfoSchema = [...]probabilisticInfoField{
	{"Compression", infoPositiveInteger},
	{"Capacity", infoPositiveInteger},
	{"Merged nodes", infoNonNegativeInteger},
	{"Unmerged nodes", infoNonNegativeInteger},
	{"Merged weight", infoNonNegativeFloat},
	{"Unmerged weight", infoNonNegativeFloat},
	{"Total compressions", infoNonNegativeInteger},
	{"Memory usage", infoNonNegativeInteger},
}

func probabilisticInfoResponse(command string, value any, schema []probabilisticInfoField) (map[string]any, error) {
	result, err := nativeMap(value)
	if err != nil {
		return nil, err
	}
	for _, field := range schema {
		fieldValue, exists := result[field.name]
		if !exists {
			return nil, fmt.Errorf("%s response is missing %q", command, field.name)
		}
		if err := validateProbabilisticInfoField(command, field, fieldValue); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func validateProbabilisticInfoField(command string, field probabilisticInfoField, value any) error {
	switch field.kind {
	case infoPositiveInteger, infoNonNegativeInteger:
		number, err := responseInt64(value, nil)
		if err != nil || number < 0 || (field.kind == infoPositiveInteger && number == 0) {
			return fmt.Errorf("%s field %q has an invalid integer value", command, field.name)
		}
	case infoUnitFloat, infoExclusiveUnitFloat, infoNonNegativeFloat:
		number, err := responseFloat64(value, nil)
		if err != nil || number < 0 ||
			(field.kind == infoUnitFloat && number > 1) ||
			(field.kind == infoExclusiveUnitFloat && (number <= 0 || number >= 1)) {
			return fmt.Errorf("%s field %q has an invalid numeric value", command, field.name)
		}
	default:
		return fmt.Errorf("%s field %q has an unknown schema", command, field.name)
	}
	return nil
}
