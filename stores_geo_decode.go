package ferricstore

import (
	"errors"
	"fmt"
)

const maxGeoHash = int64(1<<52) - 1

type geoSearchMetadata struct {
	withDistance    bool
	withHash        bool
	withCoordinates bool
	maximumResults  int
	limited         bool
	ascending       bool
	descending      bool
}

func (m geoSearchMetadata) fields() int {
	return boolInt(m.withDistance) + boolInt(m.withHash) + boolInt(m.withCoordinates)
}

func decodeGeoSearch(codec Codec, value any, err error, metadata geoSearchMetadata) (any, error) {
	if streamCodecIsRaw(codec) {
		return decodeRawGeoSearch(value, err, metadata)
	}
	return decodeGeoSearchWithCodec(codec, value, err, metadata)
}

func decodeRawGeoSearch(value any, err error, metadata geoSearchMetadata) (any, error) {
	if err != nil {
		return nil, err
	}
	items, err := exactArrayItems(value, nil, -1, "GEOSEARCH")
	if err != nil {
		return nil, err
	}
	if err := validateGeoSearchMetadata(items, metadata); err != nil {
		return nil, err
	}
	return value, nil
}

func decodeGeoSearchWithCodec(codec Codec, value any, err error, metadata geoSearchMetadata) (any, error) {
	if err != nil {
		return nil, err
	}
	items, err := exactArrayItems(value, nil, -1, "GEOSEARCH")
	if err != nil {
		return nil, err
	}
	if err := validateGeoSearchMetadata(items, metadata); err != nil {
		return nil, err
	}
	if metadata.fields() == 0 {
		return decodeArrayExactWithCodec(codec, items, nil, -1, "GEOSEARCH")
	}
	decoded := make([]any, len(items))
	for index, item := range items {
		entry := item.([]any)
		owned := append([]any(nil), entry...)
		member, err := decodeValue(codec, owned[0])
		if err != nil {
			return nil, fmt.Errorf("decode GEOSEARCH member %d: %w", index, err)
		}
		owned[0] = member
		decoded[index] = owned
	}
	return decoded, nil
}

func validateGeoSearchMetadata(items []any, metadata geoSearchMetadata) error {
	if metadata.limited && len(items) > metadata.maximumResults {
		return fmt.Errorf("GEOSEARCH returned %d results for COUNT %d", len(items), metadata.maximumResults)
	}
	fields := metadata.fields()
	if fields == 0 {
		return nil
	}
	var previousDistance float64
	for index, item := range items {
		entry, ok := item.([]any)
		if !ok || len(entry) != fields+1 {
			return fmt.Errorf("GEOSEARCH result %d has invalid metadata shape", index)
		}
		distance, err := validateGeoSearchEntry(entry, metadata)
		if err != nil {
			return fmt.Errorf("GEOSEARCH result %d: %w", index, err)
		}
		if metadata.withDistance && index > 0 {
			if metadata.ascending && distance < previousDistance {
				return fmt.Errorf("GEOSEARCH result %d distance is not ascending", index)
			}
			if metadata.descending && distance > previousDistance {
				return fmt.Errorf("GEOSEARCH result %d distance is not descending", index)
			}
		}
		previousDistance = distance
	}
	return nil
}

func validateGeoSearchEntry(entry []any, metadata geoSearchMetadata) (float64, error) {
	field := 1
	var resultDistance float64
	if metadata.withDistance {
		distance, err := responseFloat64(entry[field], nil)
		if err != nil {
			return 0, fmt.Errorf("invalid distance: %w", err)
		}
		if distance < 0 {
			return 0, fmt.Errorf("negative distance %v", distance)
		}
		resultDistance = distance
		field++
	}
	if metadata.withHash {
		hash, err := responseInt64(entry[field], nil)
		if err != nil {
			return 0, fmt.Errorf("invalid hash: %w", err)
		}
		if hash < 0 || hash > maxGeoHash {
			return 0, fmt.Errorf("hash %d is outside valid 52-bit range", hash)
		}
		field++
	}
	if metadata.withCoordinates {
		coordinate, ok := entry[field].([]any)
		if !ok || len(coordinate) != 2 {
			return 0, errors.New("invalid coordinate shape")
		}
		longitude, err := responseFloat64(coordinate[0], nil)
		if err != nil {
			return 0, fmt.Errorf("invalid longitude: %w", err)
		}
		latitude, err := responseFloat64(coordinate[1], nil)
		if err != nil {
			return 0, fmt.Errorf("invalid latitude: %w", err)
		}
		if err := validateGeoCoordinate(longitude, latitude); err != nil {
			return 0, err
		}
	}
	return resultDistance, nil
}

func geoDistanceResponse(value any, err error) (any, error) {
	if err != nil || value == nil {
		return value, err
	}
	distance, err := responseFloat64(value, nil)
	if err != nil {
		return nil, err
	}
	if distance < 0 {
		return nil, fmt.Errorf("GEODIST returned negative distance %v", distance)
	}
	return distance, nil
}

func validateGeoPositionResponse(value any, err error, expected int) (any, error) {
	items, err := exactArrayItems(value, err, expected, "GEOPOS")
	if err != nil {
		return nil, err
	}
	for index, item := range items {
		if item == nil {
			continue
		}
		coordinate, ok := item.([]any)
		if !ok || len(coordinate) != 2 {
			return nil, fmt.Errorf("GEOPOS result %d has invalid coordinate shape", index)
		}
		longitude, err := responseFloat64(coordinate[0], nil)
		if err != nil {
			return nil, fmt.Errorf("GEOPOS longitude %d: %w", index, err)
		}
		latitude, err := responseFloat64(coordinate[1], nil)
		if err != nil {
			return nil, fmt.Errorf("GEOPOS latitude %d: %w", index, err)
		}
		if err := validateGeoCoordinate(longitude, latitude); err != nil {
			return nil, fmt.Errorf("GEOPOS result %d: %w", index, err)
		}
	}
	return value, nil
}
