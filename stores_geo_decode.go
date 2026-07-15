package ferricstore

import "fmt"

func decodeGeoSearch(codec Codec, value any, err error, metadataFields int) (any, error) {
	if streamCodecIsRaw(codec) {
		return decodeRawGeoSearch(value, err, metadataFields)
	}
	return decodeGeoSearchWithCodec(codec, value, err, metadataFields)
}

func decodeRawGeoSearch(value any, err error, metadataFields int) (any, error) {
	if err != nil {
		return nil, err
	}
	items, err := exactArrayItems(value, nil, -1, "GEOSEARCH")
	if err != nil {
		return nil, err
	}
	if metadataFields > 0 {
		for index, item := range items {
			entry, ok := item.([]any)
			if !ok || len(entry) != metadataFields+1 {
				return nil, fmt.Errorf("GEOSEARCH result %d has invalid metadata shape", index)
			}
		}
	}
	return value, nil
}

func decodeGeoSearchWithCodec(codec Codec, value any, err error, metadataFields int) (any, error) {
	if err != nil {
		return nil, err
	}
	items, err := exactArrayItems(value, nil, -1, "GEOSEARCH")
	if err != nil {
		return nil, err
	}
	if metadataFields == 0 {
		return decodeArrayExactWithCodec(codec, items, nil, -1, "GEOSEARCH")
	}
	decoded := make([]any, len(items))
	for index, item := range items {
		entry, ok := item.([]any)
		if !ok || len(entry) != metadataFields+1 {
			return nil, fmt.Errorf("GEOSEARCH result %d has invalid metadata shape", index)
		}
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
