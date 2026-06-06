package ferricstore

import (
	"encoding/json"
	"fmt"
)

type Codec interface {
	Encode(value any) (any, error)
	Decode(value any) (any, error)
}

type RawCodec struct{}

func (RawCodec) Encode(value any) (any, error) {
	return value, nil
}

func (RawCodec) Decode(value any) (any, error) {
	return value, nil
}

type StringCodec struct{}

func (StringCodec) Encode(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	return fmt.Sprint(value), nil
}

func (StringCodec) Decode(value any) (any, error) {
	return asString(value), nil
}

type JSONCodec struct{}

func (JSONCodec) Encode(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	return json.Marshal(value)
}

func (JSONCodec) Decode(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	var decoded any
	if err := json.Unmarshal(asBytes(value), &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}
