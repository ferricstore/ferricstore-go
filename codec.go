package ferricstore

import (
	"encoding/json"
	"fmt"
)

// Codec transforms values stored through typed helpers. A Client serializes
// calls to custom implementations; built-in codecs remain lock-free. Callers
// that invoke Codec methods directly or share one codec across Clients must
// provide their own concurrency safety. WithConcurrentCodec can remove the
// per-Client safety wrapper when the implementation transfers ownership of
// mutable results and supports overlapping calls.
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
