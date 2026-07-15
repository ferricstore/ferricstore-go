package ferricstore

import (
	"fmt"
	"sync"
)

// serializedCodec keeps custom Codec implementations safe when one Client is
// used by multiple goroutines. Built-in codecs bypass this wrapper.
type serializedCodec struct {
	mu    sync.Mutex
	codec Codec
}

type concurrentCodec struct {
	codec Codec
}

func codecIsNil(codec Codec) bool {
	return interfaceIsNil(codec)
}

func (c *concurrentCodec) Encode(value any) (any, error) {
	return c.codec.Encode(value)
}

func (c *concurrentCodec) Decode(value any) (any, error) {
	return c.codec.Decode(value)
}

func (c *serializedCodec) Encode(value any) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	encoded, err := c.codec.Encode(value)
	return snapshotCodecOutput("encode", encoded, err)
}

func (c *serializedCodec) Decode(value any) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	decoded, err := c.codec.Decode(value)
	return snapshotCodecOutput("decode", decoded, err)
}

func snapshotCodecOutput(operation string, value any, err error) (any, error) {
	if err != nil {
		return nil, err
	}
	var visiting map[commandCloneVisit]struct{}
	owned, err := snapshotCommandArg(value, &visiting)
	if err != nil {
		return nil, fmt.Errorf("ferricstore codec %s returned mutable data that cannot be owned safely: %w", operation, err)
	}
	return owned, nil
}

func codecForClient(codec Codec) Codec {
	switch codec.(type) {
	case RawCodec, *RawCodec, StringCodec, *StringCodec, JSONCodec, *JSONCodec, *serializedCodec, *concurrentCodec:
		return codec
	default:
		return &serializedCodec{codec: codec}
	}
}

func concurrentCodecForClient(codec Codec) Codec {
	switch codec.(type) {
	case RawCodec, *RawCodec, StringCodec, *StringCodec, JSONCodec, *JSONCodec, *concurrentCodec:
		return codec
	default:
		return &concurrentCodec{codec: codec}
	}
}

func originalCodec(codec Codec) Codec {
	if serialized, ok := codec.(*serializedCodec); ok {
		return serialized.codec
	}
	if concurrent, ok := codec.(*concurrentCodec); ok {
		return concurrent.codec
	}
	return codec
}

func codecNeedsDeterministicMapOrder(codec Codec) bool {
	switch originalCodec(codec).(type) {
	case RawCodec, *RawCodec, StringCodec, *StringCodec, JSONCodec, *JSONCodec:
		return false
	default:
		return true
	}
}
