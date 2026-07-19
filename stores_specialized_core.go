package ferricstore

import (
	"context"
	"fmt"
)

type BitmapStore struct{ client *Client }

func (s *BitmapStore) SetBit(ctx context.Context, key string, offset int64, value int) (int64, error) {
	if err := validateBitmapOffset(offset); err != nil {
		return 0, err
	}
	if err := validateBitValue(value); err != nil {
		return 0, err
	}
	response, err := s.client.typedReply(ctx, "SETBIT", key, offset, value)
	return boundedCountResponse("SETBIT", 1, response, err)
}

func (s *BitmapStore) GetBit(ctx context.Context, key string, offset int64) (int64, error) {
	if err := validateBitmapOffset(offset); err != nil {
		return 0, err
	}
	response, err := s.client.typedReply(ctx, "GETBIT", key, offset)
	return boundedCountResponse("GETBIT", 1, response, err)
}

func (s *BitmapStore) Count(ctx context.Context, key string) (int64, error) {
	response, err := s.client.typedReply(ctx, "BITCOUNT", key)
	return nonNegativeInt64Response("BITCOUNT", response, err)
}

type HyperLogLogStore struct{ client *Client }

func (s *HyperLogLogStore) Add(ctx context.Context, key string, elements ...any) (int64, error) {
	if len(elements) == 0 {
		return 0, fmt.Errorf("PFADD requires at least one element")
	}
	args := []any{"PFADD", key}
	for _, element := range elements {
		encoded, err := s.client.encode(element)
		if err != nil {
			return 0, err
		}
		args = append(args, encoded)
	}
	response, err := s.client.typedReply(ctx, args...)
	return boundedCountResponse("PFADD", 1, response, err)
}

func (s *HyperLogLogStore) Count(ctx context.Context, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, fmt.Errorf("PFCOUNT requires at least one key")
	}
	args := []any{"PFCOUNT"}
	for _, key := range keys {
		args = append(args, key)
	}
	response, err := s.client.typedReply(ctx, args...)
	return nonNegativeInt64Response("PFCOUNT", response, err)
}

type GeoStore struct{ client *Client }

func (s *GeoStore) Add(ctx context.Context, key string, longitude, latitude float64, member any) (int64, error) {
	if err := validateGeoCoordinate(longitude, latitude); err != nil {
		return 0, err
	}
	encoded, err := s.client.encode(member)
	if err != nil {
		return 0, err
	}
	response, err := s.client.typedReply(ctx, "GEOADD", key, longitude, latitude, encoded)
	return boundedCountResponse("GEOADD", 1, response, err)
}

func (s *GeoStore) Distance(ctx context.Context, key string, member1, member2 any, unit string) (any, error) {
	if err := validateGeoUnit(unit, true); err != nil {
		return nil, err
	}
	encoded1, err := s.client.encode(member1)
	if err != nil {
		return nil, err
	}
	encoded2, err := s.client.encode(member2)
	if err != nil {
		return nil, err
	}
	args := []any{"GEODIST", key, encoded1, encoded2}
	if unit != "" {
		args = append(args, unit)
	}
	value, err := s.client.typedReply(ctx, args...)
	return geoDistanceResponse(value, err)
}

type BloomFilterStore struct{ client *Client }

func (s *BloomFilterStore) Reserve(ctx context.Context, key string, errorRate float64, capacity int64) (bool, error) {
	if err := validateUnitInterval("BF.RESERVE", "error rate", errorRate, true); err != nil {
		return false, err
	}
	if err := validatePositiveInt64("BF.RESERVE", "capacity", capacity); err != nil {
		return false, err
	}
	if err := validateBloomSizingV080(errorRate, capacity); err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "BF.RESERVE", key, errorRate, capacity)
	return responseOK(response, err)
}

func (s *BloomFilterStore) Add(ctx context.Context, key string, element any) (bool, error) {
	encoded, err := s.client.encode(element)
	if err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "BF.ADD", key, encoded)
	return responseBool(response, err)
}

func (s *BloomFilterStore) Exists(ctx context.Context, key string, element any) (bool, error) {
	encoded, err := s.client.encode(element)
	if err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "BF.EXISTS", key, encoded)
	return responseBool(response, err)
}

type CuckooFilterStore struct{ client *Client }

func (s *CuckooFilterStore) Reserve(ctx context.Context, key string, capacity int64) (bool, error) {
	if err := validatePositiveInt64("CF.RESERVE", "capacity", capacity); err != nil {
		return false, err
	}
	if err := validateCuckooCapacityV080(capacity); err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "CF.RESERVE", key, capacity)
	return responseOK(response, err)
}

func (s *CuckooFilterStore) Add(ctx context.Context, key string, element any) (bool, error) {
	encoded, err := s.client.encode(element)
	if err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "CF.ADD", key, encoded)
	return responseBool(response, err)
}

func (s *CuckooFilterStore) Exists(ctx context.Context, key string, element any) (bool, error) {
	encoded, err := s.client.encode(element)
	if err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "CF.EXISTS", key, encoded)
	return responseBool(response, err)
}

type CountMinSketchStore struct{ client *Client }

func (s *CountMinSketchStore) InitByDim(ctx context.Context, key string, width, depth int64) (bool, error) {
	if err := validatePositiveInt64("CMS.INITBYDIM", "width", width); err != nil {
		return false, err
	}
	if err := validatePositiveInt64("CMS.INITBYDIM", "depth", depth); err != nil {
		return false, err
	}
	if err := validateCMSDimensionsV080(width, depth); err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "CMS.INITBYDIM", key, width, depth)
	return responseOK(response, err)
}

func (s *CountMinSketchStore) IncrBy(ctx context.Context, key string, item any, count int64) (any, error) {
	if err := validatePositiveInt64("CMS.INCRBY", "count", count); err != nil {
		return nil, err
	}
	encoded, err := s.client.encode(item)
	if err != nil {
		return nil, err
	}
	value, err := s.client.typedReply(ctx, "CMS.INCRBY", key, encoded, count)
	return validateCMSIncrementResponse(value, err, 1)
}

// TopKStore exposes FerricStore's bounded TopK frequency-sketch commands.
type TopKStore struct{ client *Client }

// Reserve creates a TopK sketch with FerricStore's default width and depth.
func (s *TopKStore) Reserve(ctx context.Context, key string, k int64) (bool, error) {
	if err := validatePositiveInt64("TOPK.RESERVE", "k", k); err != nil {
		return false, err
	}
	if err := validateTopKReserveV080(k, 8, 7); err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "TOPK.RESERVE", key, k)
	return responseOK(response, err)
}

// Add increments each item by one and returns its evicted item, or nil when no
// item was evicted.
func (s *TopKStore) Add(ctx context.Context, key string, items ...any) ([]any, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if err := validateProbabilisticBatch("TOPK.ADD", len(items)); err != nil {
		return nil, err
	}
	args := make([]any, 0, 2+len(items))
	args = append(args, "TOPK.ADD", key)
	for _, item := range items {
		encoded, err := s.client.encodeWithByteLimit("TOPK.ADD", "element", item, maxTopKElementBytesV080)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.typedReply(ctx, args...)
	return decodeArrayExact(s.client.codec, value, err, len(items), "TOPK.ADD")
}

type TDigestStore struct{ client *Client }

func (s *TDigestStore) Create(ctx context.Context, key string, compression *int64) (bool, error) {
	if compression != nil {
		if err := validatePositiveInt64("TDIGEST.CREATE", "compression", *compression); err != nil {
			return false, err
		}
		if err := validateTDigestCompressionV080("TDIGEST.CREATE", *compression); err != nil {
			return false, err
		}
	}
	args := []any{"TDIGEST.CREATE", key}
	appendInt64Ptr(&args, "COMPRESSION", compression)
	response, err := s.client.typedReply(ctx, args...)
	return responseOK(response, err)
}

func (s *TDigestStore) Add(ctx context.Context, key string, values ...float64) (bool, error) {
	if len(values) == 0 {
		return false, fmt.Errorf("TDIGEST.ADD requires at least one value")
	}
	if err := validateProbabilisticBatch("TDIGEST.ADD", len(values)); err != nil {
		return false, err
	}
	if err := validateFiniteValues("TDIGEST.ADD", values); err != nil {
		return false, err
	}
	args := []any{"TDIGEST.ADD", key}
	for _, value := range values {
		args = append(args, value)
	}
	response, err := s.client.typedReply(ctx, args...)
	return responseOK(response, err)
}
