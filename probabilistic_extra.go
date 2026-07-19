package ferricstore

import (
	"context"
	"errors"
)

func (s *BloomFilterStore) MAdd(ctx context.Context, key string, elements ...any) ([]bool, error) {
	if len(elements) == 0 {
		return nil, nil
	}
	if err := validateProbabilisticBatch("BF.MADD", len(elements)); err != nil {
		return nil, err
	}
	args := []any{"BF.MADD", key}
	for _, element := range elements {
		encoded, err := s.client.encode(element)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.typedReply(ctx, args...)
	return boolArrayExact(value, err, len(elements), "BF.MADD")
}

func (s *BloomFilterStore) MExists(ctx context.Context, key string, elements ...any) ([]bool, error) {
	if len(elements) == 0 {
		return nil, nil
	}
	if err := validateProbabilisticBatch("BF.MEXISTS", len(elements)); err != nil {
		return nil, err
	}
	args := []any{"BF.MEXISTS", key}
	for _, element := range elements {
		encoded, err := s.client.encode(element)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.typedReply(ctx, args...)
	return boolArrayExact(value, err, len(elements), "BF.MEXISTS")
}

func (s *BloomFilterStore) Card(ctx context.Context, key string) (int64, error) {
	value, err := s.client.typedReply(ctx, "BF.CARD", key)
	return nonNegativeInt64Response("BF.CARD", value, err)
}

func (s *BloomFilterStore) Info(ctx context.Context, key string) (map[string]any, error) {
	value, err := s.client.typedReply(ctx, "BF.INFO", key)
	if err != nil {
		return nil, err
	}
	return probabilisticInfoResponse("BF.INFO", value, bloomInfoSchema[:])
}

func (s *CuckooFilterStore) AddNX(ctx context.Context, key string, element any) (bool, error) {
	encoded, err := s.client.encode(element)
	if err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "CF.ADDNX", key, encoded)
	return responseBool(response, err)
}

func (s *CuckooFilterStore) Del(ctx context.Context, key string, element any) (bool, error) {
	encoded, err := s.client.encode(element)
	if err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "CF.DEL", key, encoded)
	return responseBool(response, err)
}

func (s *CuckooFilterStore) MExists(ctx context.Context, key string, elements ...any) ([]bool, error) {
	if len(elements) == 0 {
		return nil, nil
	}
	if err := validateProbabilisticBatch("CF.MEXISTS", len(elements)); err != nil {
		return nil, err
	}
	args := []any{"CF.MEXISTS", key}
	for _, element := range elements {
		encoded, err := s.client.encode(element)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.typedReply(ctx, args...)
	return boolArrayExact(value, err, len(elements), "CF.MEXISTS")
}

func (s *CuckooFilterStore) Count(ctx context.Context, key string, element any) (int64, error) {
	encoded, err := s.client.encode(element)
	if err != nil {
		return 0, err
	}
	value, err := s.client.typedReply(ctx, "CF.COUNT", key, encoded)
	return nonNegativeInt64Response("CF.COUNT", value, err)
}

func (s *CuckooFilterStore) Info(ctx context.Context, key string) (map[string]any, error) {
	value, err := s.client.typedReply(ctx, "CF.INFO", key)
	if err != nil {
		return nil, err
	}
	return probabilisticInfoResponse("CF.INFO", value, cuckooInfoSchema[:])
}

func (s *CountMinSketchStore) InitByProb(ctx context.Context, key string, errorRate, probability float64) (bool, error) {
	if err := validatePositiveFinite("CMS.INITBYPROB", "error", errorRate); err != nil {
		return false, err
	}
	if err := validateUnitInterval("CMS.INITBYPROB", "probability", probability, true); err != nil {
		return false, err
	}
	if err := validateCMSProbabilityV080(errorRate, probability); err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "CMS.INITBYPROB", key, floatArg(errorRate), floatArg(probability))
	return responseOK(response, err)
}

type CMSIncrement struct {
	Item  any
	Count int64
}

func (s *CountMinSketchStore) IncrByMany(ctx context.Context, key string, increments ...CMSIncrement) ([]int64, error) {
	if len(increments) == 0 {
		return nil, nil
	}
	if err := validateProbabilisticBatch("CMS.INCRBY", len(increments)); err != nil {
		return nil, err
	}
	for _, increment := range increments {
		if err := validatePositiveInt64("CMS.INCRBY", "count", increment.Count); err != nil {
			return nil, err
		}
	}
	args := []any{"CMS.INCRBY", key}
	for _, increment := range increments {
		encoded, err := s.client.encode(increment.Item)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded, increment.Count)
	}
	value, err := s.client.typedReply(ctx, args...)
	return nonNegativeIntArrayExact(value, err, len(increments), "CMS.INCRBY")
}

func (s *CountMinSketchStore) Query(ctx context.Context, key string, items ...any) ([]int64, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if err := validateProbabilisticBatch("CMS.QUERY", len(items)); err != nil {
		return nil, err
	}
	args := []any{"CMS.QUERY", key}
	for _, item := range items {
		encoded, err := s.client.encode(item)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.typedReply(ctx, args...)
	return nonNegativeIntArrayExact(value, err, len(items), "CMS.QUERY")
}

type CMSMergeOptions struct {
	Sources []string
	Weights []int64
}

func (s *CountMinSketchStore) Merge(ctx context.Context, destination string, opt CMSMergeOptions) (bool, error) {
	if len(opt.Sources) == 0 {
		return false, errors.New("CMS.MERGE requires at least one source")
	}
	if len(opt.Weights) != 0 && len(opt.Weights) != len(opt.Sources) {
		return false, errors.New("CMS.MERGE requires one weight per source")
	}
	if err := validateCMSMergeSourceCountV080(len(opt.Sources)); err != nil {
		return false, err
	}
	args := []any{"CMS.MERGE", destination, len(opt.Sources)}
	for _, source := range opt.Sources {
		args = append(args, source)
	}
	if len(opt.Weights) > 0 {
		args = append(args, "WEIGHTS")
		for _, weight := range opt.Weights {
			args = append(args, weight)
		}
	}
	response, err := s.client.typedReply(ctx, args...)
	return responseOK(response, err)
}

func (s *CountMinSketchStore) Info(ctx context.Context, key string) (map[string]any, error) {
	value, err := s.client.typedReply(ctx, "CMS.INFO", key)
	if err != nil {
		return nil, err
	}
	return probabilisticInfoResponse("CMS.INFO", value, cmsInfoSchema[:])
}

// TopKReserveOptions configures the optional width and depth accepted by
// FerricStore 0.8. Width and Depth must either both be set or both be nil.
type TopKReserveOptions struct {
	// Width is the number of counters in each sketch row.
	Width *int64
	// Depth is the number of sketch rows.
	Depth *int64
}

// ReserveWithOptions creates a TopK sketch with an explicit width and depth.
func (s *TopKStore) ReserveWithOptions(ctx context.Context, key string, k int64, opt TopKReserveOptions) (bool, error) {
	if err := validatePositiveInt64("TOPK.RESERVE", "k", k); err != nil {
		return false, err
	}
	if (opt.Width == nil) != (opt.Depth == nil) {
		return false, errors.New("TOPK.RESERVE custom width and depth must be provided together")
	}
	if opt.Width != nil {
		if err := validatePositiveInt64("TOPK.RESERVE", "width", *opt.Width); err != nil {
			return false, err
		}
		if err := validatePositiveInt64("TOPK.RESERVE", "depth", *opt.Depth); err != nil {
			return false, err
		}
	}
	width, depth := int64(8), int64(7)
	if opt.Width != nil {
		width, depth = *opt.Width, *opt.Depth
	}
	if err := validateTopKReserveV080(k, width, depth); err != nil {
		return false, err
	}
	argumentCapacity := 3
	if opt.Width != nil {
		argumentCapacity = 5
	}
	args := make([]any, 0, argumentCapacity)
	args = append(args, "TOPK.RESERVE", key, k)
	if opt.Width != nil {
		args = append(args, *opt.Width, *opt.Depth)
	}
	response, err := s.client.typedReply(ctx, args...)
	return responseOK(response, err)
}

// TopKIncrement associates an item with a positive increment count.
type TopKIncrement struct {
	// Item is encoded with the Client's configured Codec.
	Item any
	// Count is the positive amount added to Item's estimated frequency.
	Count int64
}

// IncrBy applies item-specific increments and returns each evicted item, or nil
// when that increment did not evict an item.
func (s *TopKStore) IncrBy(ctx context.Context, key string, increments ...TopKIncrement) ([]any, error) {
	if len(increments) == 0 {
		return nil, nil
	}
	if err := validateProbabilisticBatch("TOPK.INCRBY", len(increments)); err != nil {
		return nil, err
	}
	for _, increment := range increments {
		if err := validatePositiveInt64("TOPK.INCRBY", "count", increment.Count); err != nil {
			return nil, err
		}
	}
	args := make([]any, 0, 2+2*len(increments))
	args = append(args, "TOPK.INCRBY", key)
	for _, increment := range increments {
		encoded, err := s.client.encodeWithByteLimit("TOPK.INCRBY", "element", increment.Item, maxTopKElementBytesV080)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded, increment.Count)
	}
	value, err := s.client.typedReply(ctx, args...)
	return decodeArrayExact(s.client.codec, value, err, len(increments), "TOPK.INCRBY")
}

// Query reports whether each requested item is currently tracked by the sketch.
func (s *TopKStore) Query(ctx context.Context, key string, items ...any) ([]bool, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if err := validateProbabilisticBatch("TOPK.QUERY", len(items)); err != nil {
		return nil, err
	}
	args := make([]any, 0, 2+len(items))
	args = append(args, "TOPK.QUERY", key)
	for _, item := range items {
		encoded, err := s.client.encodeWithByteLimit("TOPK.QUERY", "element", item, maxTopKElementBytesV080)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.typedReply(ctx, args...)
	return boolArrayExact(value, err, len(items), "TOPK.QUERY")
}

// List returns the ranked TopK items without their estimated counts.
func (s *TopKStore) List(ctx context.Context, key string) ([]any, error) {
	value, err := s.client.typedReply(ctx, "TOPK.LIST", key)
	return decodeTopKList(s.client.codec, value, err)
}

// ListWithCount returns the ranked TopK items and their estimated counts.
func (s *TopKStore) ListWithCount(ctx context.Context, key string) ([]TopKEntry, error) {
	value, err := s.client.typedReply(ctx, "TOPK.LIST", key, "WITHCOUNT")
	return decodeTopKListWithCount(s.client.codec, value, err)
}

// Count returns the estimated frequency of each requested item.
func (s *TopKStore) Count(ctx context.Context, key string, items ...any) ([]int64, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if err := validateProbabilisticBatch("TOPK.COUNT", len(items)); err != nil {
		return nil, err
	}
	args := make([]any, 0, 2+len(items))
	args = append(args, "TOPK.COUNT", key)
	for _, item := range items {
		encoded, err := s.client.encodeWithByteLimit("TOPK.COUNT", "element", item, maxTopKElementBytesV080)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.typedReply(ctx, args...)
	return nonNegativeIntArrayExact(value, err, len(items), "TOPK.COUNT")
}

// Info returns the sketch's k, width, and depth fields.
func (s *TopKStore) Info(ctx context.Context, key string) (map[string]any, error) {
	value, err := s.client.typedReply(ctx, "TOPK.INFO", key)
	if err != nil {
		return nil, err
	}
	return probabilisticInfoResponse("TOPK.INFO", value, topKInfoSchema[:])
}

func (s *TDigestStore) Reset(ctx context.Context, key string) (bool, error) {
	response, err := s.client.typedReply(ctx, "TDIGEST.RESET", key)
	return responseOK(response, err)
}

func (s *TDigestStore) Quantile(ctx context.Context, key string, quantiles ...float64) ([]float64, error) {
	return s.tdigestFloatQuery(ctx, "TDIGEST.QUANTILE", key, quantiles...)
}

func (s *TDigestStore) CDF(ctx context.Context, key string, values ...float64) ([]float64, error) {
	return s.tdigestFloatQuery(ctx, "TDIGEST.CDF", key, values...)
}

func (s *TDigestStore) Rank(ctx context.Context, key string, values ...float64) ([]int64, error) {
	return s.tdigestIntQuery(ctx, "TDIGEST.RANK", key, values...)
}

func (s *TDigestStore) RevRank(ctx context.Context, key string, values ...float64) ([]int64, error) {
	return s.tdigestIntQuery(ctx, "TDIGEST.REVRANK", key, values...)
}

func (s *TDigestStore) ByRank(ctx context.Context, key string, ranks ...int64) ([]float64, error) {
	if len(ranks) == 0 {
		return nil, nil
	}
	if err := validateProbabilisticBatch("TDIGEST.BYRANK", len(ranks)); err != nil {
		return nil, err
	}
	args := []any{"TDIGEST.BYRANK", key}
	for _, rank := range ranks {
		args = append(args, rank)
	}
	value, err := s.client.typedReply(ctx, args...)
	return nonFiniteFloatArrayExact(value, err, len(ranks), "TDIGEST.BYRANK")
}

func (s *TDigestStore) ByRevRank(ctx context.Context, key string, ranks ...int64) ([]float64, error) {
	if len(ranks) == 0 {
		return nil, nil
	}
	if err := validateProbabilisticBatch("TDIGEST.BYREVRANK", len(ranks)); err != nil {
		return nil, err
	}
	args := []any{"TDIGEST.BYREVRANK", key}
	for _, rank := range ranks {
		args = append(args, rank)
	}
	value, err := s.client.typedReply(ctx, args...)
	return nonFiniteFloatArrayExact(value, err, len(ranks), "TDIGEST.BYREVRANK")
}

func (s *TDigestStore) TrimmedMean(ctx context.Context, key string, low, high float64) (float64, error) {
	if err := validateQuantiles("TDIGEST.TRIMMED_MEAN", []float64{low, high}); err != nil {
		return 0, err
	}
	if low >= high {
		return 0, errors.New("TDIGEST.TRIMMED_MEAN low quantile must be less than high quantile")
	}
	value, err := s.client.typedReply(ctx, "TDIGEST.TRIMMED_MEAN", key, floatArg(low), floatArg(high))
	return tdigestFiniteOrNaNResponse(value, err, "TDIGEST.TRIMMED_MEAN")
}

func (s *TDigestStore) Min(ctx context.Context, key string) (float64, error) {
	value, err := s.client.typedReply(ctx, "TDIGEST.MIN", key)
	return tdigestFiniteOrNaNResponse(value, err, "TDIGEST.MIN")
}

func (s *TDigestStore) Max(ctx context.Context, key string) (float64, error) {
	value, err := s.client.typedReply(ctx, "TDIGEST.MAX", key)
	return tdigestFiniteOrNaNResponse(value, err, "TDIGEST.MAX")
}

func (s *TDigestStore) Info(ctx context.Context, key string) (map[string]any, error) {
	value, err := s.client.typedReply(ctx, "TDIGEST.INFO", key)
	if err != nil {
		return nil, err
	}
	return probabilisticInfoResponse("TDIGEST.INFO", value, tDigestInfoSchema[:])
}

type TDigestMergeOptions struct {
	Sources     []string
	Compression *int64
	Override    bool
}

func (s *TDigestStore) Merge(ctx context.Context, destination string, opt TDigestMergeOptions) (bool, error) {
	if len(opt.Sources) == 0 {
		return false, errors.New("TDIGEST.MERGE requires at least one source")
	}
	if err := validateTDigestMergeSourceCountV080(len(opt.Sources)); err != nil {
		return false, err
	}
	if opt.Compression != nil {
		if err := validatePositiveInt64("TDIGEST.MERGE", "compression", *opt.Compression); err != nil {
			return false, err
		}
		if err := validateTDigestCompressionV080("TDIGEST.MERGE", *opt.Compression); err != nil {
			return false, err
		}
	}
	args := []any{"TDIGEST.MERGE", destination, len(opt.Sources)}
	for _, source := range opt.Sources {
		args = append(args, source)
	}
	appendInt64Ptr(&args, "COMPRESSION", opt.Compression)
	if opt.Override {
		args = append(args, "OVERRIDE")
	}
	response, err := s.client.typedReply(ctx, args...)
	return responseOK(response, err)
}

func (s *TDigestStore) tdigestFloatQuery(ctx context.Context, command, key string, values ...float64) ([]float64, error) {
	if len(values) == 0 {
		return nil, nil
	}
	if err := validateProbabilisticBatch(command, len(values)); err != nil {
		return nil, err
	}
	if command == "TDIGEST.QUANTILE" {
		if err := validateQuantiles(command, values); err != nil {
			return nil, err
		}
	} else if err := validateFiniteValues(command, values); err != nil {
		return nil, err
	}
	args := []any{command, key}
	for _, value := range values {
		args = append(args, floatArg(value))
	}
	value, err := s.client.typedReply(ctx, args...)
	return tdigestFloatArray(value, err, len(values), command, command == "TDIGEST.CDF")
}

func (s *TDigestStore) tdigestIntQuery(ctx context.Context, command, key string, values ...float64) ([]int64, error) {
	if len(values) == 0 {
		return nil, nil
	}
	if err := validateProbabilisticBatch(command, len(values)); err != nil {
		return nil, err
	}
	if err := validateFiniteValues(command, values); err != nil {
		return nil, err
	}
	args := []any{command, key}
	for _, value := range values {
		args = append(args, floatArg(value))
	}
	response, err := s.client.typedReply(ctx, args...)
	return tdigestRankArray(response, err, len(values), command)
}
