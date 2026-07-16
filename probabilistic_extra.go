package ferricstore

import (
	"context"
	"errors"
)

func (s *BloomFilterStore) MAdd(ctx context.Context, key string, elements ...any) ([]bool, error) {
	if len(elements) == 0 {
		return nil, nil
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

type TopKReserveOptions struct {
	Width *int64
	Depth *int64
	Decay *float64
}

func (s *TopKStore) ReserveWithOptions(ctx context.Context, key string, k int64, opt TopKReserveOptions) (bool, error) {
	if err := validatePositiveInt64("TOPK.RESERVE", "k", k); err != nil {
		return false, err
	}
	customParameters := boolInt(opt.Width != nil) + boolInt(opt.Depth != nil) + boolInt(opt.Decay != nil)
	if customParameters != 0 && customParameters != 3 {
		return false, errors.New("TOPK.RESERVE custom width, depth, and decay must be provided together")
	}
	if opt.Width != nil {
		if err := validatePositiveInt64("TOPK.RESERVE", "width", *opt.Width); err != nil {
			return false, err
		}
		if err := validatePositiveInt64("TOPK.RESERVE", "depth", *opt.Depth); err != nil {
			return false, err
		}
		if err := validateUnitInterval("TOPK.RESERVE", "decay", *opt.Decay, false); err != nil {
			return false, err
		}
	}
	args := []any{"TOPK.RESERVE", key, k}
	if opt.Width != nil {
		args = append(args, *opt.Width)
		args = append(args, *opt.Depth)
		args = append(args, floatArg(*opt.Decay))
	}
	response, err := s.client.typedReply(ctx, args...)
	return responseOK(response, err)
}

type TopKIncrement struct {
	Item  any
	Count int64
}

func (s *TopKStore) IncrBy(ctx context.Context, key string, increments ...TopKIncrement) ([]any, error) {
	if len(increments) == 0 {
		return nil, nil
	}
	for _, increment := range increments {
		if err := validatePositiveInt64("TOPK.INCRBY", "count", increment.Count); err != nil {
			return nil, err
		}
	}
	args := []any{"TOPK.INCRBY", key}
	for _, increment := range increments {
		encoded, err := s.client.encode(increment.Item)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded, increment.Count)
	}
	value, err := s.client.typedReply(ctx, args...)
	return decodeArrayExact(s.client.codec, value, err, len(increments), "TOPK.INCRBY")
}

func (s *TopKStore) Query(ctx context.Context, key string, items ...any) ([]bool, error) {
	if len(items) == 0 {
		return nil, nil
	}
	args := []any{"TOPK.QUERY", key}
	for _, item := range items {
		encoded, err := s.client.encode(item)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.typedReply(ctx, args...)
	return boolArrayExact(value, err, len(items), "TOPK.QUERY")
}

func (s *TopKStore) List(ctx context.Context, key string, withCount bool) (any, error) {
	args := []any{"TOPK.LIST", key}
	if withCount {
		args = append(args, "WITHCOUNT")
	}
	value, err := s.client.typedReply(ctx, args...)
	if withCount {
		return decodeTopKListWithCount(s.client.codec, value, err)
	}
	return decodeArray(s.client.codec, value, err)
}

func (s *TopKStore) Count(ctx context.Context, key string, items ...any) ([]int64, error) {
	if len(items) == 0 {
		return nil, nil
	}
	args := []any{"TOPK.COUNT", key}
	for _, item := range items {
		encoded, err := s.client.encode(item)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.typedReply(ctx, args...)
	return nonNegativeIntArrayExact(value, err, len(items), "TOPK.COUNT")
}

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
	if opt.Compression != nil {
		if err := validatePositiveInt64("TDIGEST.MERGE", "compression", *opt.Compression); err != nil {
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
