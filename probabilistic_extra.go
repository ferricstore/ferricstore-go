package ferricstore

import "context"

func (s *BloomFilterStore) MAdd(ctx context.Context, key string, elements ...any) ([]bool, error) {
	args := []any{"BF.MADD", key}
	for _, element := range elements {
		encoded, err := s.client.encode(element)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.Command(ctx, args...)
	return boolArray(value, err)
}

func (s *BloomFilterStore) MExists(ctx context.Context, key string, elements ...any) ([]bool, error) {
	args := []any{"BF.MEXISTS", key}
	for _, element := range elements {
		encoded, err := s.client.encode(element)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.Command(ctx, args...)
	return boolArray(value, err)
}

func (s *BloomFilterStore) Card(ctx context.Context, key string) (int64, error) {
	value, err := s.client.Command(ctx, "BF.CARD", key)
	return asInt64(value), err
}

func (s *BloomFilterStore) Info(ctx context.Context, key string) (map[string]any, error) {
	value, err := s.client.Command(ctx, "BF.INFO", key)
	if err != nil {
		return nil, err
	}
	return respMap(value)
}

func (s *CuckooFilterStore) AddNX(ctx context.Context, key string, element any) (bool, error) {
	encoded, err := s.client.encode(element)
	if err != nil {
		return false, err
	}
	response, err := s.client.Command(ctx, "CF.ADDNX", key, encoded)
	return asBool(response), err
}

func (s *CuckooFilterStore) Del(ctx context.Context, key string, element any) (bool, error) {
	encoded, err := s.client.encode(element)
	if err != nil {
		return false, err
	}
	response, err := s.client.Command(ctx, "CF.DEL", key, encoded)
	return asBool(response), err
}

func (s *CuckooFilterStore) MExists(ctx context.Context, key string, elements ...any) ([]bool, error) {
	args := []any{"CF.MEXISTS", key}
	for _, element := range elements {
		encoded, err := s.client.encode(element)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.Command(ctx, args...)
	return boolArray(value, err)
}

func (s *CuckooFilterStore) Count(ctx context.Context, key string, element any) (int64, error) {
	encoded, err := s.client.encode(element)
	if err != nil {
		return 0, err
	}
	value, err := s.client.Command(ctx, "CF.COUNT", key, encoded)
	return asInt64(value), err
}

func (s *CuckooFilterStore) Info(ctx context.Context, key string) (map[string]any, error) {
	value, err := s.client.Command(ctx, "CF.INFO", key)
	if err != nil {
		return nil, err
	}
	return respMap(value)
}

func (s *CountMinSketchStore) InitByProb(ctx context.Context, key string, errorRate, probability float64) (bool, error) {
	response, err := s.client.Command(ctx, "CMS.INITBYPROB", key, floatArg(errorRate), floatArg(probability))
	return isOK(response), err
}

type CMSIncrement struct {
	Item  any
	Count int64
}

func (s *CountMinSketchStore) IncrByMany(ctx context.Context, key string, increments ...CMSIncrement) ([]int64, error) {
	args := []any{"CMS.INCRBY", key}
	for _, increment := range increments {
		encoded, err := s.client.encode(increment.Item)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded, increment.Count)
	}
	value, err := s.client.Command(ctx, args...)
	return intArray(value, err)
}

func (s *CountMinSketchStore) Query(ctx context.Context, key string, items ...any) ([]int64, error) {
	args := []any{"CMS.QUERY", key}
	for _, item := range items {
		encoded, err := s.client.encode(item)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.Command(ctx, args...)
	return intArray(value, err)
}

type CMSMergeOptions struct {
	Sources []string
	Weights []int64
}

func (s *CountMinSketchStore) Merge(ctx context.Context, destination string, opt CMSMergeOptions) (bool, error) {
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
	response, err := s.client.Command(ctx, args...)
	return isOK(response), err
}

func (s *CountMinSketchStore) Info(ctx context.Context, key string) (map[string]any, error) {
	value, err := s.client.Command(ctx, "CMS.INFO", key)
	if err != nil {
		return nil, err
	}
	return respMap(value)
}

type TopKReserveOptions struct {
	Width *int64
	Depth *int64
	Decay *float64
}

func (s *TopKStore) ReserveWithOptions(ctx context.Context, key string, k int64, opt TopKReserveOptions) (bool, error) {
	args := []any{"TOPK.RESERVE", key, k}
	if opt.Width != nil {
		args = append(args, *opt.Width)
		if opt.Depth != nil {
			args = append(args, *opt.Depth)
			if opt.Decay != nil {
				args = append(args, floatArg(*opt.Decay))
			}
		}
	}
	response, err := s.client.Command(ctx, args...)
	return isOK(response), err
}

type TopKIncrement struct {
	Item  any
	Count int64
}

func (s *TopKStore) IncrBy(ctx context.Context, key string, increments ...TopKIncrement) ([]any, error) {
	args := []any{"TOPK.INCRBY", key}
	for _, increment := range increments {
		encoded, err := s.client.encode(increment.Item)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded, increment.Count)
	}
	value, err := s.client.Command(ctx, args...)
	return decodeArray(s.client.codec, value, err)
}

func (s *TopKStore) Query(ctx context.Context, key string, items ...any) ([]bool, error) {
	args := []any{"TOPK.QUERY", key}
	for _, item := range items {
		encoded, err := s.client.encode(item)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.Command(ctx, args...)
	return boolArray(value, err)
}

func (s *TopKStore) List(ctx context.Context, key string, withCount bool) (any, error) {
	args := []any{"TOPK.LIST", key}
	if withCount {
		args = append(args, "WITHCOUNT")
	}
	return s.client.Command(ctx, args...)
}

func (s *TopKStore) Count(ctx context.Context, key string, items ...any) ([]int64, error) {
	args := []any{"TOPK.COUNT", key}
	for _, item := range items {
		encoded, err := s.client.encode(item)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.Command(ctx, args...)
	return intArray(value, err)
}

func (s *TopKStore) Info(ctx context.Context, key string) (map[string]any, error) {
	value, err := s.client.Command(ctx, "TOPK.INFO", key)
	if err != nil {
		return nil, err
	}
	return respMap(value)
}

func (s *TDigestStore) Reset(ctx context.Context, key string) (bool, error) {
	response, err := s.client.Command(ctx, "TDIGEST.RESET", key)
	return isOK(response), err
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
	args := []any{"TDIGEST.BYRANK", key}
	for _, rank := range ranks {
		args = append(args, rank)
	}
	value, err := s.client.Command(ctx, args...)
	return floatArray(value, err)
}

func (s *TDigestStore) ByRevRank(ctx context.Context, key string, ranks ...int64) ([]float64, error) {
	args := []any{"TDIGEST.BYREVRANK", key}
	for _, rank := range ranks {
		args = append(args, rank)
	}
	value, err := s.client.Command(ctx, args...)
	return floatArray(value, err)
}

func (s *TDigestStore) TrimmedMean(ctx context.Context, key string, low, high float64) (float64, error) {
	value, err := s.client.Command(ctx, "TDIGEST.TRIMMED_MEAN", key, floatArg(low), floatArg(high))
	return asFloat64(value), err
}

func (s *TDigestStore) Min(ctx context.Context, key string) (float64, error) {
	value, err := s.client.Command(ctx, "TDIGEST.MIN", key)
	return asFloat64(value), err
}

func (s *TDigestStore) Max(ctx context.Context, key string) (float64, error) {
	value, err := s.client.Command(ctx, "TDIGEST.MAX", key)
	return asFloat64(value), err
}

func (s *TDigestStore) Info(ctx context.Context, key string) (map[string]any, error) {
	value, err := s.client.Command(ctx, "TDIGEST.INFO", key)
	if err != nil {
		return nil, err
	}
	return respMap(value)
}

type TDigestMergeOptions struct {
	Sources     []string
	Compression *int64
	Override    bool
}

func (s *TDigestStore) Merge(ctx context.Context, destination string, opt TDigestMergeOptions) (bool, error) {
	args := []any{"TDIGEST.MERGE", destination, len(opt.Sources)}
	for _, source := range opt.Sources {
		args = append(args, source)
	}
	appendInt64Ptr(&args, "COMPRESSION", opt.Compression)
	if opt.Override {
		args = append(args, "OVERRIDE")
	}
	response, err := s.client.Command(ctx, args...)
	return isOK(response), err
}

func (s *TDigestStore) tdigestFloatQuery(ctx context.Context, command, key string, values ...float64) ([]float64, error) {
	args := []any{command, key}
	for _, value := range values {
		args = append(args, floatArg(value))
	}
	value, err := s.client.Command(ctx, args...)
	return floatArray(value, err)
}

func (s *TDigestStore) tdigestIntQuery(ctx context.Context, command, key string, values ...float64) ([]int64, error) {
	args := []any{command, key}
	for _, value := range values {
		args = append(args, floatArg(value))
	}
	response, err := s.client.Command(ctx, args...)
	return intArray(response, err)
}
