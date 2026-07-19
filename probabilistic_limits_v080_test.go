package ferricstore

import (
	"context"
	"math"
	"strings"
	"testing"
)

func TestV080ProbabilisticHardLimitsRejectBeforeTransport(t *testing.T) {
	const oversizedBatch = 10_001
	items := make([]any, oversizedBatch)
	for index := range items {
		items[index] = "item"
	}
	cmsIncrements := make([]CMSIncrement, oversizedBatch)
	topKIncrements := make([]TopKIncrement, oversizedBatch)
	for index := 0; index < oversizedBatch; index++ {
		cmsIncrements[index] = CMSIncrement{Item: "item", Count: 1}
		topKIncrements[index] = TopKIncrement{Item: "item", Count: 1}
	}
	values := make([]float64, oversizedBatch)
	ranks := make([]int64, oversizedBatch)
	sources := make([]string, oversizedBatch)
	for index := range sources {
		sources[index] = "source"
	}
	cmsSources := sources[:129]
	tooLargeCompression := int64(1_001)
	tooLargeTopKItem := strings.Repeat("x", 253)

	tests := []struct {
		name       string
		wantEncode int64
		call       func(*Client) error
	}{
		{name: "BF.MADD batch", call: func(client *Client) error {
			_, err := client.Bloom().MAdd(context.Background(), "key", items...)
			return err
		}},
		{name: "BF.MEXISTS batch", call: func(client *Client) error {
			_, err := client.Bloom().MExists(context.Background(), "key", items...)
			return err
		}},
		{name: "CF.RESERVE capacity", call: func(client *Client) error {
			_, err := client.Cuckoo().Reserve(context.Background(), "key", 1_073_741_825)
			return err
		}},
		{name: "CF.MEXISTS batch", call: func(client *Client) error {
			_, err := client.Cuckoo().MExists(context.Background(), "key", items...)
			return err
		}},
		{name: "CMS.INITBYDIM depth", call: func(client *Client) error {
			_, err := client.CountMinSketch().InitByDim(context.Background(), "key", 1, 1_025)
			return err
		}},
		{name: "CMS.INITBYDIM counters", call: func(client *Client) error {
			_, err := client.CountMinSketch().InitByDim(context.Background(), "key", 8_388_609, 2)
			return err
		}},
		{name: "CMS.INITBYPROB counters", call: func(client *Client) error {
			_, err := client.CountMinSketch().InitByProb(context.Background(), "key", math.SmallestNonzeroFloat64, 0.1)
			return err
		}},
		{name: "CMS.INCRBY batch", call: func(client *Client) error {
			_, err := client.CountMinSketch().IncrByMany(context.Background(), "key", cmsIncrements...)
			return err
		}},
		{name: "CMS.QUERY batch", call: func(client *Client) error {
			_, err := client.CountMinSketch().Query(context.Background(), "key", items...)
			return err
		}},
		{name: "CMS.MERGE sources", call: func(client *Client) error {
			_, err := client.CountMinSketch().Merge(context.Background(), "dest", CMSMergeOptions{Sources: cmsSources})
			return err
		}},
		{name: "TOPK.RESERVE k", call: func(client *Client) error {
			_, err := client.TopK().Reserve(context.Background(), "key", 100_001)
			return err
		}},
		{name: "TOPK.RESERVE counters", call: func(client *Client) error {
			width, depth := int64(524_289), int64(2)
			_, err := client.TopK().ReserveWithOptions(context.Background(), "key", 1, TopKReserveOptions{Width: &width, Depth: &depth})
			return err
		}},
		{name: "TOPK.ADD batch", call: func(client *Client) error {
			_, err := client.TopK().Add(context.Background(), "key", items...)
			return err
		}},
		{name: "TOPK.INCRBY batch", call: func(client *Client) error {
			_, err := client.TopK().IncrBy(context.Background(), "key", topKIncrements...)
			return err
		}},
		{name: "TOPK.QUERY batch", call: func(client *Client) error {
			_, err := client.TopK().Query(context.Background(), "key", items...)
			return err
		}},
		{name: "TOPK.COUNT batch", call: func(client *Client) error {
			_, err := client.TopK().Count(context.Background(), "key", items...)
			return err
		}},
		{name: "TOPK.ADD element bytes", wantEncode: 1, call: func(client *Client) error {
			_, err := client.TopK().Add(context.Background(), "key", tooLargeTopKItem)
			return err
		}},
		{name: "TOPK.INCRBY element bytes", wantEncode: 1, call: func(client *Client) error {
			_, err := client.TopK().IncrBy(context.Background(), "key", TopKIncrement{Item: tooLargeTopKItem, Count: 1})
			return err
		}},
		{name: "TOPK.QUERY element bytes", wantEncode: 1, call: func(client *Client) error {
			_, err := client.TopK().Query(context.Background(), "key", tooLargeTopKItem)
			return err
		}},
		{name: "TOPK.COUNT element bytes", wantEncode: 1, call: func(client *Client) error {
			_, err := client.TopK().Count(context.Background(), "key", tooLargeTopKItem)
			return err
		}},
		{name: "TDIGEST.CREATE compression", call: func(client *Client) error {
			_, err := client.TDigest().Create(context.Background(), "key", &tooLargeCompression)
			return err
		}},
		{name: "TDIGEST.ADD batch", call: func(client *Client) error {
			_, err := client.TDigest().Add(context.Background(), "key", values...)
			return err
		}},
		{name: "TDIGEST.QUANTILE batch", call: func(client *Client) error {
			_, err := client.TDigest().Quantile(context.Background(), "key", values...)
			return err
		}},
		{name: "TDIGEST.CDF batch", call: func(client *Client) error {
			_, err := client.TDigest().CDF(context.Background(), "key", values...)
			return err
		}},
		{name: "TDIGEST.RANK batch", call: func(client *Client) error {
			_, err := client.TDigest().Rank(context.Background(), "key", values...)
			return err
		}},
		{name: "TDIGEST.REVRANK batch", call: func(client *Client) error {
			_, err := client.TDigest().RevRank(context.Background(), "key", values...)
			return err
		}},
		{name: "TDIGEST.BYRANK batch", call: func(client *Client) error {
			_, err := client.TDigest().ByRank(context.Background(), "key", ranks...)
			return err
		}},
		{name: "TDIGEST.BYREVRANK batch", call: func(client *Client) error {
			_, err := client.TDigest().ByRevRank(context.Background(), "key", ranks...)
			return err
		}},
		{name: "TDIGEST.MERGE sources", call: func(client *Client) error {
			_, err := client.TDigest().Merge(context.Background(), "dest", TDigestMergeOptions{Sources: sources})
			return err
		}},
		{name: "TDIGEST.MERGE compression", call: func(client *Client) error {
			_, err := client.TDigest().Merge(context.Background(), "dest", TDigestMergeOptions{Sources: []string{"source"}, Compression: &tooLargeCompression})
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			codec := &countingKVCodec{}
			err := test.call(NewClientWithExecutor(exec, WithConcurrentCodec(codec)))
			if err == nil {
				t.Fatal("request exceeding the FerricStore 0.8 limit succeeded")
			}
			if got := codec.encodes.Load(); got != test.wantEncode {
				t.Fatalf("codec calls = %d, want %d", got, test.wantEncode)
			}
			if len(exec.calls) != 0 {
				t.Fatalf("request exceeding the FerricStore 0.8 limit reached transport: %#v", exec.calls[0])
			}
		})
	}
}

func TestV080ProbabilisticHardLimitBoundaries(t *testing.T) {
	if err := validateProbabilisticBatch("TEST", maxProbabilisticBatchItemsV080); err != nil {
		t.Fatalf("maximum batch rejected: %v", err)
	}
	if err := validateCuckooCapacityV080(maxCuckooCapacityV080); err != nil {
		t.Fatalf("maximum Cuckoo capacity rejected: %v", err)
	}
	if err := validateCMSDimensionsV080(maxCMSCountersV080/maxCMSDepthV080, maxCMSDepthV080); err != nil {
		t.Fatalf("maximum CMS dimensions rejected: %v", err)
	}
	if err := validateCMSMergeSourceCountV080(maxCMSMergeSourcesV080); err != nil {
		t.Fatalf("maximum CMS merge source count rejected: %v", err)
	}
	if err := validateTopKReserveV080(maxTopKKV080, maxTopKCountersV080/8, 8); err != nil {
		t.Fatalf("maximum TopK reservation rejected: %v", err)
	}
	if err := validateTDigestCompressionV080("TDIGEST.CREATE", maxTDigestCompressionV080); err != nil {
		t.Fatalf("maximum t-digest compression rejected: %v", err)
	}
	if err := validateTDigestMergeSourceCountV080(maxTDigestMergeSourcesV080); err != nil {
		t.Fatalf("maximum t-digest source count rejected: %v", err)
	}
	if err := validateEncodedByteLimit("TOPK.ADD", "element", strings.Repeat("x", maxTopKElementBytesV080), maxTopKElementBytesV080); err != nil {
		t.Fatalf("maximum TopK element rejected: %v", err)
	}

	bitsPerItem := -math.Log(0.01) / (math.Ln2 * math.Ln2)
	maxBloomCapacity := int64(math.Floor(float64(maxBloomBitsV080) / bitsPerItem))
	if err := validateBloomSizingV080(0.01, maxBloomCapacity); err != nil {
		t.Fatalf("maximum Bloom sizing rejected: %v", err)
	}
	if err := validateBloomSizingV080(0.01, maxBloomCapacity+1); err == nil {
		t.Fatal("oversized Bloom sizing accepted")
	}
	if err := validateBloomSizingV080(math.SmallestNonzeroFloat64, 1); err == nil {
		t.Fatal("Bloom sizing requiring too many hashes accepted")
	}

	probability := 0.1
	depth := int64(math.Ceil(-math.Log(probability)))
	minimumError := math.E / float64(maxCMSCountersV080/depth)
	if err := validateCMSProbabilityV080(minimumError, probability); err != nil {
		t.Fatalf("maximum CMS probability dimensions rejected: %v", err)
	}
	if err := validateCMSProbabilityV080(math.Nextafter(minimumError, 0), probability); err == nil {
		t.Fatal("oversized CMS probability dimensions accepted")
	}
}

func TestV080TopKLimitUsesEncodedBytesWithDeferredCodec(t *testing.T) {
	deferred := nativeDeferredCodecValue{
		codec:             JSONCodec{},
		value:             strings.Repeat("x", 251),
		maxEncodedBytes:   maxTopKElementBytesV080,
		encodedValueLabel: "element",
		command:           "TOPK.ADD",
	}
	if _, err := encodeNativeDeferredCodecValue(deferred); err == nil {
		t.Fatal("deferred JSON codec bypassed the encoded TopK element limit")
	}

	exec := &fakeExecutor{value: []any{nil}}
	client := NewClientWithExecutor(exec, WithCodec(JSONCodec{}))
	if _, err := client.TopK().Add(context.Background(), "key", strings.Repeat("x", 251)); err == nil {
		t.Fatal("eager JSON codec bypassed the encoded TopK element limit")
	}
	if len(exec.calls) != 0 {
		t.Fatalf("oversized encoded TopK element reached transport: %#v", exec.calls)
	}
}
