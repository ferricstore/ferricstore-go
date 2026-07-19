package ferricstore

import (
	"context"
	"math"
	"testing"
)

func TestProbabilisticInfoResponsesValidateServerSchemas(t *testing.T) {
	tests := []struct {
		name     string
		response any
		call     func(*Client) error
	}{
		{
			name: "BF.INFO missing field",
			response: []any{
				"Capacity", int64(100), "Size", int64(1), "Number of filters", int64(1),
				"Number of items inserted", int64(1), "Expansion rate", int64(0),
				"Error rate", 0.01, "Number of hash functions", int64(7),
			},
			call: func(client *Client) error {
				_, err := client.Bloom().Info(context.Background(), "bf")
				return err
			},
		},
		{
			name: "CF.INFO negative deletes",
			response: []any{
				"Size", int64(100), "Number of buckets", int64(25), "Number of filters", int64(1),
				"Number of items inserted", int64(3), "Number of items deleted", int64(-1),
				"Bucket size", int64(4), "Fingerprint size", int64(1), "Max iterations", int64(20),
				"Expansion rate", int64(0),
			},
			call: func(client *Client) error {
				_, err := client.Cuckoo().Info(context.Background(), "cf")
				return err
			},
		},
		{
			name:     "CMS.INFO zero width",
			response: []any{"width", int64(0), "depth", int64(5), "count", int64(10)},
			call: func(client *Client) error {
				_, err := client.CountMinSketch().Info(context.Background(), "cms")
				return err
			},
		},
		{
			name: "TDIGEST.INFO non-finite weight",
			response: []any{
				"Compression", int64(100), "Capacity", int64(610), "Merged nodes", int64(1),
				"Unmerged nodes", int64(0), "Merged weight", math.Inf(1), "Unmerged weight", 0.0,
				"Total compressions", int64(0), "Memory usage", int64(128),
			},
			call: func(client *Client) error {
				_, err := client.TDigest().Info(context.Background(), "td")
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(NewClientWithExecutor(&fakeExecutor{value: test.response})); err == nil {
				t.Fatalf("accepted malformed response %#v", test.response)
			}
		})
	}
}

func TestProbabilisticInfoResponsesPreserveValidMaps(t *testing.T) {
	tests := []struct {
		name     string
		response []any
		call     func(*Client) (map[string]any, error)
	}{
		{
			name: "BF.INFO",
			response: []any{
				"Capacity", int64(100), "Size", int64(1), "Number of filters", int64(1),
				"Number of items inserted", int64(1), "Expansion rate", int64(0), "Error rate", 0.01,
				"Number of hash functions", int64(7), "Number of bits", int64(959),
			},
			call: func(client *Client) (map[string]any, error) {
				return client.Bloom().Info(context.Background(), "bf")
			},
		},
		{
			name:     "CMS.INFO",
			response: []any{"width", int64(100), "depth", int64(5), "count", int64(10)},
			call: func(client *Client) (map[string]any, error) {
				return client.CountMinSketch().Info(context.Background(), "cms")
			},
		},
		{
			name:     "TOPK.INFO",
			response: []any{"k", int64(10), "width", int64(20), "depth", int64(5)},
			call: func(client *Client) (map[string]any, error) {
				return client.TopK().Info(context.Background(), "topk")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.call(NewClientWithExecutor(&fakeExecutor{value: test.response}))
			if err != nil {
				t.Fatalf("parse valid response: %v", err)
			}
			if len(got) != len(test.response)/2 {
				t.Fatalf("map length = %d, want %d", len(got), len(test.response)/2)
			}
		})
	}
}
