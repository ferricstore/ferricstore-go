//go:build integration

package ferricstore

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func clusterURLs(t *testing.T) []string {
	t.Helper()
	return requiredClusterList(t, "FERRICSTORE_CLUSTER_URLS")
}

func clusterNodes(t *testing.T) []string {
	t.Helper()
	return requiredClusterList(t, "FERRICSTORE_CLUSTER_NODES")
}

func requiredClusterList(t *testing.T, name string) []string {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		t.Fatalf("%s is required", name)
	}
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			values = append(values, value)
		}
	}
	if len(values) < 3 {
		t.Fatalf("%s requires at least three values, got %q", name, raw)
	}
	return values
}

func newClusterTopology(t *testing.T, crossShard CrossShardWritePolicy) *TopologyNativeExecutor {
	t.Helper()
	exec, err := NewTopologyNativeExecutor(
		clusterURLs(t),
		WithTopologyEndpointPolicy(EndpointPolicyNone),
		WithTopologyWarmConnections(true),
		WithTopologyCrossShardWritePolicy(crossShard),
		WithTopologyNativeOptions(
			WithNativeTimeout(2*time.Second),
			WithNativeHeartbeat(0, 0),
			WithNativeReconnect(0),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	return exec
}

func clusterDirectClient(t *testing.T, rawURL string) *Client {
	t.Helper()
	client, err := NewClientFromURL(rawURL, WithNativeOptions(
		WithNativeTimeout(2*time.Second),
		WithNativeHeartbeat(0, 0),
		WithNativeReconnect(0),
	))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func clusterKeysByShard(t *testing.T, exec *TopologyNativeExecutor, count int) map[int]string {
	t.Helper()
	keys := make(map[int]string, count)
	for index := 0; index < 1_000_000 && len(keys) < count; index++ {
		key := fmt.Sprintf("go-sdk:cluster:key:%d", index)
		route, err := exec.Route(key)
		if err != nil {
			t.Fatal(err)
		}
		if _, exists := keys[route.Shard]; !exists {
			keys[route.Shard] = key
		}
	}
	if len(keys) < count {
		t.Fatalf("found keys for %d of %d shards", len(keys), count)
	}
	return keys
}

func waitForCluster(t *testing.T, timeout time.Duration, check func(context.Context) error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		lastErr = check(ctx)
		cancel()
		if lastErr == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("cluster did not converge within %s: %v", timeout, lastErr)
}
