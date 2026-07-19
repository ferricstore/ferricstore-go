//go:build integration

package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	osexec "os/exec"
	"strings"
	"testing"
	"time"
)

func TestIntegrationClusterReady(t *testing.T) {
	if os.Getenv("FERRICSTORE_CLUSTER_READY") != "1" {
		t.Skip("cluster readiness is run by scripts/integration-cluster-docker.sh")
	}
	urls, nodes := clusterURLs(t), clusterNodes(t)
	for _, rawURL := range urls {
		client := clusterDirectClient(t, rawURL)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := client.Ping(ctx)
		cancel()
		client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	admin := clusterDirectClient(t, urls[0])
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	status, err := admin.ClusterStatus(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	statusText := strings.ToLower(fmt.Sprint(status))
	for _, node := range nodes {
		if !strings.Contains(statusText, strings.ToLower(node)) {
			t.Fatalf("cluster status does not include %q: %#v", node, status)
		}
	}
	shards := 0
	for key, raw := range status {
		if !strings.HasPrefix(key, "shard_") {
			continue
		}
		shard, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("cluster status %s is not a section: %#v", key, raw)
		}
		members := strings.Split(strings.ToLower(asString(shard["members"])), ",")
		memberSet := make(map[string]struct{}, len(members))
		for _, member := range members {
			memberSet[strings.TrimSpace(member)] = struct{}{}
		}
		for _, node := range nodes {
			if _, present := memberSet[strings.ToLower(node)]; !present {
				t.Fatalf("cluster status %s does not include member %q: %#v", key, node, shard)
			}
		}
		shards++
	}
	if shards < 3 {
		t.Fatalf("cluster status exposes only %d shard sections: %#v", shards, status)
	}
	topology := newClusterTopology(t, CrossShardWriteReject)
	defer topology.Close()
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	err = topology.RefreshTopology(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	topology.mu.Lock()
	shardCount := topology.topology.ShardCount
	topology.mu.Unlock()
	if shardCount < 3 {
		t.Fatalf("cluster exposes only %d shards", shardCount)
	}
}

func TestIntegrationClusterTopologyRoutingAndFailover(t *testing.T) {
	if os.Getenv("FERRICSTORE_CLUSTER_TEST") != "1" {
		t.Skip("cluster integration is run by scripts/integration-cluster-docker.sh")
	}
	urls, nodes := clusterURLs(t), clusterNodes(t)
	admin := clusterDirectClient(t, urls[0])
	defer admin.Close()

	topology := newClusterTopology(t, CrossShardWriteReject)
	defer topology.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := topology.RefreshTopology(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	keys := clusterKeysByShard(t, topology, 3)

	for _, target := range []struct {
		shard int
		node  string
	}{{shard: 0, node: nodes[1]}, {shard: 1, node: nodes[2]}} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		ok, err := admin.ClusterFailover(ctx, target.shard, target.node)
		cancel()
		if err != nil || !ok {
			t.Fatalf("fail over shard %d to %s = %v, %v", target.shard, target.node, ok, err)
		}
		waitForCluster(t, 20*time.Second, func(ctx context.Context) error {
			if err := topology.RefreshTopology(ctx); err != nil {
				return err
			}
			route, err := topology.Route(keys[target.shard])
			if err != nil {
				return err
			}
			if route.LeaderNode != target.node {
				return fmt.Errorf("shard %d leader is %s, want %s", target.shard, route.LeaderNode, target.node)
			}
			return nil
		})
	}

	client := NewClientWithExecutor(topology, WithCodec(StringCodec{}))
	for shard := 0; shard < 3; shard++ {
		if err := client.KV().Set(context.Background(), keys[shard], fmt.Sprintf("value-%d", shard)); err != nil {
			t.Fatalf("SET shard %d: %v", shard, err)
		}
	}
	values, err := client.KV().MGet(context.Background(), keys[2], keys[0], keys[1])
	if err != nil {
		t.Fatal(err)
	}
	for index, want := range []string{"value-2", "value-0", "value-1"} {
		if asString(values[index]) != want {
			t.Fatalf("MGET value %d = %#v, want %q", index, values[index], want)
		}
	}
	if count, err := client.KV().Exists(context.Background(), keys[0], keys[1], keys[2]); err != nil || count != 3 {
		t.Fatalf("cross-shard EXISTS = %d, %v", count, err)
	}
	if _, err := client.Delete(context.Background(), keys[0], keys[1], keys[2]); err == nil {
		t.Fatal("default topology policy allowed a cross-shard destructive write")
	}

	commands := [][]any{{"SET", keys[0], "pipeline-0"}, {"SET", keys[1], "pipeline-1"}, {"GET", keys[0]}, {"GET", keys[1]}}
	results, err := client.Pipeline(context.Background(), commands)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != len(commands) || asString(results[2]) != "pipeline-0" || asString(results[3]) != "pipeline-1" {
		t.Fatalf("cross-node pipeline results = %#v", results)
	}

	partial := newClusterTopology(t, CrossShardWritePerShard)
	defer partial.Close()
	if err := partial.RefreshTopology(context.Background()); err != nil {
		t.Fatal(err)
	}
	partialClient := NewClientWithExecutor(partial, WithCodec(StringCodec{}))
	for shard := 0; shard < 3; shard++ {
		if err := partialClient.KV().Set(context.Background(), keys[shard], "before-pause"); err != nil {
			t.Fatal(err)
		}
	}

	container := os.Getenv("FERRICSTORE_CLUSTER_FAILURE_CONTAINER")
	if container == "" {
		t.Fatal("FERRICSTORE_CLUSTER_FAILURE_CONTAINER is required")
	}
	runDockerClusterCommand(t, 10*time.Second, "pause", container)
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	deleted, err := partialClient.Delete(ctx, keys[2], keys[1])
	cancel()
	var partialErr *TopologyPartialWriteError
	if !errors.As(err, &partialErr) || deleted != 0 || partialErr.Succeeded != 1 || len(partialErr.Failures) != 1 {
		t.Fatalf("paused-node DEL = %d, %T %#v", deleted, err, err)
	}
	runDockerClusterCommand(t, 10*time.Second, "unpause", container)
	waitForCluster(t, 10*time.Second, func(ctx context.Context) error {
		return partialClient.KV().Set(ctx, keys[1], "recovered")
	})

	runDockerClusterCommand(t, 10*time.Second, "kill", "--signal", "KILL", container)
	if err := partialClient.KV().Set(context.Background(), keys[2], "healthy-during-failure"); err != nil {
		t.Fatalf("healthy shard failed during peer loss: %v", err)
	}
	waitForCluster(t, 30*time.Second, func(ctx context.Context) error {
		if err := partial.RefreshTopology(ctx); err != nil {
			return err
		}
		route, err := partial.Route(keys[1])
		if err != nil {
			return err
		}
		if route.LeaderNode == nodes[2] {
			return errors.New("failed node remains shard leader")
		}
		return partialClient.KV().Set(ctx, keys[1], "after-failover")
	})
}

func runDockerClusterCommand(t *testing.T, timeout time.Duration, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	output, err := osexec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v: %s", strings.Join(args, " "), err, output)
	}
}
