package ferricstore

import (
	"context"
	"errors"
	"fmt"
)

func (c *Client) ClusterHealth(ctx context.Context) (map[string]any, error) {
	value, err := c.typedReply(ctx, "CLUSTER.HEALTH")
	if err != nil {
		return nil, err
	}
	return clusterHealthResponse(value)
}

func (c *Client) ClusterStats(ctx context.Context) (map[string]any, error) {
	value, err := c.typedReply(ctx, "CLUSTER.STATS")
	if err != nil {
		return nil, err
	}
	return clusterStatsResponse(value)
}

func (c *Client) ClusterKeySlot(ctx context.Context, key string) (int64, error) {
	value, err := c.typedReply(ctx, "CLUSTER.KEYSLOT", key)
	slot, err := responseInt64(value, err)
	if err != nil {
		return 0, err
	}
	if slot < 0 || slot >= routeSlotCount {
		return 0, fmt.Errorf("CLUSTER.KEYSLOT returned out-of-range slot %d", slot)
	}
	return slot, nil
}

func (c *Client) ClusterSlots(ctx context.Context) (any, error) {
	value, err := c.typedReply(ctx, "CLUSTER.SLOTS")
	if err := validateClusterSlotsResponse(value, err); err != nil {
		return nil, err
	}
	return value, nil
}

func (c *Client) ClusterStatus(ctx context.Context) (map[string]any, error) {
	value, err := c.typedReply(ctx, "CLUSTER.STATUS")
	if err != nil {
		return nil, err
	}
	return clusterStatusResponse(value)
}

func (c *Client) ClusterRole(ctx context.Context) (any, error) {
	value, err := c.typedReply(ctx, "CLUSTER.ROLE")
	if err := validateClusterRoleResponse(value, err); err != nil {
		return nil, err
	}
	return value, nil
}

func (c *Client) ClusterJoin(ctx context.Context, node string, replace bool) (bool, error) {
	if err := validateRequiredText("cluster node", node); err != nil {
		return false, err
	}
	args := []any{"CLUSTER.JOIN", node}
	if replace {
		args = append(args, "REPLACE")
	}
	value, err := c.typedReply(ctx, args...)
	return responseOK(value, err)
}

func (c *Client) ClusterLeave(ctx context.Context) (bool, error) {
	value, err := c.typedReply(ctx, "CLUSTER.LEAVE")
	return responseOK(value, err)
}

func (c *Client) ClusterFailover(ctx context.Context, shardIndex int, targetNode string) (bool, error) {
	if shardIndex < 0 {
		return false, errors.New("cluster shard index must be non-negative")
	}
	if err := validateRequiredText("cluster target node", targetNode); err != nil {
		return false, err
	}
	value, err := c.typedReply(ctx, "CLUSTER.FAILOVER", shardIndex, targetNode)
	return responseOK(value, err)
}

func (c *Client) ClusterPromote(ctx context.Context, node string) (bool, error) {
	if err := validateRequiredText("cluster node", node); err != nil {
		return false, err
	}
	value, err := c.typedReply(ctx, "CLUSTER.PROMOTE", node)
	return responseOK(value, err)
}

func (c *Client) ClusterDemote(ctx context.Context, node string) (bool, error) {
	if err := validateRequiredText("cluster node", node); err != nil {
		return false, err
	}
	value, err := c.typedReply(ctx, "CLUSTER.DEMOTE", node)
	return responseOK(value, err)
}

func (c *Client) FerricStoreConfig(ctx context.Context, args ...any) (any, error) {
	command := append([]any{"FERRICSTORE.CONFIG"}, args...)
	return c.typedReply(ctx, command...)
}

func (c *Client) FerricStoreHotness(ctx context.Context, args ...any) (map[string]any, error) {
	if err := validateHotnessArgs(args); err != nil {
		return nil, err
	}
	command := append([]any{"FERRICSTORE.HOTNESS"}, args...)
	value, err := c.typedReply(ctx, command...)
	if err != nil {
		return nil, err
	}
	return hotnessResponse(value)
}

// FerricStoreMetrics returns the metrics response as the key/value mapping
// exposed by earlier SDK releases.
//
// Deprecated: use FerricStoreMetricsText for lossless Prometheus exposition.
func (c *Client) FerricStoreMetrics(ctx context.Context, args ...any) (map[string]any, error) {
	if err := validateMetricsArgs(args); err != nil {
		return nil, err
	}
	command := append([]any{"FERRICSTORE.METRICS"}, args...)
	value, err := c.typedReply(ctx, command...)
	if err != nil {
		return nil, err
	}
	return metricsMapResponse(value)
}

// FerricStoreMetricsText returns the server's Prometheus text exposition
// without parsing or normalizing it, preserving labels, duplicate series,
// comments, and timestamps.
func (c *Client) FerricStoreMetricsText(ctx context.Context, args ...any) (string, error) {
	if err := validateMetricsArgs(args); err != nil {
		return "", err
	}
	command := append([]any{"FERRICSTORE.METRICS"}, args...)
	value, err := c.typedReply(ctx, command...)
	return responseString(value, err)
}

func (c *Client) FerricStoreBlobGC(ctx context.Context, args ...any) (any, error) {
	command := append([]any{"FERRICSTORE.BLOBGC"}, args...)
	return c.typedReply(ctx, command...)
}
