package ferricstore

import "context"

func (c *Client) ClusterHealth(ctx context.Context) (map[string]any, error) {
	value, err := c.typedReply(ctx, "CLUSTER.HEALTH")
	if err != nil {
		return nil, err
	}
	return kvResponse(value)
}

func (c *Client) ClusterStats(ctx context.Context) (map[string]any, error) {
	value, err := c.typedReply(ctx, "CLUSTER.STATS")
	if err != nil {
		return nil, err
	}
	return kvResponse(value)
}

func (c *Client) ClusterKeySlot(ctx context.Context, key string) (int64, error) {
	value, err := c.typedReply(ctx, "CLUSTER.KEYSLOT", key)
	return responseInt64(value, err)
}

func (c *Client) ClusterSlots(ctx context.Context) (any, error) {
	return c.typedReply(ctx, "CLUSTER.SLOTS")
}

func (c *Client) ClusterStatus(ctx context.Context) (map[string]any, error) {
	value, err := c.typedReply(ctx, "CLUSTER.STATUS")
	if err != nil {
		return nil, err
	}
	return kvResponse(value)
}

func (c *Client) ClusterRole(ctx context.Context) (any, error) {
	return c.typedReply(ctx, "CLUSTER.ROLE")
}

func (c *Client) ClusterJoin(ctx context.Context, node string, replace bool) (bool, error) {
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
	value, err := c.typedReply(ctx, "CLUSTER.FAILOVER", shardIndex, targetNode)
	return responseOK(value, err)
}

func (c *Client) ClusterPromote(ctx context.Context, node string) (bool, error) {
	value, err := c.typedReply(ctx, "CLUSTER.PROMOTE", node)
	return responseOK(value, err)
}

func (c *Client) ClusterDemote(ctx context.Context, node string) (bool, error) {
	value, err := c.typedReply(ctx, "CLUSTER.DEMOTE", node)
	return responseOK(value, err)
}

func (c *Client) FerricStoreConfig(ctx context.Context, args ...any) (any, error) {
	command := append([]any{"FERRICSTORE.CONFIG"}, args...)
	return c.typedReply(ctx, command...)
}

func (c *Client) FerricStoreHotness(ctx context.Context, args ...any) (map[string]any, error) {
	command := append([]any{"FERRICSTORE.HOTNESS"}, args...)
	value, err := c.typedReply(ctx, command...)
	if err != nil {
		return nil, err
	}
	return kvResponse(value)
}

// FerricStoreMetrics returns the server's Prometheus text exposition without
// parsing or normalizing it, so labels, duplicate series, comments, and
// timestamps remain intact.
func (c *Client) FerricStoreMetrics(ctx context.Context, args ...any) (string, error) {
	command := append([]any{"FERRICSTORE.METRICS"}, args...)
	value, err := c.typedReply(ctx, command...)
	return responseString(value, err)
}

func (c *Client) FerricStoreBlobGC(ctx context.Context, args ...any) (any, error) {
	command := append([]any{"FERRICSTORE.BLOBGC"}, args...)
	return c.typedReply(ctx, command...)
}
