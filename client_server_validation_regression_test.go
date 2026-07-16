package ferricstore

import (
	"context"
	"testing"
)

func TestServerWrappersRejectInvalidArgumentsBeforeTransport(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "negative database", call: func(c *Client) error { return c.Select(ctx, -1) }},
		{name: "negative wait replicas", call: func(c *Client) error { _, err := c.Wait(ctx, -1, 0); return err }},
		{name: "negative wait timeout", call: func(c *Client) error { _, err := c.Wait(ctx, 0, -1); return err }},
		{name: "negative waitaof local", call: func(c *Client) error { _, err := c.WaitAOF(ctx, -1, 0, 0); return err }},
		{name: "negative waitaof replicas", call: func(c *Client) error { _, err := c.WaitAOF(ctx, 0, -1, 0); return err }},
		{name: "negative waitaof timeout", call: func(c *Client) error { _, err := c.WaitAOF(ctx, 0, 0, -1); return err }},
		{name: "negative slowlog count", call: func(c *Client) error { _, err := c.SlowLogGet(ctx, Int(-1)); return err }},
		{name: "subscribe without channels", call: func(c *Client) error { _, err := c.Subscribe(ctx); return err }},
		{name: "psubscribe without patterns", call: func(c *Client) error { _, err := c.PSubscribe(ctx); return err }},
		{name: "negative failover shard", call: func(c *Client) error { _, err := c.ClusterFailover(ctx, -1, "node"); return err }},
		{name: "missing failover target", call: func(c *Client) error { _, err := c.ClusterFailover(ctx, 0, ""); return err }},
		{name: "missing cluster join node", call: func(c *Client) error { _, err := c.ClusterJoin(ctx, "", false); return err }},
		{name: "missing cluster promote node", call: func(c *Client) error { _, err := c.ClusterPromote(ctx, ""); return err }},
		{name: "missing cluster demote node", call: func(c *Client) error { _, err := c.ClusterDemote(ctx, ""); return err }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			if err := test.call(NewClientWithExecutor(exec)); err == nil {
				t.Fatal("invalid command succeeded")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid command reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestServerWrappersRejectImpossibleIntegerResponses(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name  string
		value any
		call  func(*Client) error
	}{
		{name: "cluster negative slot", value: int64(-1), call: func(c *Client) error { _, err := c.ClusterKeySlot(ctx, "key"); return err }},
		{name: "cluster out of range slot", value: int64(routeSlotCount), call: func(c *Client) error { _, err := c.ClusterKeySlot(ctx, "key"); return err }},
		{name: "negative slowlog length", value: int64(-1), call: func(c *Client) error { _, err := c.SlowLogLen(ctx); return err }},
		{name: "negative wait count", value: int64(-1), call: func(c *Client) error { _, err := c.Wait(ctx, 0, 0); return err }},
		{name: "negative object refcount", value: int64(-1), call: func(c *Client) error { _, err := c.ObjectRefCount(ctx, "key"); return err }},
		{name: "negative publish count", value: int64(-1), call: func(c *Client) error { _, err := c.Publish(ctx, "channel", "message"); return err }},
		{name: "negative pattern count", value: int64(-1), call: func(c *Client) error { _, err := c.PubSubNumPat(ctx); return err }},
		{name: "negative last save", value: int64(-1), call: func(c *Client) error { _, err := c.LastSave(ctx); return err }},
		{name: "negative command count", value: int64(-1), call: func(c *Client) error { _, err := c.CommandCount(ctx); return err }},
		{name: "invalid waitaof shape", value: []any{int64(0)}, call: func(c *Client) error { _, err := c.WaitAOF(ctx, 0, 0, 0); return err }},
		{name: "negative waitaof acknowledgement", value: []any{int64(0), int64(-1)}, call: func(c *Client) error { _, err := c.WaitAOF(ctx, 0, 0, 0); return err }},
		{name: "negative deleted ACL users", value: int64(-1), call: func(c *Client) error { _, err := c.ACLDelUser(ctx, "user"); return err }},
		{name: "too many deleted ACL users", value: int64(2), call: func(c *Client) error { _, err := c.ACLDelUser(ctx, "user"); return err }},
		{name: "negative channel subscribers", value: []any{"channel", int64(-1)}, call: func(c *Client) error { _, err := c.PubSubNumSub(ctx, "channel"); return err }},
		{name: "duplicate channel subscribers", value: []any{"channel", int64(1), "channel", int64(2)}, call: func(c *Client) error { _, err := c.PubSubNumSub(ctx, "channel"); return err }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: test.value}
			if err := test.call(NewClientWithExecutor(exec)); err == nil {
				t.Fatal("impossible server response was accepted")
			}
			if len(exec.calls) != 1 {
				t.Fatalf("transport calls = %d, want 1", len(exec.calls))
			}
		})
	}
}

func TestManagementHelpersRejectAmbiguousOptionMapsBeforeTransport(t *testing.T) {
	for _, attrs := range []map[string]any{
		{"limit": 1, "LIMIT": 2},
		{"": 1},
		{"   ": 1},
	} {
		exec := &fakeExecutor{value: []byte("OK")}
		if _, err := NewClientWithExecutor(exec).EnsureNamespace(
			context.Background(), "tenant:", attrs,
		); err == nil {
			t.Fatalf("ambiguous management options %#v were accepted", attrs)
		}
		if len(exec.calls) != 0 {
			t.Fatalf("invalid management options reached transport: %#v", exec.calls)
		}
	}
}
