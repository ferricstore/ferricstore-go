package ferricstore

import (
	"context"
	"testing"
)

func TestErrorOnlyClientMethodsRejectMalformedSuccessResponses(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "rename", call: func(c *Client) error { return c.Rename(ctx, "old", "new") }},
		{name: "flush db", call: func(c *Client) error { return c.FlushDB(ctx, "") }},
		{name: "flush all", call: func(c *Client) error { return c.FlushAll(ctx, "") }},
		{name: "config set", call: func(c *Client) error { return c.ConfigSet(ctx, "maxmemory", "1") }},
		{name: "config resetstat", call: func(c *Client) error { return c.ConfigResetStat(ctx) }},
		{name: "config rewrite", call: func(c *Client) error { return c.ConfigRewrite(ctx) }},
		{name: "client setname", call: func(c *Client) error { return c.ClientSetName(ctx, "worker") }},
		{name: "acl setuser", call: func(c *Client) error { return c.ACLSetUser(ctx, "worker", "on") }},
		{name: "acl save", call: func(c *Client) error { return c.ACLSave(ctx) }},
		{name: "acl load", call: func(c *Client) error { return c.ACLLoad(ctx) }},
		{name: "slowlog reset", call: func(c *Client) error { return c.SlowLogReset(ctx) }},
		{name: "select", call: func(c *Client) error { return c.Select(ctx, 0) }},
		{name: "save", call: func(c *Client) error { return c.Save(ctx) }},
		{name: "bgsave", call: func(c *Client) error { return c.BgSave(ctx) }},
		{
			name: "run steps many",
			call: func(c *Client) error {
				return c.RunStepsMany(ctx, RunStepsManyOptions{
					Type: "order", States: []string{"pending"}, Worker: "worker", Items: []RunStepsItem{{ID: "id"}},
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := NewClientWithExecutor(&fakeExecutor{value: []byte("NOT-OK")})
			if err := test.call(client); err == nil {
				t.Fatal("expected malformed success response to be rejected")
			}
		})
	}
}

func TestBgSaveAcceptsItsDocumentedSuccessResponse(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{value: []byte("Background saving started")})
	if err := client.BgSave(context.Background()); err != nil {
		t.Fatalf("BGSAVE rejected its success response: %v", err)
	}
}
