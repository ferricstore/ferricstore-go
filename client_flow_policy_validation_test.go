package ferricstore

import (
	"context"
	"strings"
	"testing"
)

func TestFlowPolicyCommandsRejectInvalidArgumentsBeforeTransport(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "set empty type", call: func(c *Client) error { _, err := c.SetPolicy(ctx, "", PolicyOptions{}); return err }},
		{name: "empty indexed attribute", call: func(c *Client) error {
			_, err := c.SetPolicy(ctx, "type", PolicyOptions{IndexedAttributes: []string{""}})
			return err
		}},
		{name: "too many indexed attributes", call: func(c *Client) error {
			_, err := c.SetPolicy(ctx, "type", PolicyOptions{IndexedAttributes: []string{"a", "b", "c", "d"}})
			return err
		}},
		{name: "duplicate indexed attribute", call: func(c *Client) error {
			_, err := c.SetPolicy(ctx, "type", PolicyOptions{IndexedAttributes: []string{"a", "a"}})
			return err
		}},
		{name: "normalized duplicate indexed attribute", call: func(c *Client) error {
			_, err := c.SetPolicy(ctx, "type", PolicyOptions{IndexedAttributes: []string{"a", " a "}})
			return err
		}},
		{name: "reserved indexed state meta", call: func(c *Client) error {
			_, err := c.SetPolicy(ctx, "type", PolicyOptions{IndexedStateMeta: "__internal", IndexedStateMetaSet: true})
			return err
		}},
		{name: "empty retry state", call: func(c *Client) error {
			_, err := c.SetPolicy(ctx, "type", PolicyOptions{States: map[string]RetryPolicy{"": {MaxRetries: 1}}})
			return err
		}},
		{name: "empty full state", call: func(c *Client) error {
			_, err := c.SetPolicy(ctx, "type", PolicyOptions{StatePolicies: map[string]FlowStatePolicy{"": {Mode: FlowStateModeFIFO}}})
			return err
		}},
		{name: "negative retries", call: func(c *Client) error {
			_, err := c.SetPolicy(ctx, "type", PolicyOptions{Retry: &RetryPolicy{MaxRetries: -1, MaxRetriesSet: true}})
			return err
		}},
		{name: "excessive retries", call: func(c *Client) error {
			_, err := c.SetPolicy(ctx, "type", PolicyOptions{Retry: &RetryPolicy{MaxRetries: 1001}})
			return err
		}},
		{name: "invalid backoff", call: func(c *Client) error {
			_, err := c.SetPolicy(ctx, "type", PolicyOptions{Retry: &RetryPolicy{Backoff: "random"}})
			return err
		}},
		{name: "negative base delay", call: func(c *Client) error {
			_, err := c.SetPolicy(ctx, "type", PolicyOptions{Retry: &RetryPolicy{BaseMS: -1, BaseMSSet: true}})
			return err
		}},
		{name: "excessive maximum delay", call: func(c *Client) error {
			_, err := c.SetPolicy(ctx, "type", PolicyOptions{Retry: &RetryPolicy{MaxMS: 2_592_000_001}})
			return err
		}},
		{name: "invalid jitter", call: func(c *Client) error {
			_, err := c.SetPolicy(ctx, "type", PolicyOptions{Retry: &RetryPolicy{JitterPct: 101}})
			return err
		}},
		{name: "running exhausted state", call: func(c *Client) error {
			_, err := c.SetPolicy(ctx, "type", PolicyOptions{Retry: &RetryPolicy{ExhaustedTo: "running"}})
			return err
		}},
		{name: "get empty type", call: func(c *Client) error { _, err := c.PolicyGet(ctx, "", ""); return err }},
		{name: "cleanup zero limit", call: func(c *Client) error {
			_, err := c.RetentionCleanup(ctx, RetentionCleanupOptions{Limit: Int(0)})
			return err
		}},
		{name: "cleanup negative time", call: func(c *Client) error {
			_, err := c.RetentionCleanup(ctx, RetentionCleanupOptions{NowMS: Int64(-1)})
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			if err := test.call(NewClientWithExecutor(exec)); err == nil {
				t.Fatal("invalid policy command succeeded")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid policy command reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestSetPolicyCanonicalizesIndexedMetadataKeys(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{
		"indexed_attributes": []any{"tenant"},
		"indexed_state_meta": "attempt",
	}}
	_, err := NewClientWithExecutor(exec).SetPolicy(context.Background(), "type", PolicyOptions{
		IndexedAttributes:   []string{" tenant "},
		IndexedStateMeta:    " attempt ",
		IndexedStateMetaSet: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertCall(t, exec, []any{
		"FLOW.POLICY.SET", "type", "INDEXED_ATTRIBUTES", []string{"tenant"}, "INDEXED_STATE_META", "attempt",
	})
}

func TestSetPolicyEmitsStateMapsDeterministically(t *testing.T) {
	ctx := context.Background()
	opt := PolicyOptions{
		States: map[string]RetryPolicy{
			"zeta":  {MaxRetries: 1},
			"alpha": {MaxRetries: 2},
		},
		StatePolicies: map[string]FlowStatePolicy{
			"theta": {Mode: FlowStateModeFIFO},
			"beta":  {Mode: FlowStateModeParallel},
		},
	}
	want := []any{
		"FLOW.POLICY.SET", "type",
		"STATE", "alpha", "MAX_RETRIES", 2,
		"STATE", "zeta", "MAX_RETRIES", 1,
		"STATE", "beta", "MODE", "PARALLEL",
		"STATE", "theta", "MODE", "FIFO",
	}

	for iteration := 0; iteration < 32; iteration++ {
		exec := &fakeExecutor{value: []byte("OK")}
		if _, err := NewClientWithExecutor(exec).SetPolicy(ctx, "type", opt); err != nil {
			t.Fatal(err)
		}
		assertCall(t, exec, want)
	}
}

func TestSetPolicyAcceptsOnlyExactAcknowledgementOrVerifiableMap(t *testing.T) {
	opt := PolicyOptions{Retry: &RetryPolicy{MaxRetries: 1}}
	accepted := []any{
		[]byte("OK"),
		"ok",
		nativeCompactOKCount(1),
		map[string]any{"retry": map[string]any{"max_retries": int64(1)}},
	}
	for _, value := range accepted {
		exec := &fakeExecutor{value: value}
		if _, err := NewClientWithExecutor(exec).SetPolicy(context.Background(), "type", opt); err != nil {
			t.Fatalf("response %#v rejected: %v", value, err)
		}
	}

	rejected := []any{
		nil,
		"garbage",
		[]byte("QUEUED"),
		int64(1),
		[]any{"retry"},
	}
	for _, value := range rejected {
		exec := &fakeExecutor{value: value}
		_, err := NewClientWithExecutor(exec).SetPolicy(context.Background(), "type", opt)
		if err == nil || !strings.Contains(err.Error(), "policy map or OK") {
			t.Fatalf("response %#v error = %v, want policy-map-or-OK rejection", value, err)
		}
	}
}
