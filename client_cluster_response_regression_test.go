package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestClusterReportsPreservePerShardSections(t *testing.T) {
	response := "shard_0:\r\n  role: leader\r\n  keys: 3\r\nshard_1:\r\n  role: follower\r\n  keys: 7\r\ntotal_keys: 10"
	want := map[string]any{
		"shard_0":    map[string]any{"role": "leader", "keys": int64(3)},
		"shard_1":    map[string]any{"role": "follower", "keys": int64(7)},
		"total_keys": int64(10),
	}

	got, err := clusterReportResponse(response)
	if err != nil {
		t.Fatalf("parse cluster report: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("cluster report = %#v, want %#v", got, want)
	}
}

func TestClusterReportCommandsValidateTheirSchemas(t *testing.T) {
	tests := []struct {
		name     string
		response any
		call     func(*Client) (map[string]any, error)
	}{
		{
			name: "health",
			response: "shard_0:\n  role: leader\n  status: ok\n  keys: 3\n" +
				"  memory_bytes: 64\nfuture_field: retained",
			call: func(client *Client) (map[string]any, error) {
				return client.ClusterHealth(context.Background())
			},
		},
		{
			name: "stats",
			response: "shard_0:\n  keys: 3\n  memory_bytes: 64\n" +
				"shard_1:\n  keys: 7\n  memory_bytes: 128\n" +
				"total_keys: 10\ntotal_memory_bytes: 192",
			call: func(client *Client) (map[string]any, error) {
				return client.ClusterStats(context.Background())
			},
		},
		{
			name: "status",
			response: "mode: cluster\nreplication_mode: raft\ncluster_state: healthy\n" +
				"role: voter\nnode: one@host\nsync_status: ready\nconnected_nodes: two@host\n" +
				"shard_0:\n  leader: one@host\n  members: one@host, two@host",
			call: func(client *Client) (map[string]any, error) {
				return client.ClusterStatus(context.Background())
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.call(NewClientWithExecutor(&fakeExecutor{value: test.response}))
			if err != nil {
				t.Fatalf("valid report rejected: %v", err)
			}
			if got["future_field"] == nil && test.name == "health" {
				t.Fatal("forward-compatible field was discarded")
			}
		})
	}
}

func TestClusterStatusAcceptsEmptyNodeLists(t *testing.T) {
	response := "mode: standalone\nreplication_mode: none\ncluster_state: healthy\n" +
		"role: standalone\nnode: one@host\nsync_status: ready\nconnected_nodes:\n" +
		"shard_0:\n  leader: unknown\n  members:"
	got, err := NewClientWithExecutor(&fakeExecutor{value: response}).ClusterStatus(context.Background())
	if err != nil {
		t.Fatalf("standalone CLUSTER.STATUS rejected: %v", err)
	}
	if got["connected_nodes"] != "" {
		t.Fatalf("connected_nodes = %#v, want empty text", got["connected_nodes"])
	}
	shard := got["shard_0"].(map[string]any)
	if shard["members"] != "" {
		t.Fatalf("members = %#v, want empty text", shard["members"])
	}
}

func TestClusterReportCommandsRejectSemanticallyInvalidSuccesses(t *testing.T) {
	tests := []struct {
		name     string
		response any
		call     func(*Client) error
	}{
		{
			name:     "health missing status",
			response: "shard_0:\n  role: leader\n  keys: 3\n  memory_bytes: 64",
			call:     func(client *Client) error { _, err := client.ClusterHealth(context.Background()); return err },
		},
		{
			name:     "health negative keys",
			response: "shard_0:\n  role: leader\n  status: ok\n  keys: -1\n  memory_bytes: 64",
			call:     func(client *Client) error { _, err := client.ClusterHealth(context.Background()); return err },
		},
		{
			name:     "stats mismatched totals",
			response: "shard_0:\n  keys: 3\n  memory_bytes: 64\ntotal_keys: 4\ntotal_memory_bytes: 64",
			call:     func(client *Client) error { _, err := client.ClusterStats(context.Background()); return err },
		},
		{
			name:     "stats missing total",
			response: "shard_0:\n  keys: 3\n  memory_bytes: 64\ntotal_keys: 3",
			call:     func(client *Client) error { _, err := client.ClusterStats(context.Background()); return err },
		},
		{
			name: "status missing node",
			response: "mode: cluster\nreplication_mode: raft\ncluster_state: healthy\n" +
				"role: voter\nsync_status: ready\nconnected_nodes: two@host\n" +
				"shard_0:\n  leader: one@host\n  members: one@host",
			call: func(client *Client) error { _, err := client.ClusterStatus(context.Background()); return err },
		},
		{
			name: "status incomplete shard",
			response: "mode: cluster\nreplication_mode: raft\ncluster_state: healthy\n" +
				"role: voter\nnode: one@host\nsync_status: ready\nconnected_nodes: two@host\n" +
				"shard_0:\n  leader: one@host",
			call: func(client *Client) error { _, err := client.ClusterStatus(context.Background()); return err },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(NewClientWithExecutor(&fakeExecutor{value: test.response})); err == nil {
				t.Fatalf("accepted invalid report %#v", test.response)
			}
		})
	}
}

func TestClusterReportsRejectMalformedHierarchy(t *testing.T) {
	for _, response := range []any{
		"  role: leader",
		"shard_0:\n  role: leader\n  role: follower",
		"shard_0:\nshard_0:",
		"shard_0:\nmalformed",
	} {
		client := NewClientWithExecutor(&fakeExecutor{value: response})
		if _, err := client.ClusterStatus(context.Background()); err == nil {
			t.Fatalf("accepted malformed cluster report %#v", response)
		}
	}
}

func TestFerricStoreHotnessPreservesRepeatedPrefixEntries(t *testing.T) {
	response := []any{
		"hot_reads", "12",
		"cold_reads", []byte("3"),
		"hot_read_pct", "80.00",
		"cold_reads_per_second", "1.50",
		"top_n", "2",
		"prefix", "user", "hot", "10", "cold", "1", "cold_pct", "9.09",
		"prefix", []byte("job"), "hot", int64(2), "cold", int64(2), "cold_pct", "50.00",
	}
	want := map[string]any{
		"hot_reads":             int64(12),
		"cold_reads":            int64(3),
		"hot_read_pct":          80.0,
		"cold_reads_per_second": 1.5,
		"top_n":                 int64(2),
		"prefixes": []any{
			map[string]any{"prefix": "user", "hot": int64(10), "cold": int64(1), "cold_pct": 9.09},
			map[string]any{"prefix": "job", "hot": int64(2), "cold": int64(2), "cold_pct": 50.0},
		},
	}

	got, err := NewClientWithExecutor(&fakeExecutor{value: response}).FerricStoreHotness(context.Background())
	if err != nil {
		t.Fatalf("parse hotness: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("hotness = %#v, want %#v", got, want)
	}
}

func TestFerricStoreHotnessRejectsMalformedResponses(t *testing.T) {
	valid := []any{
		"hot_reads", "12", "cold_reads", "3", "hot_read_pct", "80",
		"cold_reads_per_second", "1.5", "top_n", "1",
		"prefix", "user", "hot", "10", "cold", "1", "cold_pct", "9.09",
	}
	tests := []any{
		append([]any(nil), valid[:len(valid)-1]...),
		append([]any{"wrong"}, valid[1:]...),
		append(append([]any(nil), valid[:10]...), append([]any{"wrong_prefix"}, valid[11:]...)...),
		[]any{"hot_reads", "-1", "cold_reads", "0", "hot_read_pct", "0", "cold_reads_per_second", "0", "top_n", "0"},
		[]any{"hot_reads", "1", "cold_reads", "0", "hot_read_pct", "101", "cold_reads_per_second", "0", "top_n", "1"},
	}
	for _, response := range tests {
		client := NewClientWithExecutor(&fakeExecutor{value: response})
		if _, err := client.FerricStoreHotness(context.Background()); err == nil {
			t.Fatalf("accepted malformed hotness response %#v", response)
		}
	}
}

func TestFerricStoreHotnessValidatesOptionsBeforeTransport(t *testing.T) {
	invalid := [][]any{
		{"TOP"},
		{"UNKNOWN", 1},
		{"TOP", 0},
		{"WINDOW", -1},
		{"TOP", "1.5"},
		{"TOP", 1, "top", 2},
		{"WINDOW", 1, "WINDOW", 2},
	}
	for _, args := range invalid {
		exec := &fakeExecutor{value: []any{}}
		if _, err := NewClientWithExecutor(exec).FerricStoreHotness(context.Background(), args...); err == nil {
			t.Fatalf("accepted invalid hotness options %#v", args)
		}
		if len(exec.calls) != 0 {
			t.Fatalf("invalid hotness options reached transport: %#v", exec.calls)
		}
	}
}

func TestFerricStoreHotnessAcceptsDocumentedOptions(t *testing.T) {
	response := []any{
		"hot_reads", "0", "cold_reads", "0", "hot_read_pct", "0",
		"cold_reads_per_second", "0", "top_n", "2",
	}
	exec := &fakeExecutor{value: response}
	if _, err := NewClientWithExecutor(exec).FerricStoreHotness(
		context.Background(), []byte("window"), int64(30), "TOP", 2,
	); err != nil {
		t.Fatalf("valid hotness options rejected: %v", err)
	}
	assertCall(t, exec, []any{"FERRICSTORE.HOTNESS", []byte("window"), int64(30), "TOP", 2})
}

func TestFerricStoreMetricsRejectsArgumentsBeforeTransport(t *testing.T) {
	for _, call := range []func(*Client) error{
		func(client *Client) error {
			_, err := client.FerricStoreMetrics(context.Background(), "unexpected")
			return err
		},
		func(client *Client) error {
			_, err := client.FerricStoreMetricsText(context.Background(), "unexpected")
			return err
		},
	} {
		exec := &fakeExecutor{value: "metric 1\n"}
		if err := call(NewClientWithExecutor(exec)); err == nil {
			t.Fatal("FERRICSTORE.METRICS accepted arguments")
		}
		if len(exec.calls) != 0 {
			t.Fatalf("invalid metrics call reached transport: %#v", exec.calls)
		}
	}
}

func TestClusterSlotsAndRoleRejectMalformedResponses(t *testing.T) {
	for _, response := range []any{
		[]any{},
		[]any{[]any{int64(0), int64(routeSlotCount), int64(0)}},
		[]any{[]any{int64(0), int64(10)}},
		[]any{[]any{int64(1), int64(routeSlotCount - 1), int64(0)}},
		[]any{[]any{int64(0), int64(10), int64(0)}, []any{int64(12), int64(routeSlotCount - 1), int64(1)}},
	} {
		client := NewClientWithExecutor(&fakeExecutor{value: response})
		if _, err := client.ClusterSlots(context.Background()); err == nil {
			t.Fatalf("accepted malformed CLUSTER.SLOTS response %#v", response)
		}
	}

	for _, response := range []any{nil, "", []byte(" \t"), int64(1)} {
		client := NewClientWithExecutor(&fakeExecutor{value: response})
		if _, err := client.ClusterRole(context.Background()); err == nil {
			t.Fatalf("accepted malformed CLUSTER.ROLE response %#v", response)
		}
	}
}

func TestClusterSlotsAndRolePreserveValidRawResponses(t *testing.T) {
	slots := []any{
		[]any{int64(0), int64(511), int64(0)},
		[]any{int64(512), int64(routeSlotCount - 1), int64(1)},
	}
	gotSlots, err := NewClientWithExecutor(&fakeExecutor{value: slots}).ClusterSlots(context.Background())
	if err != nil {
		t.Fatalf("CLUSTER.SLOTS: %v", err)
	}
	if !reflect.DeepEqual(gotSlots, slots) {
		t.Fatalf("CLUSTER.SLOTS changed raw response: %#v", gotSlots)
	}

	role := []byte("voter")
	gotRole, err := NewClientWithExecutor(&fakeExecutor{value: role}).ClusterRole(context.Background())
	if err != nil {
		t.Fatalf("CLUSTER.ROLE: %v", err)
	}
	if !reflect.DeepEqual(gotRole, role) {
		t.Fatalf("CLUSTER.ROLE changed raw response: %#v", gotRole)
	}
}
