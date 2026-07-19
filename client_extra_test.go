package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestFerricStoreDoctorBuildsCommand(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)

	_, err := client.FerricStoreDoctor(context.Background(), "CHECK", "flow")

	if err != nil {
		t.Fatal(err)
	}
	assertCall(t, exec, []any{"FERRICSTORE.DOCTOR", "CHECK", "flow"})
}

func TestServerInfoParsesTextResponse(t *testing.T) {
	exec := &fakeExecutor{value: []byte("used_memory:10\nhealthy:true\n")}
	client := NewClientWithExecutor(exec)

	info, err := client.ServerInfo(context.Background(), "default")

	if err != nil {
		t.Fatal(err)
	}
	if info["used_memory"] != int64(10) || info["healthy"] != true {
		t.Fatalf("unexpected info: %#v", info)
	}
	assertCall(t, exec, []any{"INFO", "default"})
}

func TestPubSubNumSubParsesPairs(t *testing.T) {
	exec := &fakeExecutor{value: []any{[]byte("a"), int64(2), "b", int64(0)}}
	client := NewClientWithExecutor(exec)

	counts, err := client.PubSubNumSub(context.Background(), "a", "b")

	if err != nil {
		t.Fatal(err)
	}
	if counts["a"] != 2 || counts["b"] != 0 {
		t.Fatalf("unexpected counts: %#v", counts)
	}
	assertCall(t, exec, []any{"PUBSUB", "NUMSUB", "a", "b"})
}

func TestPubSubNumSubRejectsMalformedPairs(t *testing.T) {
	tests := []struct {
		name     string
		response any
	}{
		{name: "not an array", response: map[string]any{"a": int64(1)}},
		{name: "odd pair count", response: []any{"a", int64(1), "b"}},
		{name: "channel", response: []any{int64(1), int64(1)}},
		{name: "subscriber count", response: []any{"a", "invalid"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewClientWithExecutor(&fakeExecutor{value: tt.response})
			if _, err := client.PubSubNumSub(context.Background(), "a"); err == nil {
				t.Fatalf("accepted malformed PUBSUB NUMSUB response %#v", tt.response)
			}
		})
	}
}

func TestTransactionHelpersBuildStatefulCommands(t *testing.T) {
	exec := &fakeExecutor{values: []any{
		[]byte("OK"),
		[]byte("OK"),
		[]byte("QUEUED"),
		[]any{[]byte("OK")},
		[]byte("OK"),
		[]byte("OK"),
	}}
	client := NewClientWithExecutor(exec)
	ctx := context.Background()

	if err := client.Watch(ctx, "k1", "k2"); err != nil {
		t.Fatal(err)
	}
	tx, err := client.Transaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := tx.Command(ctx, "SET", "k", []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	results, err := tx.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if asString(queued) != "QUEUED" || len(results) != 1 || asString(results[0]) != "OK" {
		t.Fatalf("unexpected transaction result queued=%#v results=%#v", queued, results)
	}
	want := [][]any{
		{"WATCH", "k1", "k2"},
		{"MULTI"},
		{"COMMAND_EXEC", "SET", "k", []byte("v")},
		{"EXEC"},
	}
	if len(exec.calls) != len(want) {
		t.Fatalf("unexpected calls: %#v", exec.calls)
	}
	for i := range want {
		if !reflect.DeepEqual(exec.calls[i], want[i]) {
			t.Fatalf("unexpected call %d\n got: %#v\nwant: %#v", i, exec.calls[i], want[i])
		}
	}
}

func TestSubscribeHelpersBuildStatefulCommands(t *testing.T) {
	exec := &fakeExecutor{values: []any{
		[]any{[]byte("subscribe"), []byte("jobs"), int64(1)},
		[]any{[]byte("unsubscribe"), []byte("jobs"), int64(0)},
		[]any{[]byte("psubscribe"), []byte("jobs:*"), int64(1)},
		[]any{[]byte("punsubscribe"), []byte("jobs:*"), int64(0)},
	}}
	client := NewClientWithExecutor(exec)
	ctx := context.Background()

	if _, err := client.Subscribe(ctx, "jobs"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Unsubscribe(ctx, "jobs"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PSubscribe(ctx, "jobs:*"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PUnsubscribe(ctx, "jobs:*"); err != nil {
		t.Fatal(err)
	}

	want := [][]any{
		{"SUBSCRIBE", "jobs"},
		{"UNSUBSCRIBE", "jobs"},
		{"PSUBSCRIBE", "jobs:*"},
		{"PUNSUBSCRIBE", "jobs:*"},
	}
	for i := range want {
		if !reflect.DeepEqual(exec.calls[i], want[i]) {
			t.Fatalf("unexpected call %d\n got: %#v\nwant: %#v", i, exec.calls[i], want[i])
		}
	}
}

func TestClientConnectionHelpersBuildCommands(t *testing.T) {
	exec := &fakeExecutor{values: []any{
		[]byte("OK"),
		[]byte("name=worker-1\nage=10\n"),
	}}
	client := NewClientWithExecutor(exec)
	ctx := context.Background()

	if err := client.ClientSetName(ctx, "worker-1"); err != nil {
		t.Fatal(err)
	}
	info, err := client.ClientInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info["name"] != "worker-1" || info["age"] != int64(10) {
		t.Fatalf("unexpected client info: %#v", info)
	}

	want := [][]any{
		{"CLIENT", "SETNAME", "worker-1"},
		{"CLIENT", "INFO"},
	}
	for i := range want {
		if !reflect.DeepEqual(exec.calls[i], want[i]) {
			t.Fatalf("unexpected call %d\n got: %#v\nwant: %#v", i, exec.calls[i], want[i])
		}
	}
}

func TestACLHelpersBuildCommands(t *testing.T) {
	exec := &fakeExecutor{values: []any{
		[]byte("OK"),
		int64(1),
		[]any{[]byte("user default on +@all ~*")},
		[]any{[]byte("flags"), []any{[]byte("on")}},
		[]byte("OK"),
		[]byte("default"),
		[]byte("OK"),
	}}
	client := NewClientWithExecutor(exec)
	ctx := context.Background()

	if err := client.ACLSetUser(ctx, "alice", "on", ">secret", "+@flow"); err != nil {
		t.Fatal(err)
	}
	deleted, err := client.ACLDelUser(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	users, err := client.ACLList(ctx)
	if err != nil {
		t.Fatal(err)
	}
	user, err := client.ACLGetUser(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ACLSave(ctx); err != nil {
		t.Fatal(err)
	}
	whoami, err := client.ACLWhoAmI(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ACLLoad(ctx); err != nil {
		t.Fatal(err)
	}
	if deleted != 1 || len(users) != 1 || len(user) == 0 || whoami != "default" {
		t.Fatalf("unexpected ACL results deleted=%d users=%#v user=%#v whoami=%q", deleted, users, user, whoami)
	}

	want := [][]any{
		{"ACL", "SETUSER", "alice", "on", ">secret", "+@flow"},
		{"ACL", "DELUSER", "alice"},
		{"ACL", "LIST"},
		{"ACL", "GETUSER", "default"},
		{"ACL", "SAVE"},
		{"ACL", "WHOAMI"},
		{"ACL", "LOAD"},
	}
	for i := range want {
		if !reflect.DeepEqual(exec.calls[i], want[i]) {
			t.Fatalf("unexpected call %d\n got: %#v\nwant: %#v", i, exec.calls[i], want[i])
		}
	}
}

func TestManagementHelpersBuildNarrowCommands(t *testing.T) {
	exec := &fakeExecutor{values: []any{
		map[string]any{"sdk": true, "quota_management": false},
		[]byte("OK"),
		map[string]any{"prefix": "tenant:a:"},
		[]any{map[string]any{"prefix": "tenant:a:"}},
		[]byte("OK"),
		[]byte("OK"),
		map[string]any{"keys": int64(100)},
		map[string]any{"keys": int64(3)},
		map[string]any{"nodes": int64(1)},
		map[string]any{"bytes": int64(8)},
		[]any{map[string]any{"id": "flow-1"}},
		[]any{map[string]any{"event": "created"}},
	}}
	client := NewClientWithExecutor(exec)
	ctx := context.Background()

	capabilities, err := client.Capabilities(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if capabilities["sdk"] != true || capabilities["quota_management"] != false {
		t.Fatalf("unexpected capabilities: %#v", capabilities)
	}
	if _, err := client.EnsureNamespace(ctx, "tenant:a:", map[string]any{"durability": "disk", "limit": int64(10), "skip": nil}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetNamespace(ctx, "tenant:a:"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListNamespaces(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DeleteNamespace(ctx, "tenant:a:"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SetQuota(ctx, "tenant:a:", map[string]any{"bytes": int64(1024), "ops_per_sec": 20}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetQuota(ctx, "tenant:a:"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.QuotaUsage(ctx, "tenant:a:"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ClusterInfo(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.NamespaceUsage(ctx, "tenant:a:"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.FlowQuery(ctx, map[string]any{"type": "order", "state": "queued"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.FlowHistory(ctx, "flow-1", map[string]any{"max_events": 10}); err != nil {
		t.Fatal(err)
	}

	want := [][]any{
		{"FERRICSTORE.CAPABILITIES"},
		{"FERRICSTORE.NAMESPACE", "ENSURE", "tenant:a:", "DURABILITY", "disk", "LIMIT", int64(10)},
		{"FERRICSTORE.NAMESPACE", "GET", "tenant:a:"},
		{"FERRICSTORE.NAMESPACE", "LIST"},
		{"FERRICSTORE.NAMESPACE", "DELETE", "tenant:a:"},
		{"FERRICSTORE.QUOTA", "SET", "tenant:a:", "BYTES", int64(1024), "OPS_PER_SEC", 20},
		{"FERRICSTORE.QUOTA", "GET", "tenant:a:"},
		{"FERRICSTORE.QUOTA", "USAGE", "tenant:a:"},
		{"FERRICSTORE.TELEMETRY", "CLUSTER_INFO"},
		{"FERRICSTORE.TELEMETRY", "NAMESPACE_USAGE", "tenant:a:"},
		{"FERRICSTORE.TELEMETRY", "FLOW_QUERY", "STATE", "queued", "TYPE", "order"},
		{"FERRICSTORE.TELEMETRY", "FLOW_HISTORY", "flow-1", "MAX_EVENTS", 10},
	}
	if len(exec.calls) != len(want) {
		t.Fatalf("unexpected calls: %#v", exec.calls)
	}
	for i := range want {
		if !reflect.DeepEqual(exec.calls[i], want[i]) {
			t.Fatalf("unexpected call %d\n got: %#v\nwant: %#v", i, exec.calls[i], want[i])
		}
	}
}
