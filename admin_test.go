package ferricstore

import (
	"context"
	"testing"
)

func TestClusterJoinReplaceBuildsCommand(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)

	ok, err := client.ClusterJoin(context.Background(), "127.0.0.1:7379", true)

	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected join to return true")
	}
	assertCall(t, exec, []any{"CLUSTER.JOIN", "127.0.0.1:7379", "REPLACE"})
}

func TestRetentionCleanupBuildsCommand(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{"removed": int64(3)}}
	client := NewClientWithExecutor(exec)

	result, err := client.RetentionCleanup(context.Background(), RetentionCleanupOptions{
		Limit: Int(10),
		NowMS: Int64(100),
	})

	if err != nil {
		t.Fatal(err)
	}
	if result["removed"] != int64(3) {
		t.Fatalf("unexpected cleanup result: %#v", result)
	}
	assertCall(t, exec, []any{"FLOW.RETENTION_CLEANUP", "LIMIT", 10, "NOW", int64(100)})
}

func TestFerricStoreMetricsParsesTextResponse(t *testing.T) {
	exec := &fakeExecutor{value: []byte("ops: 10\nhealthy: true\n")}
	client := NewClientWithExecutor(exec)

	result, err := client.FerricStoreMetrics(context.Background())

	if err != nil {
		t.Fatal(err)
	}
	if result["ops"] != int64(10) || result["healthy"] != true {
		t.Fatalf("unexpected metrics result: %#v", result)
	}
	assertCall(t, exec, []any{"FERRICSTORE.METRICS"})
}
