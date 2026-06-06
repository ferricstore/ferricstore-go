package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestKeyValueStoreBuildsCommands(t *testing.T) {
	exec := &fakeExecutor{value: int64(2)}
	client := NewClientWithExecutor(exec)

	n, err := client.KV().Incr(context.Background(), "counter")

	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("expected 2, got %d", n)
	}
	assertCall(t, exec, []any{"INCR", "counter"})
}

func TestJSONStoreGetAppendsOptionalPath(t *testing.T) {
	exec := &fakeExecutor{value: []byte(`"ok"`)}
	client := NewClientWithExecutor(exec)

	value, err := client.JSON().Get(context.Background(), "doc:1", "$.status")

	if err != nil {
		t.Fatal(err)
	}
	if value != "ok" {
		t.Fatalf("expected decoded status, got %#v", value)
	}
	assertCall(t, exec, []any{"JSON.GET", "doc:1", "$.status"})
}

func TestFlowValuePutBuildsCommand(t *testing.T) {
	exec := &fakeExecutor{value: []byte("ref-1")}
	client := NewClientWithExecutor(exec)
	override := true
	ttl := int64(60000)

	_, err := client.PutValue(context.Background(), "summary", []byte("payload"), ValuePutOptions{
		PartitionKey: "tenant:1",
		OwnerFlowID:  "flow-1",
		Override:     &override,
		TTLMS:        &ttl,
		NowMS:        100,
	})

	if err != nil {
		t.Fatal(err)
	}
	want := []any{
		"FLOW.VALUE.PUT", []byte("payload"), "NOW", int64(100),
		"PARTITION", "tenant:1", "OWNER_FLOW_ID", "flow-1",
		"NAME", "summary", "OVERRIDE", "true", "TTL", int64(60000),
	}
	if !reflect.DeepEqual(exec.calls[0], want) {
		t.Fatalf("unexpected call\n got: %#v\nwant: %#v", exec.calls[0], want)
	}
}
