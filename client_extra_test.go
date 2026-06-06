package ferricstore

import (
	"context"
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
