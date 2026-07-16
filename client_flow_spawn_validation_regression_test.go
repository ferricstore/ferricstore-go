package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestSpawnChildrenRejectsInvalidOptionsBeforeCodecOrTransport(t *testing.T) {
	fencing := int64(1)
	negative := int64(-1)
	base := func() SpawnChildrenOptions {
		return SpawnChildrenOptions{
			ParentID: "parent", PartitionKey: "tenant", FencingToken: &fencing,
			Success: "done", Failure: "failed",
			Children: []ChildSpec{{ID: "child", Type: "order", Payload: "payload"}},
		}
	}
	tests := []struct {
		name   string
		mutate func(*SpawnChildrenOptions)
	}{
		{name: "missing parent", mutate: func(opt *SpawnChildrenOptions) { opt.ParentID = "" }},
		{name: "missing partition", mutate: func(opt *SpawnChildrenOptions) { opt.PartitionKey = "" }},
		{name: "missing fencing", mutate: func(opt *SpawnChildrenOptions) { opt.FencingToken = nil }},
		{name: "negative fencing", mutate: func(opt *SpawnChildrenOptions) { opt.FencingToken = &negative }},
		{name: "missing children", mutate: func(opt *SpawnChildrenOptions) { opt.Children = nil }},
		{name: "missing child id", mutate: func(opt *SpawnChildrenOptions) { opt.Children[0].ID = "" }},
		{name: "missing child type", mutate: func(opt *SpawnChildrenOptions) { opt.Children[0].Type = "" }},
		{name: "child equals parent", mutate: func(opt *SpawnChildrenOptions) { opt.Children[0].ID = opt.ParentID }},
		{name: "duplicate child", mutate: func(opt *SpawnChildrenOptions) { opt.Children = append(opt.Children, opt.Children[0]) }},
		{name: "reserved group", mutate: func(opt *SpawnChildrenOptions) { opt.GroupID = "__internal" }},
		{name: "invalid wait", mutate: func(opt *SpawnChildrenOptions) { opt.Wait = "sometimes" }},
		{name: "invalid child failure", mutate: func(opt *SpawnChildrenOptions) { opt.OnChildFailed = "retry" }},
		{name: "invalid parent close", mutate: func(opt *SpawnChildrenOptions) { opt.OnParentClosed = "keep" }},
		{name: "missing success", mutate: func(opt *SpawnChildrenOptions) { opt.Success = "" }},
		{name: "missing failure", mutate: func(opt *SpawnChildrenOptions) { opt.Failure = "" }},
		{name: "negative now", mutate: func(opt *SpawnChildrenOptions) { opt.NowMS = -1 }},
		{name: "unsupported attributes", mutate: func(opt *SpawnChildrenOptions) { opt.Children[0].Attributes = map[string]any{"tenant": "acme"} }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opt := base()
			test.mutate(&opt)
			exec := &fakeExecutor{value: []byte("OK")}
			codec := &countingKVCodec{}
			_, err := NewClientWithExecutor(exec, WithCodec(codec)).SpawnChildren(context.Background(), opt)
			if err == nil {
				t.Fatal("invalid spawn options were accepted")
			}
			if calls := codec.encodes.Load(); calls != 0 {
				t.Fatalf("invalid spawn invoked codec %d times", calls)
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid spawn reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestSpawnChildrenFillsParentPartitionForMixedChildren(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)
	_, err := client.SpawnChildren(context.Background(), SpawnChildrenOptions{
		ParentID: "parent", PartitionKey: "tenant:parent", FencingToken: Int64(1),
		Success: "done", Failure: "failed",
		Children: []ChildSpec{
			{ID: "child-a", Type: "order", Payload: []byte("a")},
			{ID: "child-b", Type: "order", PartitionKey: "tenant:b", Payload: []byte("b")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	wantTail := []any{
		"ITEMS", "MIXED",
		"child-a", "tenant:parent", "order", []byte("a"),
		"child-b", "tenant:b", "order", []byte("b"),
	}
	call := exec.calls[0]
	if len(call) < len(wantTail) || !reflect.DeepEqual(call[len(call)-len(wantTail):], wantTail) {
		t.Fatalf("spawn tail = %#v, want %#v", call, wantTail)
	}
}
