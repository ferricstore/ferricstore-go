package ferricstore

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestNewAutoBatchClientAppliesOptionsOnceAndSharesCodec(t *testing.T) {
	codec := &orderedMapCodec{}
	optionCalls := 0
	client := NewAutoBatchClient(
		"127.0.0.1:6388",
		AutoBatchOptions{FlushInterval: time.Hour},
		WithCodec(codec),
		func(*Client) { optionCalls++ },
	)
	defer func() { _ = client.Close() }()

	if optionCalls != 1 {
		t.Fatalf("client option calls = %d; want 1", optionCalls)
	}
	exec, ok := client.exec.(*AutoBatchExecutor)
	if !ok {
		t.Fatalf("client executor = %T; want *AutoBatchExecutor", client.exec)
	}
	if client.codec != exec.client.codec {
		t.Fatal("autobatch client and transport client do not share their codec wrapper")
	}
	if client.Codec() != codec {
		t.Fatalf("client codec = %T; want original codec", client.Codec())
	}
}

func TestNewAutoBatchClientFromURLAppliesOptionsOnceAndSharesCodec(t *testing.T) {
	codec := &orderedMapCodec{}
	optionCalls := 0
	client, err := NewAutoBatchClientFromURL(
		"ferric://127.0.0.1:6388",
		AutoBatchOptions{FlushInterval: time.Hour},
		WithCodec(codec),
		func(*Client) { optionCalls++ },
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	if optionCalls != 1 {
		t.Fatalf("client option calls = %d; want 1", optionCalls)
	}
	exec, ok := client.exec.(*AutoBatchExecutor)
	if !ok {
		t.Fatalf("client executor = %T; want *AutoBatchExecutor", client.exec)
	}
	if client.codec != exec.client.codec {
		t.Fatal("autobatch client and transport client do not share their codec wrapper")
	}
}

func TestAutoBatchExecutorPipelinePublicSurface(t *testing.T) {
	pipeline := &fakePipelineExecutor{prefix: "ok:"}
	exec := NewAutoBatchExecutor(
		NewClientWithExecutor(pipeline),
		AutoBatchOptions{MaxSize: 10, FlushInterval: time.Hour},
	)
	defer func() { _ = exec.Close() }()
	commands := [][]any{{"SET", "a", "1"}, {"GET", "a"}}

	values, err := exec.Pipeline(context.Background(), commands)
	if err != nil {
		t.Fatal(err)
	}
	want := []any{[]byte("ok:SET"), []byte("ok:GET")}
	if !reflect.DeepEqual(values, want) {
		t.Fatalf("pipeline values = %#v; want %#v", values, want)
	}
}
