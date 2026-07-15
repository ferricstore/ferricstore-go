package ferricstore

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestAutoBatchClientPipelineFlushesImmediatelyInOrder(t *testing.T) {
	pipeline := &fakePipelineExecutor{prefix: "ok:"}
	exec := NewAutoBatchExecutor(
		NewClientWithExecutor(pipeline),
		AutoBatchOptions{MaxSize: 10, FlushInterval: time.Hour},
	)
	defer func() { _ = exec.Close() }()
	client := NewClientWithExecutor(exec)
	commands := [][]any{{"SET", "a", "1"}, {"GET", "a"}, {"PING"}}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	values, err := client.Pipeline(ctx, commands)
	if err != nil {
		t.Fatal(err)
	}
	if got := pipeline.batchCount(); got != 1 {
		t.Fatalf("downstream batch count = %d; want 1", got)
	}
	if got := pipeline.firstBatch(); !reflect.DeepEqual(got, commands) {
		t.Fatalf("downstream batch = %#v; want %#v", got, commands)
	}
	want := []any{[]byte("ok:SET"), []byte("ok:GET"), []byte("ok:PING")}
	if !reflect.DeepEqual(values, want) {
		t.Fatalf("pipeline values = %#v; want %#v", values, want)
	}
}

func TestAutoBatchClientPipelinePreservesPerItemErrors(t *testing.T) {
	wantErr := errors.New("command failed")
	exec := NewAutoBatchExecutor(
		NewClientWithExecutor(&itemErrorPipelineExecutor{wantErr: wantErr}),
		AutoBatchOptions{MaxSize: 10, FlushInterval: time.Hour},
	)
	defer func() { _ = exec.Close() }()

	values, err := NewClientWithExecutor(exec).Pipeline(
		context.Background(),
		[][]any{{"PING"}, {"FAIL"}, {"GET", "key"}},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("pipeline error = %v; want %v", err, wantErr)
	}
	if len(values) != 3 {
		t.Fatalf("pipeline values = %#v", values)
	}
	if itemErr, ok := values[1].(error); !ok || !errors.Is(itemErr, wantErr) {
		t.Fatalf("failed pipeline item = %#v; want %v", values[1], wantErr)
	}
}

type autoBatchAllocationPipelineExecutor struct{}

func (*autoBatchAllocationPipelineExecutor) Do(context.Context, ...any) (any, error) {
	return nil, errors.New("unexpected direct Do call")
}

func (*autoBatchAllocationPipelineExecutor) Pipeline(
	_ context.Context,
	commands [][]any,
) ([]any, error) {
	values := make([]any, len(commands))
	for index := range values {
		values[index] = []byte("value")
	}
	return values, nil
}

var autoBatchAllocationPipelineSink []any

func TestAutoBatchExplicitPipelineHasBoundedAllocationOverhead(t *testing.T) {
	commands := make([][]any, 100)
	for index := range commands {
		commands[index] = []any{"GET", "key:" + strconv.Itoa(index)}
	}
	exec := NewAutoBatchExecutor(
		NewClientWithExecutor(&autoBatchAllocationPipelineExecutor{}),
		AutoBatchOptions{MaxSize: 100, FlushInterval: time.Hour, QueueSize: 128},
	)
	defer func() { _ = exec.Close() }()
	client := NewClientWithExecutor(exec)

	allocations := testing.AllocsPerRun(20, func() {
		values, err := client.Pipeline(context.Background(), commands)
		if err != nil {
			panic(err)
		}
		autoBatchAllocationPipelineSink = values
	})
	if allocations > 300 {
		t.Fatalf("explicit 100-command pipeline allocations = %.0f; want <= 300", allocations)
	}
}

func BenchmarkAutoBatchExplicitPipeline100(b *testing.B) {
	commands := make([][]any, 100)
	for index := range commands {
		commands[index] = []any{"GET", "key:" + strconv.Itoa(index)}
	}
	pipeline := &fakePipelineExecutor{prefix: "ok:"}
	exec := NewAutoBatchExecutor(
		NewClientWithExecutor(pipeline),
		AutoBatchOptions{MaxSize: 100, FlushInterval: time.Hour, QueueSize: 128},
	)
	defer func() { _ = exec.Close() }()
	client := NewClientWithExecutor(exec)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := client.Pipeline(context.Background(), commands); err != nil {
			b.Fatal(err)
		}
	}
}
