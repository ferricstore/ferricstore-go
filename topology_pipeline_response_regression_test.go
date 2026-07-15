package ferricstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTopologyPipelineValidatesSingleRouteScatterResponses(t *testing.T) {
	listenerA, _, errsA := startRoutedNativeEndpoint(t, func(frame nativeFrame, _ int) any {
		if frame.opcode != nativeOpPipeline {
			return nil
		}
		return []any{
			[]any{"ok", []any{}},
			[]any{"ok", int64(2)},
		}
	})
	listenerB, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return nil })
	exec, keyA, _ := topologyExecutorForTwoEndpoints(t, listenerA, listenerB)
	t.Cleanup(func() { _ = exec.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	values, err := exec.Pipeline(ctx, [][]any{
		{"MGET", keyA},
		{"EXISTS", keyA},
	})
	if err == nil {
		t.Fatal("topology pipeline accepted malformed single-route scatter responses")
	}
	var pipelineErr *PipelineError
	if !errors.As(err, &pipelineErr) || len(pipelineErr.Failures) != 2 {
		t.Fatalf("pipeline error = %v; want two item failures", err)
	}
	if len(values) != 2 {
		t.Fatalf("pipeline values = %#v; want two error positions", values)
	}
	for index, value := range values {
		if _, ok := value.(error); !ok {
			t.Fatalf("pipeline value %d = %#v; want error", index, value)
		}
	}
	if endpointErr := <-errsA; endpointErr != nil {
		t.Fatal(endpointErr)
	}
}
