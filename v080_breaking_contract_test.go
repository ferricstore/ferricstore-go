package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestV080FetchOrComputeRequiresOwnershipToken(t *testing.T) {
	token := []byte("ownership-token")
	exec := &fakeExecutor{values: []any{
		[]any{"compute", "caller-hint", token},
		[]byte("OK"),
		[]byte("OK"),
	}}
	client := NewClientWithExecutor(exec)

	result, err := client.FetchOrCompute(context.Background(), "cache-key", 1_000, "caller-hint")
	if err != nil {
		t.Fatal(err)
	}
	if result.Hint != "caller-hint" || !reflect.DeepEqual(result.OwnershipToken, token) {
		t.Fatalf("compute result = %#v", result)
	}
	if ok, err := client.FetchOrComputeResult(
		context.Background(), "cache-key", token, []byte("value"), 2_000,
	); err != nil || !ok {
		t.Fatalf("FetchOrComputeResult = %v, %v", ok, err)
	}
	if ok, err := client.FetchOrComputeError(
		context.Background(), "cache-key", token, "failed",
	); err != nil || !ok {
		t.Fatalf("FetchOrComputeError = %v, %v", ok, err)
	}

	want := [][]any{
		{"FETCH_OR_COMPUTE", "cache-key", int64(1_000), "caller-hint"},
		{"FETCH_OR_COMPUTE_RESULT", "cache-key", token, []byte("value"), int64(2_000)},
		{"FETCH_OR_COMPUTE_ERROR", "cache-key", token, "failed"},
	}
	if !reflect.DeepEqual(exec.calls, want) {
		t.Fatalf("calls = %#v; want %#v", exec.calls, want)
	}
}

func TestV080TopKReserveHasNoDecay(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)

	ok, err := client.TopK().ReserveWithOptions(context.Background(), "tk", 3, TopKReserveOptions{
		Width: Int64(20),
		Depth: Int64(7),
	})
	if err != nil || !ok {
		t.Fatalf("ReserveWithOptions = %v, %v", ok, err)
	}
	assertCall(t, exec, []any{"TOPK.RESERVE", "tk", int64(3), int64(20), int64(7)})
}

func TestV080PublicSurfaceOmitsUnsupportedInvocationCommands(t *testing.T) {
	clientType := reflect.TypeOf((*Client)(nil))
	for _, name := range []string{
		"InvocationCreate",
		"InvocationDefinitionGet",
		"InvocationDefinitionList",
		"InvocationDefinitionPut",
		"InvocationGet",
		"InvocationPartitionList",
	} {
		if _, exists := clientType.MethodByName(name); exists {
			t.Errorf("exact FerricStore v0.8.0 does not support Client.%s", name)
		}
	}
}

func TestV080SignalPublicSurfaceOmitsUnsupportedPriority(t *testing.T) {
	if _, exists := reflect.TypeOf(SignalOptions{}).FieldByName("Priority"); exists {
		t.Fatal("exact FerricStore v0.8.0 FLOW.SIGNAL does not support priority")
	}
}

func TestV080RawSignalRejectsUnsupportedPriorityBeforeTransport(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)

	if _, err := client.Command(
		context.Background(),
		"FLOW.SIGNAL", "flow-1", "SIGNAL", "wake", "PRIORITY", int64(1),
	); err == nil {
		t.Fatal("FLOW.SIGNAL accepted unsupported PRIORITY")
	}
	if len(exec.calls) != 0 {
		t.Fatalf("invalid FLOW.SIGNAL reached transport: %#v", exec.calls)
	}
}

func TestV080FlowPublicSurfaceOmitsRejectedMutationFields(t *testing.T) {
	for _, optionType := range []struct {
		name   string
		value  any
		fields []string
	}{
		{
			name: "RetryOptions", value: RetryOptions{},
			fields: []string{"Values", "ValueRefs", "DropValues", "OverrideValues"},
		},
		{
			name: "RetryManyOptions", value: RetryManyOptions{},
			fields: []string{"Values", "ValueRefs", "DropValues", "OverrideValues"},
		},
		{
			name: "RetryResult", value: RetryResult{},
			fields: []string{"Values", "ValueRefs", "DropValues", "OverrideValues"},
		},
		{name: "RewindOptions", value: RewindOptions{}, fields: []string{"ReasonRef"}},
	} {
		t.Run(optionType.name, func(t *testing.T) {
			typeOf := reflect.TypeOf(optionType.value)
			for _, field := range optionType.fields {
				if _, exists := typeOf.FieldByName(field); exists {
					t.Errorf("exact FerricStore v0.8.0 %s rejects field %s", optionType.name, field)
				}
			}
			for _, supported := range []string{"AttributesMerge", "AttributesDelete"} {
				if optionType.name != "RewindOptions" {
					if _, exists := typeOf.FieldByName(supported); !exists {
						t.Errorf("%s lost supported field %s", optionType.name, supported)
					}
				}
			}
		})
	}
}
