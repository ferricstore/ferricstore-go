package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestFetchOrComputeParsesV080ProtocolShapesStrictly(t *testing.T) {
	token := []byte("ownership-token")
	client := NewClientWithExecutor(&fakeExecutor{value: []any{
		[]byte("compute"), []byte("caller-hint"), token,
	}})
	result, err := client.FetchOrCompute(context.Background(), "cache-key", 1_000, "caller-hint")
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "compute" || result.Hint != "caller-hint" ||
		!reflect.DeepEqual(result.OwnershipToken, token) {
		t.Fatalf("compute result = %#v", result)
	}

	client = NewClientWithExecutor(&fakeExecutor{value: []any{"hit", []byte("cached")}})
	result, err = client.FetchOrCompute(context.Background(), "cache-key", 1_000, "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "hit" || !reflect.DeepEqual(result.Value, []byte("cached")) {
		t.Fatalf("hit result = %#v", result)
	}

	malformed := []any{
		[]any{"compute"},
		[]any{"compute", "legacy-tokenless"},
		[]any{"compute", "hint", ""},
		[]any{"compute", "hint", "token", "trailing"},
		[]any{"hit"},
		[]any{"hit", "value", "trailing"},
		[]any{"unknown", "value"},
		[]any{int64(1), "value"},
	}
	for _, response := range malformed {
		client = NewClientWithExecutor(&fakeExecutor{value: response})
		if _, err := client.FetchOrCompute(context.Background(), "cache-key", 1_000, ""); err == nil {
			t.Fatalf("accepted malformed fetch_or_compute response %#v", response)
		}
	}
}

func TestFetchOrComputeCompletionsAlwaysIncludeOwnershipToken(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)
	token := []byte("ownership-token")

	ok, err := client.FetchOrComputeResult(context.Background(), "cache-key", token, []byte("value"), 2_000)
	if err != nil || !ok {
		t.Fatalf("FetchOrComputeResult = %v, %v", ok, err)
	}
	ok, err = client.FetchOrComputeError(context.Background(), "cache-key", token, "failed")
	if err != nil || !ok {
		t.Fatalf("FetchOrComputeError = %v, %v", ok, err)
	}

	want := [][]any{
		{"FETCH_OR_COMPUTE_RESULT", "cache-key", token, []byte("value"), int64(2_000)},
		{"FETCH_OR_COMPUTE_ERROR", "cache-key", token, "failed"},
	}
	if !reflect.DeepEqual(exec.calls, want) {
		t.Fatalf("completion calls = %#v; want %#v", exec.calls, want)
	}
}

func TestFetchOrComputeCompletionsRejectInvalidOwnershipTokens(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)

	if _, err := client.FetchOrComputeResult(context.Background(), "cache-key", "", "value", 1_000); err == nil {
		t.Fatal("FetchOrComputeResult accepted an empty ownership token")
	}
	if _, err := client.FetchOrComputeError(context.Background(), "cache-key", nil, "failed"); err == nil {
		t.Fatal("FetchOrComputeError accepted a nil ownership token")
	}
	if len(exec.calls) != 0 {
		t.Fatalf("invalid completion issued %d commands", len(exec.calls))
	}
}

func TestFetchOrComputeCompletionsAcceptNamedOwnershipTokenTypes(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)

	if ok, err := client.FetchOrComputeResult(
		context.Background(), "cache-key", namedCommandString("token"), "value", 1_000,
	); err != nil || !ok {
		t.Fatalf("named string ownership token = %v, %v", ok, err)
	}
	if ok, err := client.FetchOrComputeError(
		context.Background(), "cache-key", namedCommandBytes("token"), "failed",
	); err != nil || !ok {
		t.Fatalf("named byte ownership token = %v, %v", ok, err)
	}
}
