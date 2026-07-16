package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestFetchOrComputeParsesProtocolShapesStrictly(t *testing.T) {
	computeToken := []byte("compute-token")
	client := NewClientWithExecutor(&fakeExecutor{value: []any{
		[]byte("compute"), []byte("caller-hint"), computeToken,
	}})
	result, err := client.FetchOrCompute(context.Background(), "cache-key", 1_000, "caller-hint")
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "compute" || !reflect.DeepEqual(result.ComputeToken, computeToken) {
		t.Fatalf("compute result = %#v; want token from third response field", result)
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
		[]any{"compute", ""},
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

func TestFetchOrComputeSupportsReleasedProtocolShape(t *testing.T) {
	legacyChannel := []byte("legacy-channel")
	exec := &fakeExecutor{values: []any{
		[]any{[]byte("compute"), legacyChannel},
		[]byte("OK"),
		[]byte("OK"),
	}}
	client := NewClientWithExecutor(exec)

	result, err := client.FetchOrCompute(context.Background(), "cache-key", 1_000, "caller-hint")
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "compute" || result.ComputeToken == nil {
		t.Fatalf("legacy compute result = %#v", result)
	}
	if ok, err := client.FetchOrComputeResultWithToken(
		context.Background(), "cache-key", []byte("value"), 2_000, result.ComputeToken,
	); err != nil || !ok {
		t.Fatalf("legacy FetchOrComputeResult = %v, %v", ok, err)
	}
	if ok, err := client.FetchOrComputeErrorWithToken(
		context.Background(), "cache-key", "failed", result.ComputeToken,
	); err != nil || !ok {
		t.Fatalf("legacy FetchOrComputeError = %v, %v", ok, err)
	}

	want := [][]any{
		{"FETCH_OR_COMPUTE", "cache-key", int64(1_000), "caller-hint"},
		{"FETCH_OR_COMPUTE_RESULT", "cache-key", []byte("value"), int64(2_000)},
		{"FETCH_OR_COMPUTE_ERROR", "cache-key", "failed"},
	}
	if !reflect.DeepEqual(exec.calls, want) {
		t.Fatalf("legacy protocol calls = %#v; want %#v", exec.calls, want)
	}
}

func TestFetchOrComputeCompletionIncludesComputeToken(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)
	token := []byte("compute-token")

	ok, err := client.FetchOrComputeResultWithToken(context.Background(), "cache-key", []byte("value"), 2_000, token)
	if err != nil || !ok {
		t.Fatalf("FetchOrComputeResult = %v, %v", ok, err)
	}
	ok, err = client.FetchOrComputeErrorWithToken(context.Background(), "cache-key", "failed", token)
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

func TestFetchOrComputeCompletionSupportsLegacyOmittedToken(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)

	if ok, err := client.FetchOrComputeResult(context.Background(), "cache-key", "value", 1_000); err != nil || !ok {
		t.Fatalf("legacy FetchOrComputeResult = %v, %v", ok, err)
	}
	if ok, err := client.FetchOrComputeError(context.Background(), "cache-key", "failed"); err != nil || !ok {
		t.Fatalf("legacy FetchOrComputeError = %v, %v", ok, err)
	}

	want := [][]any{
		{"FETCH_OR_COMPUTE_RESULT", "cache-key", "value", int64(1_000)},
		{"FETCH_OR_COMPUTE_ERROR", "cache-key", "failed"},
	}
	if !reflect.DeepEqual(exec.calls, want) {
		t.Fatalf("legacy completion calls = %#v; want %#v", exec.calls, want)
	}
}

func TestFetchOrComputeCompletionRejectsInvalidTokens(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)

	if _, err := client.FetchOrComputeResultWithToken(context.Background(), "cache-key", "value", 1_000, ""); err == nil {
		t.Fatal("FetchOrComputeResult accepted an empty compute token")
	}
	if len(exec.calls) != 0 {
		t.Fatalf("invalid completion issued %d commands", len(exec.calls))
	}
}
