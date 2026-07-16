package ferricstore

import (
	"context"
	"testing"
	"unsafe"
)

type commandSnapshotUnsafeHolder struct {
	pointer unsafe.Pointer
}

func TestBufferedSnapshotHandlesUnexportedUnsafePointerWithoutPanicking(t *testing.T) {
	buffered := NewBufferedExecutor(nil)
	if _, err := buffered.Do(context.Background(), "CUSTOM", commandSnapshotUnsafeHolder{}); err != nil {
		t.Fatalf("nil unsafe pointer snapshot error = %v", err)
	}

	value := 1
	_, err := buffered.Do(context.Background(), "CUSTOM", commandSnapshotUnsafeHolder{
		pointer: unsafe.Pointer(&value),
	})
	if err == nil {
		t.Fatal("non-nil unsafe pointer snapshot succeeded; want ownership error")
	}
}

func TestBufferedSnapshotRejectsUnsnapshotableTopLevelReferences(t *testing.T) {
	value := 1
	tests := []struct {
		name  string
		value any
	}{
		{name: "channel", value: make(chan int)},
		{name: "function", value: func() {}},
		{name: "unsafe pointer", value: unsafe.Pointer(&value)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buffered := NewBufferedExecutor(nil)
			if _, err := buffered.Do(context.Background(), "CUSTOM", test.value); err == nil {
				t.Fatal("unsnapshotable command argument was queued")
			}
		})
	}
}

func TestBufferedSnapshotAcceptsNonCyclicOverlappingSlices(t *testing.T) {
	value := []any{"original", nil}
	value[1] = value[:1]
	buffered := NewBufferedExecutor(nil)

	if _, err := buffered.Do(context.Background(), "CUSTOM", value); err != nil {
		t.Fatalf("non-cyclic overlapping slices were rejected: %v", err)
	}
	value[0] = "mutated"
	queued := buffered.commands[0][1].([]any)
	if queued[0] != "original" {
		t.Fatalf("queued outer value = %#v; want owned snapshot", queued[0])
	}
	nested := queued[1].([]any)
	if nested[0] != "original" {
		t.Fatalf("queued nested value = %#v; want owned snapshot", nested[0])
	}
}

func TestBufferedSnapshotStillRejectsSliceCycles(t *testing.T) {
	value := make([]any, 1)
	value[0] = value
	if _, err := NewBufferedExecutor(nil).Do(context.Background(), "CUSTOM", value); err == nil {
		t.Fatal("cyclic slice was queued")
	}
}
