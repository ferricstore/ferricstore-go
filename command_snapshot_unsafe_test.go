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
