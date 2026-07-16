package ferricstore

import "testing"

func TestNilBufferedFlushErrorIsSafeToInspect(t *testing.T) {
	var err *BufferedFlushError
	if got := err.Error(); got != "" {
		t.Fatalf("nil BufferedFlushError text = %q; want empty", got)
	}
	if got := err.Unwrap(); got != nil {
		t.Fatalf("nil BufferedFlushError unwrap = %v; want nil", got)
	}
}
