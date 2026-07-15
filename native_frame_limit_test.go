package ferricstore

import "testing"

func TestNativeNegotiatedFrameLimitCannotExceedClientHardLimit(t *testing.T) {
	exec := newNativeExecutor(defaultNativeOptions("unused", false))
	exec.mu.Lock()
	exec.applyStartupCapabilitiesLocked(map[string]any{
		"max_frame_bytes": int64(nativeMaxFrameBytes) * 2,
	})
	got := exec.maxRequestFrameBytes
	exec.mu.Unlock()
	defer func() { _ = exec.Close() }()
	if got != nativeMaxFrameBytes {
		t.Fatalf("negotiated request frame limit = %d; want client hard limit %d", got, nativeMaxFrameBytes)
	}
}
