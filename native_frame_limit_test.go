package ferricstore

import "testing"

func TestNativeNegotiatedFrameLimitCannotExceedClientHardLimit(t *testing.T) {
	exec := newNativeExecutor(defaultNativeOptions("unused", false))
	contract, err := parseNativeHelloContract(nativeHelloForTestWithLimits(map[string]any{
		"max_frame_bytes": int64(nativeMaxFrameBytes) * 2,
	}), nativeDefaultResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	exec.mu.Lock()
	exec.applyHelloContractLocked(contract)
	got := exec.maxRequestFrameBytes
	exec.mu.Unlock()
	defer func() { _ = exec.Close() }()
	if got != nativeMaxFrameBytes {
		t.Fatalf("negotiated request frame limit = %d; want client hard limit %d", got, nativeMaxFrameBytes)
	}
}
