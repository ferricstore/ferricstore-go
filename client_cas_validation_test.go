package ferricstore

import (
	"context"
	"strconv"
	"testing"
)

func TestCASRejectsNonPositiveExpiryBeforeCodecOrTransport(t *testing.T) {
	for _, expiry := range []int64{0, -1} {
		t.Run(strconv.FormatInt(expiry, 10), func(t *testing.T) {
			exec := &fakeExecutor{value: true}
			codec := &countingKVCodec{}
			client := NewClientWithExecutor(exec, WithConcurrentCodec(codec))

			if _, err := client.CAS(context.Background(), "key", "old", "new", &expiry); err == nil {
				t.Fatal("non-positive CAS expiry succeeded")
			}
			if codec.encodes.Load() != 0 {
				t.Fatalf("invalid CAS expiry invoked codec %d times", codec.encodes.Load())
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid CAS expiry reached executor: %#v", exec.calls)
			}
		})
	}
}
