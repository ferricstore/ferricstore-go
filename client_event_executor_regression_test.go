package ferricstore

import (
	"testing"
	"time"
)

func TestAutoBatchClientExposesUnderlyingNativeEvents(t *testing.T) {
	client := NewAutoBatchClient(
		"127.0.0.1:6388",
		AutoBatchOptions{FlushInterval: time.Hour},
	)
	defer func() { _ = client.Close() }()
	auto := client.exec.(*AutoBatchExecutor)
	native := auto.client.exec.(*NativeExecutor)
	native.droppedEvents.Store(7)

	pubsub, err := client.OpenPubSub()
	if err != nil {
		t.Fatal(err)
	}
	if pubsub.exec != native || pubsub.owned {
		t.Fatalf("shared pubsub = %#v; want underlying non-owning native view", pubsub)
	}
	if got := client.DroppedEvents(); got != 7 {
		t.Fatalf("dropped events = %d; want 7", got)
	}
}

func TestBufferedClientExposesUnderlyingNativeEvents(t *testing.T) {
	native := NewNativeExecutor("127.0.0.1:6388")
	defer func() { _ = native.Close() }()
	base := NewClientWithExecutor(native)
	client := NewClientWithExecutor(NewBufferedExecutor(base))
	native.droppedEvents.Store(11)

	pubsub, err := client.OpenPubSub()
	if err != nil {
		t.Fatal(err)
	}
	if pubsub.exec != native || pubsub.owned {
		t.Fatalf("shared pubsub = %#v; want underlying non-owning native view", pubsub)
	}
	if got := client.DroppedEvents(); got != 11 {
		t.Fatalf("dropped events = %d; want 11", got)
	}
}

func TestTopologyDroppedEventsSumsUniqueAdapters(t *testing.T) {
	first := NewNativeExecutor("127.0.0.1:6388")
	second := NewNativeExecutor("127.0.0.1:6389")
	defer func() { _ = first.Close() }()
	defer func() { _ = second.Close() }()
	first.droppedEvents.Store(2)
	second.droppedEvents.Store(3)
	exec := &TopologyNativeExecutor{
		adapters: map[string]*NativeExecutor{
			"first":  first,
			"alias":  first,
			"second": second,
		},
	}

	if got := exec.DroppedEvents(); got != 5 {
		t.Fatalf("topology dropped events = %d; want 5", got)
	}
	if got := NewClientWithExecutor(exec).DroppedEvents(); got != 5 {
		t.Fatalf("topology client dropped events = %d; want 5", got)
	}
}
