package ferricstore

import (
	"bufio"
	"testing"
)

func TestNativeManagementOnlyConnectionDoesNotAllocatePublicEventQueue(t *testing.T) {
	options := defaultNativeOptions("unused", false)
	options.HeartbeatInterval = 0
	options.eventSubscription = &nativeEventSubscription{handler: func(nativeServerEvent) {}}
	exec := newNativeExecutor(options)
	exec.mu.Lock()
	exec.installNativeConnectionLocked(&nativeConnectedTransport{
		reader: bufio.NewReader(nil),
		writer: bufio.NewWriter(nil),
	})
	hasEvents := exec.events != nil
	exec.mu.Unlock()
	defer func() { _ = exec.Close() }()
	if hasEvents {
		t.Fatal("management-only native connection allocated an undrained public event queue")
	}
}

func TestNativePublicEventQueueHasByteLimit(t *testing.T) {
	pubsub := NewPubSub("unused", WithNativeHeartbeat(0, 0))
	defer func() { _ = pubsub.Close() }()
	payload := make([]byte, 1<<20)
	for range 17 {
		pubsub.exec.deliverEvent(payload)
	}
	if dropped := pubsub.DroppedEvents(); dropped == 0 {
		t.Fatal("17 MiB of events fit in the public queue without a byte-bound drop")
	}
}
