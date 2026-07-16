package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestClientInfoParsesFerricStoreWhitespaceFormat(t *testing.T) {
	response := []byte("id=42 addr=127.0.0.1:6379 fd=7 name=worker one age=10\n")
	want := map[string]any{
		"id": int64(42), "addr": "127.0.0.1:6379", "fd": int64(7),
		"name": "worker one", "age": int64(10),
	}
	got, err := NewClientWithExecutor(&fakeExecutor{value: response}).ClientInfo(context.Background())
	if err != nil {
		t.Fatalf("CLIENT INFO: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CLIENT INFO = %#v, want %#v", got, want)
	}
}

func TestClientInfoRejectsMalformedOrDuplicateFields(t *testing.T) {
	for _, response := range []any{
		"id=1 malformed age=2",
		"id=1 id=2",
		"=value",
		"   ",
	} {
		client := NewClientWithExecutor(&fakeExecutor{value: response})
		if _, err := client.ClientInfo(context.Background()); err == nil {
			t.Fatalf("accepted malformed CLIENT INFO response %#v", response)
		}
	}
}

func TestServerInfoIgnoresSectionComments(t *testing.T) {
	response := "# Server\r\nredis_version:7.4.0\r\nuptime_in_seconds:12\r\n# Clients\r\nconnected_clients:3\r\n"
	got, err := NewClientWithExecutor(&fakeExecutor{value: response}).ServerInfo(context.Background())
	if err != nil {
		t.Fatalf("INFO: %v", err)
	}
	want := map[string]any{
		"redis_version": "7.4.0", "uptime_in_seconds": int64(12), "connected_clients": int64(3),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("INFO = %#v, want %#v", got, want)
	}
}
