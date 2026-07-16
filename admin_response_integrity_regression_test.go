package ferricstore

import (
	"context"
	"testing"
)

func TestAdminResponsesRejectNonStringMapKeys(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{value: map[interface{}]interface{}{
		int64(1): []byte("value"),
	}})
	if _, err := client.EnsureNamespace(context.Background(), "prefix", nil); err == nil {
		t.Fatal("admin response accepted a non-string map key")
	}
}

func TestAdminResponsesRejectKeysDuplicatedAfterNormalization(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{value: map[interface{}]interface{}{
		int64(1): []byte("first"),
		"1":      []byte("second"),
	}})
	if _, err := client.Capabilities(context.Background()); err == nil {
		t.Fatal("admin response silently overwrote a duplicate normalized key")
	}
}

func TestAdminArrayResponsesRejectMalformedNestedMaps(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{value: []any{
		map[interface{}]interface{}{true: "value"},
	}})
	if _, err := client.InvocationDefinitionList(context.Background(), RequestContextOptions{}); err == nil {
		t.Fatal("admin array accepted a malformed nested map")
	}
}
