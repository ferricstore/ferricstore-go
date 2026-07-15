package ferricstore

import (
	"context"
	"testing"
)

func TestClaimJobsDecodesReturnedPayloadWithClientCodec(t *testing.T) {
	exec := &fakeExecutor{value: []any{map[string]any{
		"id": "flow-1", "type": "order", "state": "running",
		"lease_token": "lease-1", "fencing_token": int64(7),
		"payload": []byte(`{"ok":true}`),
	}}}
	client := NewClientWithExecutor(exec, WithCodec(JSONCodec{}))

	jobs, err := client.ClaimJobs(context.Background(), ClaimDueOptions{
		Type: "order", Worker: "worker-1", Payload: Bool(true),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("claimed jobs = %#v", jobs)
	}
	payload, ok := jobs[0].Payload.(map[string]any)
	if !ok || payload["ok"] != true {
		t.Fatalf("claimed payload = %#v; want decoded JSON object", jobs[0].Payload)
	}
}
