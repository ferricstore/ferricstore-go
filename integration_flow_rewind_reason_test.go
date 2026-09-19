//go:build integration && !compatibility_floor

package ferricstore

import (
	"fmt"
	"testing"
	"time"
)

func TestIntegrationV0119RewindAcceptsCodecEncodedReason(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationDirectClient(JSONCodec{})
	defer client.Close()

	runID := integrationSuffix("flow-contract-rewind")
	flowType := "go-sdk-flow-contract-rewind-" + runID
	id := "go-sdk-flow-contract-rewind:" + runID
	partition := id + ":partition"
	now := time.Now().UnixMilli()
	created := must[*FlowRecord](t)(client.Create(ctx, CreateOptions{
		ID:           id,
		Type:         flowType,
		State:        "queued",
		PartitionKey: partition,
		Payload:      map[string]any{"kind": "rewind", "marker": runID},
		RunAtMS:      now,
		NowMS:        now,
		Idempotent:   Bool(true),
		ReturnRecord: true,
	}))
	if created == nil || created.Version <= 0 {
		t.Fatalf("create event identity missing: %+v", created)
	}
	createdAtMS := asInt64(created.Raw["updated_at_ms"])
	if createdAtMS <= 0 {
		t.Fatalf("create updated_at_ms missing: %#v", created.Raw)
	}
	job := claimOne(t, ctx, client, flowType, "queued", partition, "go-sdk-flow-contract-rewind-worker", now+1, 30_000)
	must[*FlowRecord](t)(client.Complete(ctx, CompleteOptions{
		ID:           job.ID,
		LeaseToken:   job.LeaseToken,
		FencingToken: job.FencingToken,
		PartitionKey: partition,
		NowMS:        now + 2,
	}))
	flushHistoryProjectorForRewind(t, ctx, client, claimedFlow{
		id:             id,
		partitionKey:   partition,
		createdEventID: fmt.Sprintf("%d-%d", createdAtMS, created.Version),
	})

	reason := map[string]any{"source": "go-sdk", "attempt": int64(2)}
	if _, err := client.Rewind(ctx, RewindOptions{
		ID:           id,
		ToEvent:      fmt.Sprintf("%d-%d", createdAtMS, created.Version),
		Reason:       reason,
		PartitionKey: partition,
		ExpectState:  "completed",
		NowMS:        now + 3,
	}); err != nil {
		t.Fatal(err)
	}
	rewound := must[*FlowRecord](t)(client.Get(ctx, id, partition, nil))
	if rewound == nil || rewound.State != "queued" ||
		rewound.Version != created.Version+3 ||
		rewound.FencingToken != job.FencingToken+1 {
		t.Fatalf("rewound state/version/fencing = %+v; created=%+v job=%+v", rewound, created, job)
	}
	rewoundEventID := fmt.Sprintf("%d-%d", createdAtMS, created.Version)
	if asString(rewound.Raw["rewound_to_event_id"]) != rewoundEventID {
		t.Fatalf("rewound_to_event_id = %#v, want %q", rewound.Raw["rewound_to_event_id"], rewoundEventID)
	}
	reasonRef := asString(rewound.Raw["error_ref"])
	if reasonRef == "" {
		t.Fatalf("rewound error_ref missing: %#v", rewound.Raw)
	}
	values := must[[]any](t)(client.ValueMGet(ctx, []string{reasonRef}, nil))
	requireLen(t, values, 1)
	decodedReason, ok := values[0].(map[string]any)
	if !ok || len(decodedReason) != 2 || asString(decodedReason["source"]) != "go-sdk" || asInt64(decodedReason["attempt"]) != 2 {
		t.Fatalf("rewound reason = %#v, want decoded source/attempt", values[0])
	}
}
