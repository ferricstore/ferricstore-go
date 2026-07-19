package ferricstore

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestV080ScheduleRejectsContractViolationsBeforeTransport(t *testing.T) {
	t.Parallel()

	tooLarge := maxFlowExactIntegerV080 + 1
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{
			name: "create at_ms exceeds exact integer",
			call: func(client *Client) error {
				_, err := client.ScheduleCreate(context.Background(), "schedule", ScheduleOptions{
					AtMS: &tooLarge, Target: map[string]any{"type": "job"},
				})
				return err
			},
		},
		{
			name: "delay addition exceeds exact integer",
			call: func(client *Client) error {
				_, err := client.ScheduleCreate(context.Background(), "schedule", ScheduleOptions{
					DelayMS: Int64(1), NowMS: Int64(maxFlowExactIntegerV080),
					Target: map[string]any{"type": "job"},
				})
				return err
			},
		},
		{
			name: "target run_at_ms exceeds exact integer",
			call: func(client *Client) error {
				_, err := client.ScheduleCreate(context.Background(), "schedule", ScheduleOptions{
					Target: map[string]any{"type": "job", "run_at_ms": tooLarge},
				})
				return err
			},
		},
		{
			name: "target lineage reference too large",
			call: func(client *Client) error {
				_, err := client.ScheduleCreate(context.Background(), "schedule", ScheduleOptions{
					Target: map[string]any{"type": "job", "parent_flow_id": strings.Repeat("p", maxFlowReferenceBytesV080+1)},
				})
				return err
			},
		},
		{
			name: "target legacy lineage alias",
			call: func(client *Client) error {
				_, err := client.ScheduleCreate(context.Background(), "schedule", ScheduleOptions{
					Target: map[string]any{"type": "job", "parent_id": "parent"},
				})
				return err
			},
		},
		{
			name: "target reserved fixed id",
			call: func(client *Client) error {
				_, err := client.ScheduleCreate(context.Background(), "schedule", ScheduleOptions{
					Target: map[string]any{"type": "job", "id": "__ferricstore_schedule__:internal"},
				})
				return err
			},
		},
		{
			name: "target reserved id prefix",
			call: func(client *Client) error {
				_, err := client.ScheduleCreate(context.Background(), "schedule", ScheduleOptions{
					Kind: "interval", EveryMS: Int64(1),
					Target: map[string]any{"type": "job", "id_prefix": "__ferricstore_schedule__:internal"},
				})
				return err
			},
		},
		{
			name: "pause now_ms exceeds exact integer",
			call: func(client *Client) error {
				_, err := client.SchedulePause(context.Background(), "schedule", &tooLarge)
				return err
			},
		},
		{
			name: "manual fire time exceeds exact integer",
			call: func(client *Client) error {
				_, err := client.ScheduleFireWithOptions(context.Background(), "schedule", ScheduleFireOptions{FireAtMS: &tooLarge})
				return err
			},
		},
		{
			name: "fire due lease exceeds exact integer",
			call: func(client *Client) error {
				_, err := client.ScheduleFireDueWithOptions(context.Background(), ScheduleFireDueOptions{LeaseMS: &tooLarge})
				return err
			},
		},
		{
			name: "list from_ms exceeds exact integer",
			call: func(client *Client) error {
				_, err := client.ScheduleList(context.Background(), ScheduleListOptions{FromMS: &tooLarge})
				return err
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			exec := &fakeExecutor{value: map[string]any{"id": "schedule", "kind": "one_shot", "status": "active"}}
			if err := test.call(NewClientWithExecutor(exec)); err == nil {
				t.Fatal("invalid v0.8 schedule request succeeded")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid schedule request reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestV080ScheduleTargetUsesClientCodecWithoutMutatingInput(t *testing.T) {
	t.Parallel()

	payload := map[string]any{"kind": "email"}
	values := map[string]any{"recipient": map[string]any{"id": "42"}}
	target := map[string]any{
		"type": "job", "payload": payload, "values": values,
	}
	exec := &fakeExecutor{value: map[string]any{"id": "schedule", "kind": "one_shot", "status": "active"}}
	client := NewClientWithExecutor(exec, WithCodec(JSONCodec{}))

	if _, err := client.ScheduleCreate(context.Background(), "schedule", ScheduleOptions{Target: target}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(target["payload"], payload) || !reflect.DeepEqual(target["values"], values) {
		t.Fatalf("ScheduleCreate mutated caller target: %#v", target)
	}

	gotTarget, ok := exec.calls[0][3].(map[string]any)
	if !ok {
		t.Fatalf("wire target = %T, want map", exec.calls[0][3])
	}
	wantPayload, _ := (JSONCodec{}).Encode(payload)
	wantValue, _ := (JSONCodec{}).Encode(values["recipient"])
	if !reflect.DeepEqual(gotTarget["payload"], wantPayload) {
		t.Fatalf("wire payload = %#v, want encoded %#v", gotTarget["payload"], wantPayload)
	}
	wireValues, ok := gotTarget["values"].(map[string]any)
	if !ok || !reflect.DeepEqual(wireValues["recipient"], wantValue) {
		t.Fatalf("wire values = %#v, want encoded recipient %#v", gotTarget["values"], wantValue)
	}
}

func TestV080ScheduleTargetResponseUsesClientCodecWithoutMutatingRaw(t *testing.T) {
	t.Parallel()

	codec := JSONCodec{}
	encodedPayload, _ := codec.Encode(map[string]any{"kind": "email"})
	encodedRecipient, _ := codec.Encode(map[string]any{"id": "42"})
	rawTarget := map[string]any{
		"type": "job", "payload": encodedPayload,
		"values": map[string]any{"recipient": encodedRecipient},
	}
	exec := &fakeExecutor{value: map[string]any{
		"id": "schedule", "kind": "one_shot", "state": "active", "target": rawTarget,
	}}
	result, err := NewClientWithExecutor(exec, WithCodec(codec)).ScheduleGet(context.Background(), "schedule", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantTarget := map[string]any{
		"type": "job", "payload": map[string]any{"kind": "email"},
		"values": map[string]any{"recipient": map[string]any{"id": "42"}},
	}
	if !reflect.DeepEqual(result.Target, wantTarget) {
		t.Fatalf("decoded target = %#v, want %#v", result.Target, wantTarget)
	}
	if !reflect.DeepEqual(rawTarget["payload"], encodedPayload) ||
		!reflect.DeepEqual(rawTarget["values"], map[string]any{"recipient": encodedRecipient}) {
		t.Fatalf("schedule decoding mutated raw target: %#v", rawTarget)
	}
}
