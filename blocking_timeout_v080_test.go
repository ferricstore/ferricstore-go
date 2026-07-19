package ferricstore

import (
	"context"
	"testing"
	"time"
)

func TestV080BlockingTimeoutsRejectValuesAboveProtocolMaximum(t *testing.T) {
	t.Parallel()

	tooLarge := int64(0x1_0000_0000)
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{
			name: "XREAD",
			call: func(client *Client) error {
				_, err := client.Stream().Read(context.Background(), StreamReadOptions{
					BlockMS: &tooLarge,
					Streams: []StreamRef{{Key: "events", ID: "0"}},
				})
				return err
			},
		},
		{
			name: "XREADGROUP",
			call: func(client *Client) error {
				_, err := client.Stream().ReadGroup(context.Background(), StreamReadGroupOptions{
					Group: "workers", Consumer: "one", BlockMS: &tooLarge,
					Streams: []StreamRef{{Key: "events", ID: ">"}},
				})
				return err
			},
		},
		{
			name: "FLOW.CLAIM_DUE",
			call: func(client *Client) error {
				_, err := client.ClaimDue(context.Background(), ClaimDueOptions{
					Type: "job", Worker: "one", BlockMS: &tooLarge,
				})
				return err
			},
		},
		{
			name: "FLOW.SCHEDULE.FIRE_DUE",
			call: func(client *Client) error {
				_, err := client.ScheduleFireDueWithOptions(context.Background(), ScheduleFireDueOptions{
					BlockMS: &tooLarge,
				})
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []any{}}
			if err := test.call(NewClientWithExecutor(exec)); err == nil {
				t.Fatal("oversized blocking timeout was accepted")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("oversized blocking timeout reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestV080BlockingTimeoutProtocolMaximumRemainsSupported(t *testing.T) {
	t.Parallel()

	maximum := int64(0xFFFF_FFFF)
	if err := validateStreamRead("XREAD", nil, &maximum, []StreamRef{{Key: "events", ID: "0"}}); err != nil {
		t.Fatalf("XREAD maximum timeout rejected: %v", err)
	}
	if err := validateClaimDueOptions(ClaimDueOptions{
		Type: "job", Worker: "one", BlockMS: &maximum,
	}); err != nil {
		t.Fatalf("FLOW.CLAIM_DUE maximum timeout rejected: %v", err)
	}
	if err := validateScheduleFireDueOptions(ScheduleFireDueOptions{BlockMS: &maximum}); err != nil {
		t.Fatalf("FLOW.SCHEDULE.FIRE_DUE maximum timeout rejected: %v", err)
	}
}

func TestV080TopologyRoutingPreservesBlockingCommandBudget(t *testing.T) {
	endpoint := RoutingEndpoint{Host: "127.0.0.1", NativePort: 6388}
	topology := topologyForEndpoint(endpoint, 1)
	exec := &TopologyNativeExecutor{endpointPolicy: EndpointPolicyAny, topology: topology}
	data, err := exec.routeDataInSnapshot(
		[]any{"BLPOP", "queue", int64(60)},
		topologyRoutingSnapshot{topology: topology},
	)
	if err != nil {
		t.Fatal(err)
	}
	if data == nil || data.command.budget.extension != 60*time.Second {
		t.Fatalf("topology blocking budget = %#v; want 60s", data)
	}
}

func TestV080FlowBlockingCommandsExtendNativeRequestBudget(t *testing.T) {
	tests := []struct {
		name string
		args []any
	}{
		{name: "claim due", args: []any{"FLOW.CLAIM_DUE", "job", "BLOCK", int64(250)}},
		{name: "schedule fire due", args: []any{"FLOW.SCHEDULE.FIRE_DUE", "BLOCK", int64(250)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			budget := blockingCommandBudget(test.args)
			if budget.extension != 250*time.Millisecond || budget.disableDefault {
				t.Fatalf("blocking budget = %+v; want 250ms extension", budget)
			}
		})
	}
}

func TestV080ReclaimDoesNotClaimABlockingRequestBudget(t *testing.T) {
	// FerricStore 0.8.0's reclaim API forces the running-state scan directly;
	// unlike CLAIM_DUE, it does not enter the blocking waiter path.
	if budget := blockingCommandBudget([]any{"FLOW.RECLAIM", "job", "BLOCK", int64(250)}); budget != (nativeRequestBudget{}) {
		t.Fatalf("FLOW.RECLAIM blocking budget = %+v; want none", budget)
	}
}
