package ferricstore

import (
	"context"
	"testing"
)

func TestFlowMutationsRejectInvalidArgumentsBeforeCodecOrTransport(t *testing.T) {
	negative := int64(-1)
	zero := int64(0)
	priority := int64(3)
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "create missing id", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{Type: "order", Payload: "payload"})
			return err
		}},
		{name: "create missing type", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{ID: "flow", Payload: "payload"})
			return err
		}},
		{name: "create negative now", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{ID: "flow", Type: "order", NowMS: -1, Payload: "payload"})
			return err
		}},
		{name: "create negative run at", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{ID: "flow", Type: "order", RunAtMS: -1, Payload: "payload"})
			return err
		}},
		{name: "create invalid priority", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{ID: "flow", Type: "order", Priority: &priority, Payload: "payload"})
			return err
		}},
		{name: "create invalid retention", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{ID: "flow", Type: "order", RetentionTTLMS: &zero, Payload: "payload"})
			return err
		}},
		{name: "create empty value name", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{
				ID: "flow", Type: "order", Values: map[string]any{"": "payload"},
			})
			return err
		}},
		{name: "create empty value ref", call: func(client *Client) error {
			_, err := client.Create(context.Background(), CreateOptions{
				ID: "flow", Type: "order", ValueRefs: map[string]string{"result": ""},
			})
			return err
		}},
		{name: "value put negative now", call: func(client *Client) error {
			_, err := client.ValuePut(context.Background(), "payload", ValuePutOptions{NowMS: -1})
			return err
		}},
		{name: "value put invalid ttl", call: func(client *Client) error {
			_, err := client.ValuePut(context.Background(), "payload", ValuePutOptions{TTLMS: &zero})
			return err
		}},
		{name: "named value put with ttl", call: func(client *Client) error {
			ttl := int64(1)
			_, err := client.ValuePut(context.Background(), "payload", ValuePutOptions{
				OwnerFlowID: "flow", Name: "result", TTLMS: &ttl,
			})
			return err
		}},
		{name: "named value put without owner", call: func(client *Client) error {
			_, err := client.ValuePut(context.Background(), "payload", ValuePutOptions{Name: "result"})
			return err
		}},
		{name: "shared value put with override", call: func(client *Client) error {
			override := true
			_, err := client.ValuePut(context.Background(), "payload", ValuePutOptions{Override: &override})
			return err
		}},
		{name: "put value without name", call: func(client *Client) error {
			_, err := client.PutValue(context.Background(), "", "payload", ValuePutOptions{OwnerFlowID: "flow"})
			return err
		}},
		{name: "value mget empty ref", call: func(client *Client) error {
			_, err := client.ValueMGet(context.Background(), []string{""}, nil)
			return err
		}},
		{name: "value mget negative maximum", call: func(client *Client) error {
			_, err := client.ValueMGet(context.Background(), []string{"ref"}, &negative)
			return err
		}},
		{name: "signal missing id", call: func(client *Client) error {
			_, err := client.Signal(context.Background(), SignalOptions{Signal: "approve", NamedValues: NamedValues{Values: map[string]any{"v": "payload"}}})
			return err
		}},
		{name: "signal missing signal", call: func(client *Client) error {
			_, err := client.Signal(context.Background(), SignalOptions{ID: "flow", NamedValues: NamedValues{Values: map[string]any{"v": "payload"}}})
			return err
		}},
		{name: "signal empty state", call: func(client *Client) error {
			_, err := client.Signal(context.Background(), SignalOptions{ID: "flow", Signal: "approve", IfStates: []string{""}, NamedValues: NamedValues{Values: map[string]any{"v": "payload"}}})
			return err
		}},
		{name: "signal transition to running", call: func(client *Client) error {
			_, err := client.Signal(context.Background(), SignalOptions{ID: "flow", Signal: "approve", TransitionTo: "running", NamedValues: NamedValues{Values: map[string]any{"v": "payload"}}})
			return err
		}},
		{name: "start missing id", call: func(client *Client) error {
			_, err := client.StartAndClaim(context.Background(), StartAndClaimOptions{Type: "order", InitialState: "queued", Worker: "worker", Payload: "payload"})
			return err
		}},
		{name: "start missing type", call: func(client *Client) error {
			_, err := client.StartAndClaim(context.Background(), StartAndClaimOptions{ID: "flow", InitialState: "queued", Worker: "worker", Payload: "payload"})
			return err
		}},
		{name: "start running state", call: func(client *Client) error {
			_, err := client.StartAndClaim(context.Background(), StartAndClaimOptions{ID: "flow", Type: "order", InitialState: "running", Worker: "worker", Payload: "payload"})
			return err
		}},
		{name: "start missing worker", call: func(client *Client) error {
			_, err := client.StartAndClaim(context.Background(), StartAndClaimOptions{ID: "flow", Type: "order", InitialState: "queued", Payload: "payload"})
			return err
		}},
		{name: "start negative lease", call: func(client *Client) error {
			_, err := client.StartAndClaim(context.Background(), StartAndClaimOptions{ID: "flow", Type: "order", InitialState: "queued", Worker: "worker", LeaseMS: -1, Payload: "payload"})
			return err
		}},
		{name: "start empty value name", call: func(client *Client) error {
			_, err := client.StartAndClaim(context.Background(), StartAndClaimOptions{
				ID: "flow", Type: "order", InitialState: "queued", Worker: "worker",
				Values: map[string]any{"": "payload"},
			})
			return err
		}},
		{name: "start empty value ref", call: func(client *Client) error {
			_, err := client.StartAndClaim(context.Background(), StartAndClaimOptions{
				ID: "flow", Type: "order", InitialState: "queued", Worker: "worker",
				ValueRefs: map[string]string{"result": ""},
			})
			return err
		}},
		{name: "extend missing id", call: func(client *Client) error {
			_, err := client.ExtendLease(context.Background(), "", "lease", 1, 1000, "")
			return err
		}},
		{name: "extend missing lease", call: func(client *Client) error {
			_, err := client.ExtendLease(context.Background(), "flow", "", 1, 1000, "")
			return err
		}},
		{name: "extend negative fencing", call: func(client *Client) error {
			_, err := client.ExtendLease(context.Background(), "flow", "lease", -1, 1000, "")
			return err
		}},
		{name: "extend invalid lease", call: func(client *Client) error {
			_, err := client.ExtendLease(context.Background(), "flow", "lease", 1, 0, "")
			return err
		}},
		{name: "transition missing from", call: func(client *Client) error {
			_, err := client.Transition(context.Background(), TransitionOptions{ID: "flow", ToState: "ready", Payload: "payload"})
			return err
		}},
		{name: "transition missing to", call: func(client *Client) error {
			_, err := client.Transition(context.Background(), TransitionOptions{ID: "flow", FromState: "queued", Payload: "payload"})
			return err
		}},
		{name: "transition to running", call: func(client *Client) error {
			_, err := client.Transition(context.Background(), TransitionOptions{ID: "flow", FromState: "queued", ToState: "running", Payload: "payload"})
			return err
		}},
		{name: "transition negative fencing", call: func(client *Client) error {
			_, err := client.Transition(context.Background(), TransitionOptions{ID: "flow", FromState: "queued", ToState: "ready", FencingToken: -1, Payload: "payload"})
			return err
		}},
		{name: "transition missing lease", call: func(client *Client) error {
			_, err := client.Transition(context.Background(), TransitionOptions{ID: "flow", FromState: "queued", ToState: "ready", Payload: "payload"})
			return err
		}},
		{name: "step missing lease", call: func(client *Client) error {
			_, err := client.StepContinue(context.Background(), StepContinueOptions{ID: "flow", FromState: "a", ToState: "b", Payload: "payload"})
			return err
		}},
		{name: "step to running", call: func(client *Client) error {
			_, err := client.StepContinue(context.Background(), StepContinueOptions{ID: "flow", LeaseToken: "lease", FromState: "a", ToState: "running", Payload: "payload"})
			return err
		}},
		{name: "step negative lease", call: func(client *Client) error {
			_, err := client.StepContinue(context.Background(), StepContinueOptions{ID: "flow", LeaseToken: "lease", FromState: "a", ToState: "b", LeaseMS: -1, Payload: "payload"})
			return err
		}},
		{name: "complete missing lease", call: func(client *Client) error {
			_, err := client.Complete(context.Background(), CompleteOptions{ID: "flow", Result: "payload"})
			return err
		}},
		{name: "complete negative fencing", call: func(client *Client) error {
			_, err := client.Complete(context.Background(), CompleteOptions{ID: "flow", LeaseToken: "lease", FencingToken: -1, Result: "payload"})
			return err
		}},
		{name: "complete invalid ttl", call: func(client *Client) error {
			_, err := client.Complete(context.Background(), CompleteOptions{ID: "flow", LeaseToken: "lease", TTLMS: &zero, Result: "payload"})
			return err
		}},
		{name: "retry missing lease", call: func(client *Client) error {
			_, err := client.Retry(context.Background(), RetryOptions{ID: "flow", Error: "payload"})
			return err
		}},
		{name: "fail missing id", call: func(client *Client) error {
			_, err := client.Fail(context.Background(), FailOptions{LeaseToken: "lease", Error: "payload"})
			return err
		}},
		{name: "cancel negative fencing", call: func(client *Client) error {
			_, err := client.Cancel(context.Background(), CancelOptions{ID: "flow", FencingToken: -1, Reason: "payload"})
			return err
		}},
		{name: "rewind missing event", call: func(client *Client) error {
			_, err := client.Rewind(context.Background(), RewindOptions{ID: "flow"})
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			codec := &countingKVCodec{}
			client := NewClientWithExecutor(exec, WithCodec(codec))
			if err := test.call(client); err == nil {
				t.Fatal("invalid mutation was accepted")
			}
			if calls := codec.encodes.Load(); calls != 0 {
				t.Fatalf("invalid mutation invoked codec %d times", calls)
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid mutation reached transport: %#v", exec.calls)
			}
		})
	}
}
