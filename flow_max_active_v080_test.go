package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestV080FlowMutationsCarryMaxActiveMS(t *testing.T) {
	tests := []struct {
		name     string
		response any
		call     func(*Client) error
	}{
		{
			name: "create integer",
			call: func(client *Client) error {
				_, err := client.Create(context.Background(), CreateOptions{
					ID: "flow-1", Type: "order", MaxActiveMS: int64(500),
				})
				return err
			},
		},
		{
			name:     "start and claim infinity",
			response: map[string]any{"id": "flow-1"},
			call: func(client *Client) error {
				_, err := client.StartAndClaim(context.Background(), StartAndClaimOptions{
					ID: "flow-1", Type: "order", InitialState: "queued", Worker: "worker-1",
					MaxActiveMS: FlowMaxActiveInfinity,
				})
				return err
			},
		},
		{
			name: "type policy",
			call: func(client *Client) error {
				_, err := client.SetPolicy(context.Background(), "order", PolicyOptions{
					MaxActiveMS: int64(900),
				})
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := test.response
			if response == nil {
				response = []byte("OK")
			}
			exec := &fakeExecutor{value: response}
			if err := test.call(NewClientWithExecutor(exec)); err != nil {
				t.Fatal(err)
			}
			if len(exec.calls) != 1 || !commandContainsPair(exec.calls[0], "MAX_ACTIVE_MS") {
				t.Fatalf("command = %#v; want MAX_ACTIVE_MS", exec.calls)
			}
		})
	}
}

func TestV080CreateManyAndChildrenCarryPerItemMaxActiveMS(t *testing.T) {
	createExec := &fakeExecutor{value: []byte("OK")}
	createClient := NewClientWithExecutor(createExec)
	if _, err := createClient.CreateMany(context.Background(), CreateManyOptions{
		Type:  "order",
		Items: []CreateItem{{ID: "child-1", MaxActiveMS: int64(500)}},
	}); err != nil {
		t.Fatal(err)
	}
	createCommand, err := buildNativeCommand(createExec.calls[0])
	if err != nil {
		t.Fatal(err)
	}
	createPayload := createCommand.payload.(map[string]any)
	items := createPayload["items"].([]any)
	if got := asInt64(items[0].(map[string]any)["max_active_ms"]); got != 500 {
		t.Fatalf("create-many payload = %#v", createPayload)
	}

	spawnExec := &fakeExecutor{value: []byte("OK")}
	spawnClient := NewClientWithExecutor(spawnExec)
	if _, err := spawnClient.SpawnChildren(context.Background(), SpawnChildrenOptions{
		ID: "parent-1", PartitionKey: "tenant-1", FencingToken: Int64(1),
		GroupID: "group-1", Wait: "none", Success: "completed", Failure: "failed",
		Children: []ChildSpec{{ID: "child-1", Type: "order", MaxActiveMS: FlowMaxActiveInfinity}},
	}); err != nil {
		t.Fatal(err)
	}
	spawnCommand, err := buildNativeCommand(spawnExec.calls[0])
	if err != nil {
		t.Fatal(err)
	}
	spawnPayload := spawnCommand.payload.(map[string]any)
	children := spawnPayload["children"].([]any)
	if got := asString(children[0].(map[string]any)["max_active_ms"]); got != FlowMaxActiveInfinity {
		t.Fatalf("spawn payload = %#v", spawnPayload)
	}
}

func TestV080CreateManyKeepsPerItemMetadataWithMaxActiveMS(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)
	items := []CreateItem{
		{
			ID: "flow-1", MaxActiveMS: int64(500),
			Attributes: map[string]any{"tenant": "one"},
			StateMeta:  map[string]any{"step": int64(1)},
		},
		{
			ID: "flow-2", MaxActiveMS: FlowMaxActiveInfinity,
			Attributes: map[string]any{"tenant": "two"},
			StateMeta:  map[string]any{"step": int64(2)},
		},
	}
	if _, err := client.CreateMany(context.Background(), CreateManyOptions{
		Type: "order", Items: items,
	}); err != nil {
		t.Fatal(err)
	}

	command, err := buildNativeCommand(exec.calls[0])
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpFlowCreateMany {
		t.Fatalf("CREATE_MANY opcode = %#x, want %#x", command.opcode, nativeOpFlowCreateMany)
	}
	payload := command.payload.(map[string]any)
	mapped := payload["items"].([]any)
	for index, item := range items {
		got := mapped[index].(map[string]any)
		if !reflect.DeepEqual(got["attributes"], item.Attributes) {
			t.Errorf("item %d attributes = %#v, want %#v", index, got["attributes"], item.Attributes)
		}
		if !reflect.DeepEqual(got["state_meta"], item.StateMeta) {
			t.Errorf("item %d state_meta = %#v, want %#v", index, got["state_meta"], item.StateMeta)
		}
	}
}

func TestV080SpawnChildrenKeepsPerChildMetadataWithMaxActiveMS(t *testing.T) {
	fencing := int64(7)
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)
	children := []ChildSpec{
		{
			ID: "child-1", Type: "order", MaxActiveMS: int64(500),
			Attributes: map[string]any{"tenant": "one"},
			StateMeta:  map[string]any{"step": int64(1)},
		},
		{
			ID: "child-2", Type: "order", MaxActiveMS: FlowMaxActiveInfinity,
			Attributes: map[string]any{"tenant": "two"},
			StateMeta:  map[string]any{"step": int64(2)},
		},
	}
	if _, err := client.SpawnChildren(context.Background(), SpawnChildrenOptions{
		ID: "parent-1", PartitionKey: "tenant", FencingToken: &fencing,
		GroupID: "group-1", Wait: "none", Success: "completed", Failure: "failed",
		Children: children,
	}); err != nil {
		t.Fatal(err)
	}

	command, err := buildNativeCommand(exec.calls[0])
	if err != nil {
		t.Fatal(err)
	}
	payload := command.payload.(map[string]any)
	mapped := payload["children"].([]any)
	for index, child := range children {
		got := mapped[index].(map[string]any)
		if !reflect.DeepEqual(got["attributes"], child.Attributes) {
			t.Errorf("child %d attributes = %#v, want %#v", index, got["attributes"], child.Attributes)
		}
		if !reflect.DeepEqual(got["state_meta"], child.StateMeta) {
			t.Errorf("child %d state_meta = %#v, want %#v", index, got["state_meta"], child.StateMeta)
		}
	}
}

func TestV080MaxActiveMSValidationIsLocal(t *testing.T) {
	for _, invalid := range []any{int64(0), int64(31_536_000_001), "forever", " infinity ", 1.5} {
		exec := &fakeExecutor{value: []byte("OK")}
		client := NewClientWithExecutor(exec)
		if _, err := client.Create(context.Background(), CreateOptions{
			ID: "flow-1", Type: "order", MaxActiveMS: invalid,
		}); err == nil {
			t.Fatalf("accepted max_active_ms %#v", invalid)
		}
		if len(exec.calls) != 0 {
			t.Fatalf("invalid max_active_ms reached transport: %#v", exec.calls)
		}
	}
}

func TestV080MaxActiveMSAcceptsNamedInfinityString(t *testing.T) {
	type maxActiveLimit string

	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)
	if _, err := client.Create(context.Background(), CreateOptions{
		ID: "flow-1", Type: "order", MaxActiveMS: maxActiveLimit("infinity"),
	}); err != nil {
		t.Fatal(err)
	}
	if len(exec.calls) != 1 || !commandContainsPair(exec.calls[0], "MAX_ACTIVE_MS") {
		t.Fatalf("command = %#v; want named infinity MAX_ACTIVE_MS", exec.calls)
	}
}

func TestV080PolicyInfinityRejectsStructuredResponseThatOmitsMaxActiveMS(t *testing.T) {
	exec := &fakeExecutor{value: map[string]any{"type": "order"}}
	_, err := NewClientWithExecutor(exec).SetPolicy(context.Background(), "order", PolicyOptions{
		MaxActiveMS: FlowMaxActiveInfinity,
	})
	if err == nil {
		t.Fatal("accepted a structured policy response that omitted max_active_ms")
	}
}

func TestV080PolicyMaxActiveAcknowledgementMatchesRequestedValue(t *testing.T) {
	for _, test := range []struct {
		name     string
		actual   any
		expected any
		wantErr  bool
	}{
		{name: "integer", actual: int64(500), expected: int64(500)},
		{name: "infinity nil", actual: nil, expected: FlowMaxActiveInfinity},
		{name: "infinity text", actual: "INFINITY", expected: FlowMaxActiveInfinity},
		{name: "integer mismatch", actual: int64(501), expected: int64(500), wantErr: true},
		{name: "infinity mismatch", actual: int64(500), expected: FlowMaxActiveInfinity, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateAppliedMaxActiveMS(test.actual, test.expected)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateAppliedMaxActiveMS(%#v, %#v) error = %v", test.actual, test.expected, err)
			}
		})
	}
}

func commandContainsPair(args []any, token string) bool {
	for index := 0; index+1 < len(args); index++ {
		if commandPart(args[index]) == token {
			return true
		}
	}
	return false
}
