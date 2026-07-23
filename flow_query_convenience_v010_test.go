package ferricstore

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestV010ListUsesBoundedFQLAndDefaultQueuedState(t *testing.T) {
	exec := &fakeExecutor{value: flowQueryPageResponse([]any{
		map[string]any{"id": "run-1", "type": "order", "state": "queued", "partition_key": "tenant-a"},
	}, false, nil)}
	client := NewClientWithExecutor(exec)

	records, err := client.List(context.Background(), "order", ReadOptions{PartitionKey: "tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].ID != "run-1" {
		t.Fatalf("records = %#v", records)
	}
	query := "FROM runs WHERE partition_key = @partition_key AND type = @type AND state = @state ORDER BY updated_at_ms ASC LIMIT 100 RETURN RECORDS"
	want := [][]any{{
		"FLOW.QUERY", "FQL1", query,
		"partition_key", "tenant-a", "state", "queued", "type", "order",
	}}
	if !reflect.DeepEqual(exec.calls, want) {
		t.Fatalf("calls = %#v, want %#v", exec.calls, want)
	}
}

func TestV010SearchEscapesMetadataSelectorsAndUsesTypedParameters(t *testing.T) {
	exec := &fakeExecutor{value: flowQueryPageResponse([]any{}, false, nil)}
	client := NewClientWithExecutor(exec)
	reverse := true

	_, err := client.Search(context.Background(), SearchOptions{
		Type: "order", State: "completed", PartitionKey: "tenant-a", Count: Int(10),
		FromMS: Int64(100), ToMS: Int64(200), Rev: &reverse,
		Attributes: map[string]any{"customer.region": "eu"},
		StateMeta:  map[string]map[string]any{"review.v2": {"ai'model": int64(3)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	query := "FROM runs WHERE partition_key = @partition_key AND type = @type AND state = @state AND attribute['customer.region'] = @attribute_0 AND state_meta['review.v2']['ai''model'] = @state_meta_0 AND updated_at_ms BETWEEN @from_ms AND @to_ms ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS"
	want := [][]any{{
		"FLOW.QUERY", "FQL1", query,
		"attribute_0", "eu", "from_ms", int64(100), "partition_key", "tenant-a",
		"state", "completed", "state_meta_0", int64(3), "to_ms", int64(200), "type", "order",
	}}
	if !reflect.DeepEqual(exec.calls, want) {
		t.Fatalf("calls = %#v, want %#v", exec.calls, want)
	}
}

func TestV010ConvenienceQueriesRequirePartitionAndRejectUnrepresentableOptions(t *testing.T) {
	includeCold := true
	consistent := true
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "list partition", call: func(client *Client) error {
			_, err := client.List(context.Background(), "order", ReadOptions{})
			return err
		}},
		{name: "search metadata", call: func(client *Client) error {
			_, err := client.Search(context.Background(), SearchOptions{PartitionKey: "tenant-a", Type: "order"})
			return err
		}},
		{name: "include cold", call: func(client *Client) error {
			_, err := client.List(context.Background(), "order", ReadOptions{PartitionKey: "tenant-a", IncludeCold: &includeCold})
			return err
		}},
		{name: "consistent projection", call: func(client *Client) error {
			_, err := client.List(context.Background(), "order", ReadOptions{PartitionKey: "tenant-a", ConsistentProjection: &consistent})
			return err
		}},
		{name: "limit", call: func(client *Client) error {
			_, err := client.List(context.Background(), "order", ReadOptions{PartitionKey: "tenant-a", Count: Int(101)})
			return err
		}},
		{name: "stuck concrete type", call: func(client *Client) error {
			_, err := client.Stuck(context.Background(), "any", "tenant-a", Int(5), Int64(100), Int64(1_000))
			return err
		}},
		{name: "list requires a bounded source", call: func(client *Client) error {
			_, err := client.List(context.Background(), "any", ReadOptions{PartitionKey: "tenant-a"})
			return err
		}},
		{name: "list any state requires metadata", call: func(client *Client) error {
			_, err := client.List(context.Background(), "order", ReadOptions{
				PartitionKey: "tenant-a", State: "any",
			})
			return err
		}},
		{name: "state metadata requires a concrete type", call: func(client *Client) error {
			_, err := client.Search(context.Background(), SearchOptions{
				Type: "any", PartitionKey: "tenant-a",
				StateMeta: map[string]map[string]any{"queued": {"risk": int64(3)}},
			})
			return err
		}},
		{name: "terminals require a concrete type", call: func(client *Client) error {
			_, err := client.Terminals(context.Background(), "any", ReadOptions{PartitionKey: "tenant-a"})
			return err
		}},
		{name: "terminals reject ignored attributes", call: func(client *Client) error {
			_, err := client.Terminals(context.Background(), "order", ReadOptions{
				PartitionKey: "tenant-a", Attributes: map[string]any{"tenant": "acme"},
			})
			return err
		}},
		{name: "failures require a bounded source", call: func(client *Client) error {
			_, err := client.Failures(context.Background(), "any", ReadOptions{PartitionKey: "tenant-a"})
			return err
		}},
		{name: "lineage rejects metadata", call: func(client *Client) error {
			_, err := client.ByParent(context.Background(), "parent-1", ReadOptions{
				PartitionKey: "tenant-a", Attributes: map[string]any{"tenant": "acme"},
			})
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{}
			if err := test.call(NewClientWithExecutor(exec)); err == nil {
				t.Fatal("expected validation error")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid query performed IO: %#v", exec.calls)
			}
		})
	}
}

func TestV010LineageAndStuckConveniencesUseFQL(t *testing.T) {
	exec := &fakeExecutor{values: []any{
		flowQueryPageResponse([]any{}, false, nil),
		flowQueryPageResponse([]any{}, false, nil),
	}}
	client := NewClientWithExecutor(exec)

	if _, err := client.ByParent(context.Background(), "parent-1", ReadOptions{PartitionKey: "tenant-a", Count: Int(5)}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Stuck(context.Background(), "order", "tenant-a", Int(7), Int64(250), Int64(1000)); err != nil {
		t.Fatal(err)
	}
	lineageQuery := "FROM runs WHERE partition_key = @partition_key AND parent_flow_id = @lineage_id ORDER BY updated_at_ms ASC LIMIT 5 RETURN RECORDS"
	stuckQuery := "FROM runs WHERE partition_key = @partition_key AND type = @type AND state = @state AND lease_deadline_ms BETWEEN @lease_from_ms AND @lease_to_ms ORDER BY lease_deadline_ms ASC LIMIT 7 RETURN RECORDS"
	if exec.calls[0][2] != lineageQuery || exec.calls[1][2] != stuckQuery {
		t.Fatalf("queries = %q / %q", exec.calls[0][2], exec.calls[1][2])
	}
	if strings.Contains(strings.Join([]string{asString(exec.calls[0][0]), asString(exec.calls[1][0])}, " "), "FLOW.BY_") {
		t.Fatalf("removed command emitted: %#v", exec.calls)
	}
	wantStuckParams := []any{
		"lease_from_ms", int64(0), "lease_to_ms", int64(750), "partition_key", "tenant-a",
		"state", "running", "type", "order",
	}
	if !reflect.DeepEqual(exec.calls[1][3:], wantStuckParams) {
		t.Fatalf("stuck params = %#v, want %#v", exec.calls[1][3:], wantStuckParams)
	}
}
