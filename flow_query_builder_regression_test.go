package ferricstore

import (
	"reflect"
	"testing"
)

func TestFlowSearchBuilderCanonicalizesAndSortsMetadataWithoutChangingValues(t *testing.T) {
	query, params, err := buildFlowSearchQuery(SearchOptions{
		Type: "invoice", PartitionKey: "tenant-a", Count: Int(10),
		Attributes: map[string]any{" z ": "last", "a": int64(1)},
		StateMeta: map[string]map[string]any{
			"review": {"b": true},
			"queued": {" c ": int64(3), "a": "first"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	wantQuery := "FROM runs WHERE partition_key = @partition_key AND type = @type" +
		" AND attribute['a'] = @attribute_0 AND attribute['z'] = @attribute_1" +
		" AND state_meta['queued']['a'] = @state_meta_0" +
		" AND state_meta['queued']['c'] = @state_meta_1" +
		" AND state_meta['review']['b'] = @state_meta_2" +
		" ORDER BY updated_at_ms ASC LIMIT 10 RETURN RECORDS"
	if query != wantQuery {
		t.Fatalf("query = %q, want %q", query, wantQuery)
	}
	wantParams := map[string]any{
		"partition_key": "tenant-a", "type": "invoice",
		"attribute_0": int64(1), "attribute_1": "last",
		"state_meta_0": "first", "state_meta_1": int64(3), "state_meta_2": true,
	}
	if !reflect.DeepEqual(params, wantParams) {
		t.Fatalf("params = %#v, want %#v", params, wantParams)
	}
}
