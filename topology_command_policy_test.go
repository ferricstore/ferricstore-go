package ferricstore

import "testing"

func TestTopologyCommandPoliciesAreInternallyConsistent(t *testing.T) {
	if len(topologyCommandPolicies) == 0 {
		t.Fatal("topology command policy registry is empty")
	}
	for name, policy := range topologyCommandPolicies {
		if policy.keyMode == topologyKeysNone {
			t.Errorf("%s has no key extraction mode", name)
		}
		if policy.scatter && policy.requireSameSlot {
			t.Errorf("%s cannot be both scatterable and same-slot-only", name)
		}
		if policy.destructive && !policy.scatter {
			t.Errorf("%s is destructive but not scatterable", name)
		}
	}

	tests := []struct {
		name        string
		args        []any
		wantKeys    int
		wantScatter bool
		wantSame    bool
	}{
		{name: "first key", args: []any{"GET", "key"}, wantKeys: 1},
		{name: "native key info alias", args: []any{"KEY_INFO", "key"}, wantKeys: 1},
		{name: "scatter", args: []any{"MGET", "one", "two"}, wantKeys: 2, wantScatter: true},
		{name: "pairs", args: []any{"MSET", "one", "1", "two", "2"}, wantKeys: 2, wantSame: true},
		{name: "first two", args: []any{"RENAME", "one", "two"}, wantKeys: 2, wantSame: true},
		{name: "counted read", args: []any{"SINTERCARD", 2, "one", "two"}, wantKeys: 2, wantSame: true},
		{name: "counted store", args: []any{"CMS.MERGE", "destination", 2, "one", "two"}, wantKeys: 3, wantSame: true},
		{name: "blocking sorted-set pop", args: []any{"BZPOPMIN", "one", "two", 1.5}, wantKeys: 2, wantSame: true},
		{name: "streams", args: []any{"XREAD", "STREAMS", "one", "two", "0", "0"}, wantKeys: 2, wantSame: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			name := commandName(tc.args)
			keys, policy, ok := topologyPolicyKeys(name, tc.args)
			if !ok {
				t.Fatalf("no policy keys for %s", name)
			}
			if len(keys) != tc.wantKeys {
				t.Fatalf("%s key count = %d; want %d", name, len(keys), tc.wantKeys)
			}
			if policy.scatter != tc.wantScatter || policy.requireSameSlot != tc.wantSame {
				t.Fatalf("%s policy = %#v; want scatter=%t same-slot=%t", name, policy, tc.wantScatter, tc.wantSame)
			}
		})
	}
}

func TestTopologyScatterAndSameSlotUseCommandPolicy(t *testing.T) {
	for _, args := range [][]any{
		{"MGET", "one", "two"},
		{"DEL", "one", "two"},
		{"EXISTS", "one", "two"},
		{"MSET", "one", "1", "two", "2"},
		{"GEOSEARCHSTORE", "one", "two", "FROMMEMBER", "member"},
	} {
		name := commandName(args)
		keys, policy, ok := topologyPolicyKeys(name, args)
		if !ok {
			t.Fatalf("missing command policy for %s", name)
		}
		_, scatterKeys, scatter := safeScatterCommand(args)
		if scatter != policy.scatter {
			t.Fatalf("%s scatter = %t; policy says %t", name, scatter, policy.scatter)
		}
		if scatter && len(scatterKeys) != len(keys) {
			t.Fatalf("%s scatter keys = %d; policy keys = %d", name, len(scatterKeys), len(keys))
		}
		sameKeys, same := sameSlotCommandKeys(args)
		if same != policy.requireSameSlot {
			t.Fatalf("%s same-slot = %t; policy says %t", name, same, policy.requireSameSlot)
		}
		if same && len(sameKeys) != len(keys) {
			t.Fatalf("%s same-slot keys = %d; policy keys = %d", name, len(sameKeys), len(keys))
		}
	}
}

func TestTopologyStreamKeysIgnoreOptionValuesNamedStreams(t *testing.T) {
	tests := []struct {
		name string
		args []any
	}{
		{
			name: "group name",
			args: []any{"XREADGROUP", "GROUP", "STREAMS", "consumer", "COUNT", 1, "STREAMS", "orders", ">"},
		},
		{
			name: "consumer name",
			args: []any{"XREADGROUP", "GROUP", "workers", "STREAMS", "BLOCK", 10, "STREAMS", "orders", ">"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			keys, policy, ok := topologyPolicyKeys(commandName(test.args), test.args)
			if !ok {
				t.Fatal("stream command was not recognized")
			}
			if !policy.requireSameSlot || len(keys) != 1 || asString(keys[0]) != "orders" {
				t.Fatalf("stream keys = %#v, policy = %#v; want [orders] on one slot", keys, policy)
			}
		})
	}
}
