package ferricstore

import (
	"reflect"
	"strings"
	"testing"
)

func TestBudgetResultDecodesCanonicalServerFields(t *testing.T) {
	result, err := budgetResult(map[string]any{
		"scope": "tenant", "limit": int64(100), "window_ms": int64(60_000),
		"window_start_ms": int64(1_000), "used": int64(40), "remaining": int64(60),
		"over_budget": false, "reservations_count": int64(3),
		"reservation_id": "reservation-1", "reserved_amount": int64(50),
		"actual_amount": int64(40), "status": "committed",
		"usage": map[string]any{"tokens": int64(40)}, "overage_amount": int64(0),
		"reserved_at_ms": int64(1_100), "settled_at_ms": int64(1_200),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.WindowMS != 60_000 || result.WindowStartMS != 1_000 || result.OverBudget ||
		result.ReservationsCount != 3 || result.ReservedAmount != 50 || result.ActualAmount != 40 ||
		result.OverageAmount != 0 || result.ReservedAtMS != 1_100 || result.SettledAtMS != 1_200 ||
		!reflect.DeepEqual(result.Usage, map[string]any{"tokens": int64(40)}) {
		t.Fatalf("budget result = %#v", result)
	}
}

func TestLimitResultDecodesConfigurationAndReservationIDs(t *testing.T) {
	lease := map[string]any{
		"shard_id": int64(0), "epoch": int64(2), "expires_at_ms": int64(5_000),
		"available": int64(3), "in_use": int64(2), "pending_reclaim": int64(0),
		"drain_rate": 0.25, "last_spend_at_ms": int64(1_500),
	}
	result, err := limitResult(map[string]any{
		"owner": map[string]any{
			"scope": "tenant", "limit": int64(10), "free": int64(5), "epoch": int64(2),
			"config_version": int64(7), "policy_version_hash": "sha256-value",
			"leases": map[string]any{"0": lease},
		},
		"lease":           lease,
		"reservation_ids": []any{"flr1:2:batch:1", "flr1:2:batch:2"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.ConfigVersion != 7 || result.PolicyVersionHash != "sha256-value" ||
		!reflect.DeepEqual(result.ReservationIDs, []string{"flr1:2:batch:1", "flr1:2:batch:2"}) ||
		result.Lease == nil || result.Lease.ShardID != 0 || len(result.Leases) != 1 {
		t.Fatalf("limit result = %#v", result)
	}
}

func TestLimitResultDecodesHTTPIntegerLeaseMapKeys(t *testing.T) {
	lease := map[string]any{
		"shard_id": int64(0), "epoch": int64(2), "expires_at_ms": int64(5_000),
		"available": int64(3), "in_use": int64(2), "pending_reclaim": int64(0),
		"drain_rate": 0.25, "last_spend_at_ms": int64(1_500),
	}
	result, err := limitResult(map[string]any{
		"owner": map[string]any{
			"scope": "tenant", "limit": int64(10), "free": int64(5), "epoch": int64(2),
			"leases": map[any]any{int64(0): lease},
		},
		"lease": lease,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Leases) != 1 || result.Leases[0].ShardID != 0 {
		t.Fatalf("limit leases = %#v", result.Leases)
	}
}

func TestLimitCollectionsDecodeHTTPIntegerLeaseMapKeys(t *testing.T) {
	lease := map[string]any{"shard_id": int64(0)}
	limit := map[string]any{
		"scope":  "tenant",
		"leases": map[any]any{int64(0): lease},
	}

	listValue, err := normalizeLimitListLeaseKeys([]any{limit})
	if err != nil {
		t.Fatal(err)
	}
	maps, err := mapList(listValue, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := limitResultFromMap(maps[0])
	if err != nil || len(result.Leases) != 1 {
		t.Fatalf("limit list result=%#v error=%v", result, err)
	}

	overviewValue, err := normalizeGovernanceLimitLeaseKeys(map[string]any{"limits": []any{limit}})
	if err != nil {
		t.Fatal(err)
	}
	overviewMap, err := nativeMap(overviewValue)
	if err != nil {
		t.Fatal(err)
	}
	overview, err := governanceOverviewFromMap(overviewMap)
	if err != nil || len(overview.Limits) != 1 || len(overview.Limits[0].Leases) != 1 {
		t.Fatalf("governance overview=%#v error=%v", overview, err)
	}
}

func TestBudgetAndLimitResultsRejectIntegrityViolations(t *testing.T) {
	tests := []struct {
		name string
		want string
		run  func() error
	}{
		{name: "negative budget used", want: "used", run: func() error {
			_, err := budgetResult(map[string]any{"used": int64(-1)}, nil)
			return err
		}},
		{name: "budget exact integer overflow", want: "used", run: func() error {
			_, err := budgetResult(map[string]any{"used": int64(maxFlowExactIntegerV080 + 1)}, nil)
			return err
		}},
		{name: "inconsistent budget remaining", want: "remaining", run: func() error {
			_, err := budgetResult(map[string]any{"limit": int64(10), "used": int64(4), "remaining": int64(9)}, nil)
			return err
		}},
		{name: "inconsistent over budget", want: "over_budget", run: func() error {
			_, err := budgetResult(map[string]any{"limit": int64(10), "used": int64(11), "over_budget": false}, nil)
			return err
		}},
		{name: "negative limit free", want: "free", run: func() error {
			_, err := limitResult(map[string]any{"free": int64(-1)}, nil)
			return err
		}},
		{name: "limit exact integer overflow", want: "epoch", run: func() error {
			_, err := limitResult(map[string]any{"epoch": int64(maxFlowExactIntegerV080 + 1)}, nil)
			return err
		}},
		{name: "lease shard mismatch", want: "shard_id", run: func() error {
			_, err := limitResult(map[string]any{"leases": map[string]any{"2": map[string]any{"shard_id": int64(1)}}}, nil)
			return err
		}},
		{name: "duplicate reservation ids", want: "reservation_ids", run: func() error {
			_, err := limitResult(map[string]any{"reservation_ids": []any{"same", "same"}}, nil)
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want text %q", err, test.want)
			}
		})
	}
}
