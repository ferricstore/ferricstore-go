package ferricstore

import (
	"strings"
	"testing"
)

func TestProjectFlowQueryBuildsSourceAwareEscapedProjections(t *testing.T) {
	attribute, err := FlowAttributeProjection("customer.tier")
	if err != nil {
		t.Fatal(err)
	}
	stateMeta, err := FlowStateMetaProjection("review's", "risk tier")
	if err != nil {
		t.Fatal(err)
	}
	query, err := ProjectFlowQuery(
		"FROM runs WHERE run_id = @id",
		FlowProjectionRecord,
		FlowRunID,
		FlowRunState,
		attribute,
		stateMeta,
	)
	if err != nil {
		t.Fatal(err)
	}
	want := "FROM runs WHERE run_id = @id RETURN RECORD " +
		"(run_id, state, attribute['customer.tier'], state_meta['review''s']['risk tier'])"
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}

	eventField, err := FlowEventFieldProjection("worker's.pool")
	if err != nil {
		t.Fatal(err)
	}
	query, err = ProjectFlowQuery(
		" FROM events WHERE run_id = @id ORDER BY event_id ASC LIMIT 20; ",
		FlowProjectionRecords,
		FlowEventID,
		eventField,
	)
	if err != nil {
		t.Fatal(err)
	}
	want = "FROM events WHERE run_id = @id ORDER BY event_id ASC LIMIT 20 " +
		"RETURN RECORDS (event_id, fields['worker''s.pool'])"
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

func TestProjectFlowQueryRejectsInvalidShapesAndFields(t *testing.T) {
	for name, build := range map[string]func() error{
		"empty": func() error {
			_, err := ProjectFlowQuery("FROM runs WHERE run_id = @id", FlowProjectionRecords)
			return err
		},
		"wrong source": func() error {
			_, err := ProjectFlowQuery(
				"FROM runs WHERE run_id = @id", FlowProjectionRecord, FlowEventID,
			)
			return err
		},
		"duplicate": func() error {
			_, err := ProjectFlowQuery(
				"FROM runs WHERE run_id = @id", FlowProjectionRecord, FlowRunState, FlowRunState,
			)
			return err
		},
		"invalid cast": func() error {
			_, err := ProjectFlowQuery(
				"FROM runs WHERE run_id = @id",
				FlowProjectionRecord,
				FlowRunProjectionField("internal_secret"),
			)
			return err
		},
		"forged dynamic cast": func() error {
			_, err := ProjectFlowQuery(
				"FROM runs WHERE run_id = @id",
				FlowProjectionRecord,
				FlowRunProjectionField("attribute['injected']"),
			)
			return err
		},
		"existing return": func() error {
			_, err := ProjectFlowQuery(
				"FROM runs WHERE type = 'RETURN' RETURN RECORD",
				FlowProjectionRecord,
				FlowRunID,
			)
			return err
		},
		"multiple terminators": func() error {
			_, err := ProjectFlowQuery(
				"FROM runs WHERE run_id = @id;;",
				FlowProjectionRecord,
				FlowRunID,
			)
			return err
		},
		"spaced multiple terminators": func() error {
			_, err := ProjectFlowQuery(
				"FROM runs WHERE run_id = @id; ;",
				FlowProjectionRecord,
				FlowRunID,
			)
			return err
		},
		"non grammar leading whitespace": func() error {
			_, err := ProjectFlowQuery(
				"\u00a0FROM runs WHERE run_id = @id",
				FlowProjectionRecord,
				FlowRunID,
			)
			return err
		},
		"unicode source suffix": func() error {
			_, err := ProjectFlowQuery(
				"FROM runs\u00e9 WHERE run_id = @id",
				FlowProjectionRecord,
				FlowRunID,
			)
			return err
		},
		"unicode source confusable": func() error {
			_, err := ProjectFlowQuery(
				"FROM run\u017f WHERE run_id = @id",
				FlowProjectionRecord,
				FlowRunID,
			)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := build(); err == nil {
				t.Fatal("expected projection validation error")
			}
		})
	}
}

func TestProjectFlowQueryBoundsDynamicNamesFieldCountAndQueryBytes(t *testing.T) {
	if _, err := FlowAttributeProjection("__private"); err == nil {
		t.Fatal("expected private attribute to fail")
	}
	if _, err := FlowEventFieldProjection(strings.Repeat("x", 65)); err == nil {
		t.Fatal("expected oversized event field to fail")
	}
	fields := make([]FlowProjectionField, 33)
	for index := range fields {
		field, err := FlowAttributeProjection("field_" + string(rune('a'+index)))
		if err != nil {
			t.Fatal(err)
		}
		fields[index] = field
	}
	if _, err := ProjectFlowQuery(
		"FROM runs WHERE run_id = @id", FlowProjectionRecords, fields...,
	); err == nil {
		t.Fatal("expected projection field limit to fail")
	}
	if _, err := ProjectFlowQuery(
		"FROM runs WHERE type = '"+strings.Repeat("x", 16_350)+"'",
		FlowProjectionRecord,
		FlowRunID,
	); err == nil {
		t.Fatal("expected projected query byte limit to fail")
	}
}
