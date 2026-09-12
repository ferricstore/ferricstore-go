package ferricstore

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestWrapFlowQueryErrorDecodesHTTPDetails(t *testing.T) {
	details := flowQueryDiagnosticForTest()
	details["retry_after_ms"] = json.Number("0")
	details["context"] = map[string]any{
		"max_projection_fields": json.Number("64"),
		"nested":                []any{json.Number("2")},
	}
	details["position"] = map[string]any{
		"byte": json.Number("17"), "line": json.Number("1"), "column": json.Number("19"),
	}
	cause := &HTTPError{
		StatusCode: 200,
		Code:       "unsupported_field",
		Message:    "ERR FQL1 field is not supported",
		Details:    details,
	}

	err := wrapFlowQueryError(cause)
	var queryErr *FlowQueryError
	if !errors.As(err, &queryErr) {
		t.Fatalf("error = %T %v, want *FlowQueryError", err, err)
	}
	if queryErr.Code != "unsupported_field" || queryErr.Position == nil || queryErr.Position.Column != 19 {
		t.Fatalf("query error = %#v", queryErr)
	}
	if queryErr.Context["max_projection_fields"] != int64(64) {
		t.Fatalf("query context = %#v", queryErr.Context)
	}
	nested, ok := queryErr.Context["nested"].([]any)
	if !ok || len(nested) != 1 || nested[0] != int64(2) {
		t.Fatalf("nested query context = %#v", queryErr.Context["nested"])
	}
	if !errors.Is(queryErr, cause) {
		t.Fatal("query error does not preserve the HTTP cause")
	}
}

func TestWrapFlowQueryErrorRejectsOutOfRangeHTTPContextIntegers(t *testing.T) {
	details := flowQueryDiagnosticForTest()
	details["retry_after_ms"] = json.Number("0")
	details["context"] = map[string]any{"limit": json.Number("9223372036854775808")}
	cause := &HTTPError{StatusCode: 200, Code: "invalid", Message: "invalid", Details: details}

	err := wrapFlowQueryError(cause)
	var queryErr *FlowQueryError
	if errors.As(err, &queryErr) {
		t.Fatalf("error = %#v, want original bounded HTTP error", queryErr)
	}
	if !errors.Is(err, cause) {
		t.Fatal("invalid diagnostic did not preserve the original HTTP error")
	}
	context := details["context"].(map[string]any)
	if context["limit"] != json.Number("9223372036854775808") {
		t.Fatalf("source diagnostic was mutated: %#v", details)
	}
}
