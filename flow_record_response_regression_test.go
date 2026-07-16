package ferricstore

import "testing"

func TestFlowBatchAcceptsAggregateOKForMultipleItems(t *testing.T) {
	for _, value := range []any{"OK", []byte("OK")} {
		records, err := recordsOrNil(value, RawCodec{}, 2)
		if err != nil {
			t.Fatalf("aggregate acknowledgement %T failed: %v", value, err)
		}
		if records != nil {
			t.Fatalf("aggregate acknowledgement %T returned records %#v", value, records)
		}
	}
}

func TestFlowBatchRejectsPartialPerItemAcknowledgements(t *testing.T) {
	for _, value := range []any{nativeCompactOKCount(1), []any{"OK"}} {
		if _, err := recordsOrNil(value, RawCodec{}, 2); err == nil {
			t.Fatalf("partial acknowledgement %T succeeded", value)
		}
	}
}
