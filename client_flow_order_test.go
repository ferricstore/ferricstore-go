package ferricstore

import (
	"reflect"
	"testing"
)

type orderedFlowValueCodec struct {
	seen []string
}

func (c *orderedFlowValueCodec) Encode(value any) (any, error) {
	c.seen = append(c.seen, value.(string))
	return value, nil
}

func (*orderedFlowValueCodec) Decode(value any) (any, error) { return value, nil }

func TestFlowNamedValuesUseDeterministicCustomCodecOrder(t *testing.T) {
	values := map[string]any{
		"hotel": "hotel", "alpha": "alpha", "golf": "golf", "bravo": "bravo",
		"foxtrot": "foxtrot", "charlie": "charlie", "echo": "echo", "delta": "delta",
	}
	want := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"}
	codec := &orderedFlowValueCodec{}
	client := NewClientWithExecutor(&fakeExecutor{}, WithCodec(codec))

	for range 32 {
		codec.seen = nil
		args := []any{"FLOW.COMPLETE", "flow"}
		if err := client.appendNamedValues(&args, NamedValues{Values: values}); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(codec.seen, want) {
			t.Fatalf("named value codec order = %v, want %v", codec.seen, want)
		}
	}
}

func TestFlowExtendedItemValuesUseDeterministicCustomCodecOrder(t *testing.T) {
	values := map[string]any{
		"hotel": "hotel", "alpha": "alpha", "golf": "golf", "bravo": "bravo",
		"foxtrot": "foxtrot", "charlie": "charlie", "echo": "echo", "delta": "delta",
	}
	want := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"}
	codec := &orderedFlowValueCodec{}
	client := NewClientWithExecutor(&fakeExecutor{}, WithCodec(codec))

	for range 32 {
		codec.seen = nil
		args := []any{"ITEM"}
		if err := client.appendNamedCounts(&args, values, nil); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(codec.seen, want) {
			t.Fatalf("extended item codec order = %v, want %v", codec.seen, want)
		}
	}
}
