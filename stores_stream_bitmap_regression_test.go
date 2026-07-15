package ferricstore

import (
	"context"
	"testing"
)

func TestStreamOptionsRejectInvalidWireStatesBeforeTransport(t *testing.T) {
	negative := -1
	negativeMS := int64(-1)
	limit := 1
	tests := []struct {
		name string
		call func(*StreamStore) error
	}{
		{name: "XRANGE negative count", call: func(store *StreamStore) error {
			_, err := store.Range(context.Background(), "stream", "-", "+", &negative)
			return err
		}},
		{name: "XREVRANGE negative count", call: func(store *StreamStore) error {
			_, err := store.RevRange(context.Background(), "stream", "+", "-", &negative)
			return err
		}},
		{name: "XREAD empty streams", call: func(store *StreamStore) error {
			_, err := store.Read(context.Background(), StreamReadOptions{})
			return err
		}},
		{name: "XREAD negative count", call: func(store *StreamStore) error {
			_, err := store.Read(context.Background(), StreamReadOptions{Count: &negative, Streams: []StreamRef{{Key: "stream", ID: "0-0"}}})
			return err
		}},
		{name: "XREAD negative block", call: func(store *StreamStore) error {
			_, err := store.Read(context.Background(), StreamReadOptions{BlockMS: &negativeMS, Streams: []StreamRef{{Key: "stream", ID: "0-0"}}})
			return err
		}},
		{name: "XREADGROUP empty streams", call: func(store *StreamStore) error {
			_, err := store.ReadGroup(context.Background(), StreamReadGroupOptions{Group: "group", Consumer: "consumer"})
			return err
		}},
		{name: "XREADGROUP negative block", call: func(store *StreamStore) error {
			_, err := store.ReadGroup(context.Background(), StreamReadGroupOptions{Group: "group", Consumer: "consumer", BlockMS: &negativeMS, Streams: []StreamRef{{Key: "stream", ID: ">"}}})
			return err
		}},
		{name: "XTRIM invalid threshold", call: func(store *StreamStore) error {
			_, err := store.Trim(context.Background(), "stream", false, "not-a-number", nil)
			return err
		}},
		{name: "XTRIM unsupported limit", call: func(store *StreamStore) error {
			_, err := store.Trim(context.Background(), "stream", true, "10", &limit)
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []any{}}
			if err := test.call(NewClientWithExecutor(exec).Stream()); err == nil {
				t.Fatal("invalid stream command succeeded")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid stream command reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestBitmapCommandsRejectInvalidInputsBeforeTransport(t *testing.T) {
	start := int64(0)
	end := int64(1)
	tests := []struct {
		name string
		call func(*BitmapStore) error
	}{
		{name: "SETBIT negative offset", call: func(store *BitmapStore) error {
			_, err := store.SetBit(context.Background(), "key", -1, 1)
			return err
		}},
		{name: "SETBIT oversized offset", call: func(store *BitmapStore) error {
			_, err := store.SetBit(context.Background(), "key", 4_294_967_296, 1)
			return err
		}},
		{name: "SETBIT invalid bit", call: func(store *BitmapStore) error {
			_, err := store.SetBit(context.Background(), "key", 0, 2)
			return err
		}},
		{name: "GETBIT negative offset", call: func(store *BitmapStore) error {
			_, err := store.GetBit(context.Background(), "key", -1)
			return err
		}},
		{name: "BITPOS invalid bit", call: func(store *BitmapStore) error {
			_, err := store.Pos(context.Background(), "key", 2, nil, nil)
			return err
		}},
		{name: "BITPOS end without start", call: func(store *BitmapStore) error {
			_, err := store.Pos(context.Background(), "key", 1, nil, &end)
			return err
		}},
		{name: "BITOP empty sources", call: func(store *BitmapStore) error {
			_, err := store.Op(context.Background(), "AND", "destination")
			return err
		}},
		{name: "BITOP unknown operation", call: func(store *BitmapStore) error {
			_, err := store.Op(context.Background(), "NAND", "destination", "source")
			return err
		}},
		{name: "BITOP NOT multiple sources", call: func(store *BitmapStore) error {
			_, err := store.Op(context.Background(), "NOT", "destination", "one", "two")
			return err
		}},
	}
	_ = start

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: int64(0)}
			if err := test.call(NewClientWithExecutor(exec).Bitmap()); err == nil {
				t.Fatal("invalid bitmap command succeeded")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid bitmap command reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestBitmapCommandsRejectOutOfContractResponses(t *testing.T) {
	tests := []struct {
		name     string
		response int64
		call     func(*BitmapStore) error
	}{
		{name: "SETBIT", response: 2, call: func(store *BitmapStore) error { _, err := store.SetBit(context.Background(), "key", 0, 1); return err }},
		{name: "GETBIT", response: -1, call: func(store *BitmapStore) error { _, err := store.GetBit(context.Background(), "key", 0); return err }},
		{name: "BITCOUNT", response: -1, call: func(store *BitmapStore) error { _, err := store.Count(context.Background(), "key"); return err }},
		{name: "BITPOS", response: -2, call: func(store *BitmapStore) error {
			_, err := store.Pos(context.Background(), "key", 1, nil, nil)
			return err
		}},
		{name: "BITOP", response: -1, call: func(store *BitmapStore) error {
			_, err := store.Op(context.Background(), "AND", "dest", "source")
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(NewClientWithExecutor(&fakeExecutor{value: test.response}).Bitmap()); err == nil {
				t.Fatalf("accepted response %d", test.response)
			}
		})
	}
}

func TestHyperLogLogRejectsEmptyOperandsBeforeTransport(t *testing.T) {
	tests := []struct {
		name string
		call func(*HyperLogLogStore) error
	}{
		{name: "PFADD", call: func(store *HyperLogLogStore) error { _, err := store.Add(context.Background(), "key"); return err }},
		{name: "PFCOUNT", call: func(store *HyperLogLogStore) error { _, err := store.Count(context.Background()); return err }},
		{name: "PFMERGE", call: func(store *HyperLogLogStore) error { return store.Merge(context.Background(), "destination") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: int64(0)}
			if err := test.call(NewClientWithExecutor(exec).HyperLogLog()); err == nil {
				t.Fatal("empty HLL command succeeded")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("empty HLL command reached transport: %#v", exec.calls)
			}
		})
	}
}
