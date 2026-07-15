package ferricstore

import (
	"context"
	"math"
	"testing"
)

func TestHashFieldExpiryCommandsRejectInvalidInputsBeforeTransport(t *testing.T) {
	tests := []struct {
		name string
		call func(*HashStore) error
	}{
		{name: "HEXPIRE empty fields", call: func(store *HashStore) error {
			_, err := store.Expire(context.Background(), "hash", 1)
			return err
		}},
		{name: "HEXPIRE zero expiry", call: func(store *HashStore) error {
			_, err := store.Expire(context.Background(), "hash", 0, "field")
			return err
		}},
		{name: "HPEXPIRE negative expiry", call: func(store *HashStore) error {
			_, err := store.PExpire(context.Background(), "hash", -1, "field")
			return err
		}},
		{name: "HTTL empty fields", call: func(store *HashStore) error {
			_, err := store.TTL(context.Background(), "hash")
			return err
		}},
		{name: "HPTTL empty fields", call: func(store *HashStore) error {
			_, err := store.PTTL(context.Background(), "hash")
			return err
		}},
		{name: "HEXPIRETIME empty fields", call: func(store *HashStore) error {
			_, err := store.ExpireTime(context.Background(), "hash")
			return err
		}},
		{name: "HPEXPIRETIME empty fields", call: func(store *HashStore) error {
			_, err := store.PExpireTime(context.Background(), "hash")
			return err
		}},
		{name: "HPERSIST empty fields", call: func(store *HashStore) error {
			_, err := store.Persist(context.Background(), "hash")
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []any{int64(1)}}
			if err := test.call(NewClientWithExecutor(exec).Hash()); err == nil {
				t.Fatal("invalid hash field command succeeded")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid hash field command reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestCollectionOptionErrorsFailBeforeCodecOrTransport(t *testing.T) {
	zero := int64(0)
	negative := int64(-1)
	zeroInt := 0
	negativeInt := -1
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "HRANDFIELD values without count", call: func(client *Client) error {
			_, err := client.Hash().RandField(context.Background(), "hash", nil, true)
			return err
		}},
		{name: "LPOS zero rank", call: func(client *Client) error {
			_, err := client.ListStore().Pos(context.Background(), "list", "value", &zero, nil, nil)
			return err
		}},
		{name: "LPOS negative count", call: func(client *Client) error {
			_, err := client.ListStore().Pos(context.Background(), "list", "value", nil, &negative, nil)
			return err
		}},
		{name: "LPOS negative maxlen", call: func(client *Client) error {
			_, err := client.ListStore().Pos(context.Background(), "list", "value", nil, nil, &negative)
			return err
		}},
		{name: "LMOVE invalid source direction", call: func(client *Client) error {
			_, err := client.ListStore().Move(context.Background(), "one", "two", "MIDDLE", "LEFT")
			return err
		}},
		{name: "BLPOP empty keys", call: func(client *Client) error {
			_, err := client.ListStore().BLPop(context.Background(), 0)
			return err
		}},
		{name: "BRPOP negative timeout", call: func(client *Client) error {
			_, err := client.ListStore().BRPop(context.Background(), -1, "list")
			return err
		}},
		{name: "BLMOVE invalid destination direction", call: func(client *Client) error {
			_, err := client.ListStore().BLMove(context.Background(), "one", "two", "LEFT", "MIDDLE", 0)
			return err
		}},
		{name: "BLMPOP non-finite timeout", call: func(client *Client) error {
			_, err := client.ListStore().BLMPop(context.Background(), math.Inf(1), []string{"list"}, "LEFT", nil)
			return err
		}},
		{name: "BLMPOP empty keys", call: func(client *Client) error {
			_, err := client.ListStore().BLMPop(context.Background(), 0, nil, "LEFT", nil)
			return err
		}},
		{name: "BLMPOP invalid direction", call: func(client *Client) error {
			_, err := client.ListStore().BLMPop(context.Background(), 0, []string{"list"}, "MIDDLE", nil)
			return err
		}},
		{name: "BLMPOP zero count", call: func(client *Client) error {
			_, err := client.ListStore().BLMPop(context.Background(), 0, []string{"list"}, "LEFT", &zeroInt)
			return err
		}},
		{name: "SDIFF empty keys", call: func(client *Client) error {
			_, err := client.SetStore().Diff(context.Background())
			return err
		}},
		{name: "SINTERSTORE empty sources", call: func(client *Client) error {
			_, err := client.SetStore().InterStore(context.Background(), "destination")
			return err
		}},
		{name: "SINTERCARD empty keys", call: func(client *Client) error {
			_, err := client.SetStore().InterCard(context.Background(), nil, nil)
			return err
		}},
		{name: "SINTERCARD negative limit", call: func(client *Client) error {
			_, err := client.SetStore().InterCard(context.Background(), []string{"set"}, &negative)
			return err
		}},
		{name: "ZPOPMIN negative count", call: func(client *Client) error {
			_, err := client.SortedSet().PopMin(context.Background(), "zset", &negativeInt)
			return err
		}},
		{name: "ZRANDMEMBER scores without count", call: func(client *Client) error {
			_, err := client.SortedSet().RandMember(context.Background(), "zset", nil, true)
			return err
		}},
		{name: "ZRANGEBYSCORE negative offset", call: func(client *Client) error {
			_, err := client.SortedSet().RangeByScore(context.Background(), "zset", "-inf", "+inf", false, &negative, &zero)
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []any{}}
			codec := &countingKVCodec{}
			if err := test.call(NewClientWithExecutor(exec, WithConcurrentCodec(codec))); err == nil {
				t.Fatal("invalid collection command succeeded")
			}
			if codec.encodes.Load() != 0 {
				t.Fatalf("invalid collection command invoked codec %d times", codec.encodes.Load())
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid collection command reached transport: %#v", exec.calls)
			}
		})
	}
}
