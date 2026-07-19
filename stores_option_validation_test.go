package ferricstore

import (
	"context"
	"testing"
)

func TestTypedStoreOptionsRejectInvalidStatesBeforeExecution(t *testing.T) {
	zero := int64(0)
	negative := int64(-1)
	one := int64(1)
	two := int64(2)
	coordinate := &GeoCoordinate{Longitude: 1, Latitude: 2}
	radius := &GeoRadius{Radius: 5, Unit: "km"}
	box := &GeoBox{Width: 1, Height: 2, Unit: "km"}

	tests := []struct {
		name string
		call func(*Client) error
	}{
		{
			name: "hash getex zero expiry",
			call: func(client *Client) error {
				_, err := client.Hash().GetEX(context.Background(), "key", []string{"field"}, HashGetEXOptions{EXSeconds: &zero})
				return err
			},
		},
		{
			name: "hash getex negative expiry",
			call: func(client *Client) error {
				_, err := client.Hash().GetEX(context.Background(), "key", []string{"field"}, HashGetEXOptions{PXMilliseconds: &negative})
				return err
			},
		},
		{
			name: "hash setex zero expiry",
			call: func(client *Client) error {
				_, err := client.Hash().SetEX(context.Background(), "key", map[string]any{"field": "value"}, HashSetEXOptions{EXSeconds: &zero})
				return err
			},
		},
		{
			name: "hash setex negative expiry",
			call: func(client *Client) error {
				_, err := client.Hash().SetEX(context.Background(), "key", map[string]any{"field": "value"}, HashSetEXOptions{EXSeconds: &negative})
				return err
			},
		},
		{
			name: "hash setex empty fields",
			call: func(client *Client) error {
				_, err := client.Hash().SetEX(context.Background(), "key", map[string]any{}, HashSetEXOptions{EXSeconds: &one})
				return err
			},
		},
		{
			name: "hash getex multiple expiries",
			call: func(client *Client) error {
				_, err := client.Hash().GetEX(context.Background(), "key", []string{"field"}, HashGetEXOptions{EXSeconds: &one, PXMilliseconds: &two})
				return err
			},
		},
		{
			name: "hash getex persist and expiry",
			call: func(client *Client) error {
				_, err := client.Hash().GetEX(context.Background(), "key", []string{"field"}, HashGetEXOptions{EXSeconds: &one, Persist: true})
				return err
			},
		},
		{
			name: "zadd nx and xx",
			call: func(client *Client) error {
				_, err := client.SortedSet().AddWithOptions(context.Background(), "key", ZAddOptions{NX: true, XX: true}, ZAddMember{Score: 1, Member: "one"})
				return err
			},
		},
		{
			name: "zadd gt and lt",
			call: func(client *Client) error {
				_, err := client.SortedSet().AddWithOptions(context.Background(), "key", ZAddOptions{GT: true, LT: true}, ZAddMember{Score: 1, Member: "one"})
				return err
			},
		},
		{
			name: "zadd nx and gt",
			call: func(client *Client) error {
				_, err := client.SortedSet().AddWithOptions(context.Background(), "key", ZAddOptions{NX: true, GT: true}, ZAddMember{Score: 1, Member: "one"})
				return err
			},
		},
		{
			name: "geo multiple origins",
			call: func(client *Client) error {
				_, err := client.Geo().Search(context.Background(), "key", GeoSearchOptions{FromMember: "member", FromLonLat: coordinate, ByRadius: radius})
				return err
			},
		},
		{
			name: "geo multiple shapes",
			call: func(client *Client) error {
				_, err := client.Geo().Search(context.Background(), "key", GeoSearchOptions{FromMember: "member", ByRadius: radius, ByBox: box})
				return err
			},
		},
		{
			name: "geo conflicting sort",
			call: func(client *Client) error {
				_, err := client.Geo().Search(context.Background(), "key", GeoSearchOptions{FromMember: "member", ByRadius: radius, Asc: true, Desc: true})
				return err
			},
		},
		{
			name: "geo any without count",
			call: func(client *Client) error {
				_, err := client.Geo().Search(context.Background(), "key", GeoSearchOptions{FromMember: "member", ByRadius: radius, Any: true})
				return err
			},
		},
		{
			name: "topk width only",
			call: func(client *Client) error {
				_, err := client.TopK().ReserveWithOptions(context.Background(), "key", 3, TopKReserveOptions{Width: &one})
				return err
			},
		},
		{
			name: "topk depth without width",
			call: func(client *Client) error {
				_, err := client.TopK().ReserveWithOptions(context.Background(), "key", 3, TopKReserveOptions{Depth: &two})
				return err
			},
		},
		{
			name: "zrange partial limit",
			call: func(client *Client) error {
				_, err := client.SortedSet().RangeByScore(context.Background(), "key", "-inf", "+inf", false, &one, nil)
				return err
			},
		},
		{
			name: "zrevrange partial limit",
			call: func(client *Client) error {
				_, err := client.SortedSet().RevRangeByScore(context.Background(), "key", "+inf", "-inf", false, nil, &two)
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			if err := tc.call(NewClientWithExecutor(exec)); err == nil {
				t.Fatal("expected invalid option state to fail")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid options reached executor: %#v", exec.calls)
			}
		})
	}
}
