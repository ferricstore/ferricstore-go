package ferricstore

import (
	"context"
	"strings"
	"testing"
)

func TestV080ScanLimitsRejectBeforeTransport(t *testing.T) {
	tooLargeCount := 10_001
	tooLargeCursor := "~" + strings.Repeat("x", 87_384)
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "SCAN count", call: func(client *Client) error {
			_, err := client.Scan(context.Background(), 0, "", &tooLargeCount)
			return err
		}},
		{name: "HSCAN count", call: func(client *Client) error {
			_, err := client.Hash().Scan(context.Background(), "key", 0, "", &tooLargeCount)
			return err
		}},
		{name: "SSCAN count", call: func(client *Client) error {
			_, err := client.SetStore().Scan(context.Background(), "key", 0, "", &tooLargeCount)
			return err
		}},
		{name: "ZSCAN count", call: func(client *Client) error {
			_, err := client.SortedSet().Scan(context.Background(), "key", 0, "", &tooLargeCount)
			return err
		}},
		{name: "HSCAN cursor", call: func(client *Client) error {
			_, err := client.Hash().ScanCursor(context.Background(), "key", tooLargeCursor, "", nil)
			return err
		}},
		{name: "SSCAN cursor", call: func(client *Client) error {
			_, err := client.SetStore().ScanCursor(context.Background(), "key", []byte(tooLargeCursor), "", nil)
			return err
		}},
		{name: "ZSCAN cursor", call: func(client *Client) error {
			_, err := client.SortedSet().ScanCursor(context.Background(), "key", tooLargeCursor, "", nil)
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []any{"0", []any{}}}
			if err := test.call(NewClientWithExecutor(exec)); err == nil {
				t.Fatal("request exceeding a FerricStore 0.8 scan limit succeeded")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("request exceeding a FerricStore 0.8 scan limit reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestV080BlockingListTimeoutLimitsRejectBeforeTransport(t *testing.T) {
	tooLargeSeconds := maxBlockingTimeoutMS/1_000 + 1
	tooLargeFractionalSeconds := float64(maxBlockingTimeoutMS)/1_000 + 0.001
	tests := []struct {
		name string
		call func(*ListStore) error
	}{
		{name: "BLPOP", call: func(store *ListStore) error {
			_, err := store.BLPop(context.Background(), tooLargeSeconds, "key")
			return err
		}},
		{name: "BRPOP", call: func(store *ListStore) error {
			_, err := store.BRPop(context.Background(), tooLargeSeconds, "key")
			return err
		}},
		{name: "BLMOVE", call: func(store *ListStore) error {
			_, err := store.BLMove(context.Background(), "source", "destination", "LEFT", "RIGHT", tooLargeSeconds)
			return err
		}},
		{name: "BLMPOP", call: func(store *ListStore) error {
			_, err := store.BLMPop(context.Background(), tooLargeFractionalSeconds, []string{"key"}, "LEFT", nil)
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{}
			if err := test.call(NewClientWithExecutor(exec).ListStore()); err == nil {
				t.Fatal("request exceeding the FerricStore 0.8 blocking timeout limit succeeded")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("oversized blocking timeout reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestV080ScanAndBlockingLimitBoundariesAreAccepted(t *testing.T) {
	count := maxScanCountV080
	cursor := "~" + strings.Repeat("x", maxCollectionCursorTokenBytesV080-1)
	scanExec := &fakeExecutor{value: []any{"0", []any{}}}
	client := NewClientWithExecutor(scanExec)
	if _, err := client.Scan(context.Background(), 0, "", &count); err != nil {
		t.Fatalf("maximum SCAN count rejected: %v", err)
	}
	if _, err := client.Hash().ScanCursor(context.Background(), "key", cursor, "", nil); err != nil {
		t.Fatalf("maximum collection cursor rejected: %v", err)
	}
	if len(scanExec.calls) != 2 {
		t.Fatalf("accepted scan boundary calls = %d, want 2", len(scanExec.calls))
	}

	blockingExec := &fakeExecutor{}
	list := NewClientWithExecutor(blockingExec).ListStore()
	if _, err := list.BLPop(context.Background(), maxBlockingTimeoutMS/1_000, "key"); err != nil {
		t.Fatalf("maximum whole-second blocking timeout rejected: %v", err)
	}
	if _, err := list.BLMPop(context.Background(), float64(maxBlockingTimeoutMS)/1_000, []string{"key"}, "LEFT", nil); err != nil {
		t.Fatalf("maximum fractional blocking timeout rejected: %v", err)
	}
	if len(blockingExec.calls) != 2 {
		t.Fatalf("accepted blocking boundary calls = %d, want 2", len(blockingExec.calls))
	}
}
