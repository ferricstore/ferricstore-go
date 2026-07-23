//go:build integration

package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func createAndClaim(t *testing.T, ctx context.Context, client *Client, typeName, runID, name, state string, now, leaseMS int64) claimedFlow {
	t.Helper()
	id := "go-sdk:" + name + ":" + runID
	partition := id + ":partition"
	created := must[*FlowRecord](t)(client.Create(ctx, CreateOptions{ID: id, Type: typeName, State: state, PartitionKey: partition, Payload: map[string]any{"name": name}, RunAtMS: now, NowMS: now, Idempotent: Bool(true), ReturnRecord: true}))
	if created == nil {
		t.Fatal("FLOW.CREATE did not return the requested record")
	}
	createdAtMS := asInt64(created.Raw["updated_at_ms"])
	if createdAtMS <= 0 || created.Version <= 0 {
		t.Fatalf("FLOW.CREATE event identity is incomplete: %#v", created.Raw)
	}
	return claimedFlow{
		id:             id,
		partitionKey:   partition,
		createdEventID: fmt.Sprintf("%d-%d", createdAtMS, created.Version),
		job:            claimOne(t, ctx, client, typeName, state, partition, "go-sdk-"+name+"-worker", now, leaseMS),
	}
}

func flushHistoryProjectorForRewind(t *testing.T, ctx context.Context, client *Client, flow claimedFlow) {
	t.Helper()
	history, err := client.History(ctx, HistoryOptions{
		ID:                   flow.id,
		PartitionKey:         flow.partitionKey,
		Count:                10,
		IncludeCold:          Bool(true),
		ConsistentProjection: Bool(true),
	})
	if err == nil {
		requireLenAtLeast(t, history, 1)
		return
	}
	if !strings.Contains(err.Error(), "flow LMDB projection unavailable") {
		t.Fatalf("FLOW.HISTORY projector flush: %v", err)
	}
}

func claimOne(t *testing.T, ctx context.Context, client *Client, typeName, state, partition, worker string, now, leaseMS int64) ClaimedItem {
	t.Helper()
	jobs := must[[]ClaimedItem](t)(client.ClaimJobs(ctx, ClaimDueOptions{Type: typeName, State: state, Worker: worker, PartitionKey: partition, LeaseMS: leaseMS, Limit: 1, NowMS: now}))
	requireLen(t, jobs, 1)
	return jobs[0]
}

func claimMany(t *testing.T, ctx context.Context, client *Client, typeName, state, partition, worker string, now int64, limit int) []ClaimedItem {
	t.Helper()
	jobs := must[[]ClaimedItem](t)(client.ClaimJobs(ctx, ClaimDueOptions{Type: typeName, State: state, Worker: worker, PartitionKey: partition, LeaseMS: 30_000, Limit: limit, NowMS: now}))
	requireLen(t, jobs, limit)
	return jobs
}

func fencedItems(items []ClaimedItem) []FencedItem {
	out := make([]FencedItem, 0, len(items))
	for _, item := range items {
		out = append(out, FencedItem{ID: item.ID, LeaseToken: item.LeaseToken, FencingToken: item.FencingToken, PartitionKey: item.PartitionKey})
	}
	return out
}

func responseField(value any, name string) any {
	mapping, err := nativeMap(value)
	if err != nil {
		return nil
	}
	return mapping[name]
}

func integrationContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 30*time.Second)
}

func integrationClient(codec Codec) *Client {
	return newIntegrationTrackedClient(integrationAddress(), codec)
}

func integrationDirectClient(codec Codec) *Client {
	return NewClient(integrationAddress(), WithCodec(codec))
}

func integrationAddress() string {
	addr := os.Getenv("FERRICSTORE_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6388"
	}
	return addr
}

func integrationSuffix(name string) string {
	return fmt.Sprintf("%s:%d", name, time.Now().UnixNano())
}

const (
	integrationProjectionTimeout = 30 * time.Second
	integrationProjectionRetry   = 100 * time.Millisecond
)

func waitForFlowQueryRecord(
	t *testing.T,
	ctx context.Context,
	id string,
	query func() ([]FlowRecord, error),
) []FlowRecord {
	t.Helper()
	deadline := time.NewTimer(integrationProjectionTimeout)
	defer deadline.Stop()
	retry := time.NewTicker(integrationProjectionRetry)
	defer retry.Stop()
	var lastRecords []FlowRecord
	var lastErr error

	for {
		lastRecords, lastErr = query()
		if lastErr == nil && hasRecordID(lastRecords, id) {
			return lastRecords
		}
		if lastErr != nil && !transientIntegrationFlowQueryError(lastErr) {
			t.Fatalf("FLOW.QUERY while waiting for %q: %v", id, lastErr)
		}

		select {
		case <-ctx.Done():
			t.Fatalf("FLOW.QUERY while waiting for %q: %v (records=%#v, last_error=%v)", id, ctx.Err(), lastRecords, lastErr)
		case <-deadline.C:
			t.Fatalf("FLOW.QUERY did not project %q within %s (records=%#v, last_error=%v)", id, integrationProjectionTimeout, lastRecords, lastErr)
		case <-retry.C:
		}
	}
}

func waitForFlowQueryResult(
	t *testing.T,
	ctx context.Context,
	query func() (*FlowQueryResult, error),
	ready func(*FlowQueryResult) bool,
) *FlowQueryResult {
	t.Helper()
	deadline := time.NewTimer(integrationProjectionTimeout)
	defer deadline.Stop()
	retry := time.NewTicker(integrationProjectionRetry)
	defer retry.Stop()
	var last *FlowQueryResult
	var lastErr error

	for {
		last, lastErr = query()
		if lastErr == nil && ready(last) {
			return last
		}
		if lastErr != nil && !transientIntegrationFlowQueryError(lastErr) {
			t.Fatalf("FLOW.QUERY while waiting for projection: %v", lastErr)
		}

		select {
		case <-ctx.Done():
			t.Fatalf("FLOW.QUERY while waiting for projection: %v (result=%#v, last_error=%v)", ctx.Err(), last, lastErr)
		case <-deadline.C:
			t.Fatalf("FLOW.QUERY projection did not converge within %s (result=%#v, last_error=%v)", integrationProjectionTimeout, last, lastErr)
		case <-retry.C:
		}
	}
}

func transientIntegrationFlowQueryError(err error) bool {
	var queryErr *FlowQueryError
	if !errors.As(err, &queryErr) {
		return false
	}
	switch queryErr.Code {
	case "query_concurrency_exceeded", "query_no_bounded_plan", "query_projection_changed", "query_storage_unavailable":
		return true
	default:
		return false
	}
}

func cleanupPrefix(t *testing.T, ctx context.Context, client *Client, prefix string) {
	t.Helper()
	keys, err := client.Keys(ctx, prefix+"*")
	if err != nil || len(keys) == 0 {
		return
	}
	if _, err := client.Delete(ctx, keys...); err != nil {
		t.Fatalf("cleanup %s: %v", prefix, err)
	}
}

func must[T any](t *testing.T) func(T, error) T {
	t.Helper()
	return func(value T, err error) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
}

func requireValue(t *testing.T, value any) {
	t.Helper()
	switch v := value.(type) {
	case nil:
		t.Fatalf("expected value, got %#v", value)
	case string:
		if v == "" {
			t.Fatalf("expected value, got %#v", value)
		}
	case []byte:
		if len(v) == 0 {
			t.Fatalf("expected value, got %#v", value)
		}
	}
}

func requireMap(t *testing.T, value map[string]any) {
	t.Helper()
	if len(value) == 0 {
		t.Fatalf("expected non-empty map, got %#v", value)
	}
}

func requireTrue(t *testing.T, value bool) {
	t.Helper()
	if !value {
		t.Fatal("expected true")
	}
}

func requireString(t *testing.T, value any, want string) {
	t.Helper()
	if asString(value) != want {
		t.Fatalf("expected %q, got %#v", want, value)
	}
}

func requireInt64(t *testing.T, value, want int64) {
	t.Helper()
	if value != want {
		t.Fatalf("expected %d, got %d", want, value)
	}
}

func requirePositive(t *testing.T, value int64) {
	t.Helper()
	if value < 1 {
		t.Fatalf("expected positive integer, got %d", value)
	}
}

func requireNonNegative(t *testing.T, value int64) {
	t.Helper()
	if value < 0 {
		t.Fatalf("expected non-negative integer, got %d", value)
	}
}

func requireNonNegativeFloat(t *testing.T, value float64) {
	t.Helper()
	if value < 0 {
		t.Fatalf("expected non-negative float, got %f", value)
	}
}

func requireOKResponse(t *testing.T, value any) {
	t.Helper()
	boolValue, _ := responseBool(value, nil)
	if !isOK(value) && asInt64(value) != 1 && !boolValue {
		t.Fatalf("expected OK response, got %#v", value)
	}
}

func requireCommandError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected command error")
	}
}

func requireRecognizedCommandError(t *testing.T, err error, args ...any) {
	t.Helper()
	requireCommandError(t, err)
	message := strings.ToLower(err.Error())
	for _, invalid := range []string{
		"unknown command",
		"unsupported opcode",
		"wrong number of arguments",
		"syntax error",
		"connection unavailable",
		"connection reset",
		"context deadline",
		"context canceled",
		"unexpected eof",
	} {
		if strings.Contains(message, invalid) {
			t.Fatalf("command %#v was not recognized with the expected wire shape: %v", args, err)
		}
	}
	recordIntegrationCommand(args)
}

func requireLen[T any](t *testing.T, values []T, want int) {
	t.Helper()
	if len(values) != want {
		t.Fatalf("expected %d items, got %d: %#v", want, len(values), values)
	}
}

func requireLenAtLeast[T any](t *testing.T, values []T, want int) {
	t.Helper()
	if len(values) < want {
		t.Fatalf("expected at least %d items, got %d: %#v", want, len(values), values)
	}
}

func hasRecordID(records []FlowRecord, id string) bool {
	for _, record := range records {
		if record.ID == id {
			return true
		}
	}
	return false
}

func hasRecordPrefix(records []FlowRecord, prefix string) bool {
	for _, record := range records {
		if strings.HasPrefix(record.ID, prefix) {
			return true
		}
	}
	return false
}

func flowStateMetaValue(record *FlowRecord, state, name string) string {
	if record == nil || record.StateMeta == nil {
		return ""
	}
	meta, _ := record.StateMeta[state].(map[string]any)
	return asString(meta[name])
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsRuleForUser(values []string, username string) bool {
	prefix := "user " + username + " "
	for _, value := range values {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return false
}

func containsFold(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(value, target) {
			return true
		}
	}
	return false
}
