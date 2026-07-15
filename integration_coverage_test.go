//go:build integration

package ferricstore

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
)

var integrationCommandCoverage = struct {
	sync.Mutex
	seen    map[string]struct{}
	skipped map[string]string
}{
	seen:    map[string]struct{}{},
	skipped: map[string]string{},
}

type integrationTrackingExecutor struct {
	inner Executor
}

func (e *integrationTrackingExecutor) Do(ctx context.Context, args ...any) (any, error) {
	value, err := e.inner.Do(ctx, args...)
	if err == nil {
		recordIntegrationCommand(args)
	}
	return value, err
}

func (e *integrationTrackingExecutor) Pipeline(ctx context.Context, commands [][]any) ([]any, error) {
	if exec, ok := e.inner.(pipelineExecutor); ok {
		values, err := exec.Pipeline(ctx, commands)
		recordSuccessfulIntegrationPipelineCommands(commands, values)
		return values, err
	}
	results := make([]any, 0, len(commands))
	for _, args := range commands {
		value, err := e.inner.Do(ctx, args...)
		if err != nil {
			return nil, err
		}
		recordIntegrationCommand(args)
		results = append(results, value)
	}
	return results, nil
}

func (e *integrationTrackingExecutor) pipelineDetailed(ctx context.Context, commands [][]any) ([]pipelineItemResult, error) {
	if exec, ok := e.inner.(detailedPipelineExecutor); ok {
		results, err := exec.pipelineDetailed(ctx, commands)
		if err == nil && len(results) == len(commands) {
			for index, result := range results {
				if result.err == nil {
					recordIntegrationCommand(commands[index])
				}
			}
		}
		return results, err
	}
	values, err := e.Pipeline(ctx, commands)
	if err != nil && len(values) != len(commands) {
		return nil, err
	}
	if len(values) != len(commands) {
		return nil, fmt.Errorf("integration pipeline returned %d results for %d commands", len(values), len(commands))
	}
	results := make([]pipelineItemResult, len(values))
	for index, value := range values {
		if itemErr, ok := value.(error); ok {
			results[index].err = itemErr
		} else {
			results[index].value = value
		}
	}
	return results, nil
}

func (e *integrationTrackingExecutor) keyValueMGet(ctx context.Context, keys []string) (any, error) {
	bulk, ok := e.inner.(keyValueBulkExecutor)
	if !ok {
		return nil, errors.New("integration executor lost native MGET capability")
	}
	value, err := bulk.keyValueMGet(ctx, keys)
	if err == nil {
		args := make([]any, 1, len(keys)+1)
		args[0] = "MGET"
		for _, key := range keys {
			args = append(args, key)
		}
		recordIntegrationCommand(args)
	}
	return value, err
}

func (e *integrationTrackingExecutor) keyValueMSet(ctx context.Context, keys []string, values []any) (any, error) {
	bulk, ok := e.inner.(keyValueBulkExecutor)
	if !ok {
		return nil, errors.New("integration executor lost native MSET capability")
	}
	value, err := bulk.keyValueMSet(ctx, keys, values)
	if err == nil {
		args := make([]any, 1, 1+2*len(keys))
		args[0] = "MSET"
		for index, key := range keys {
			args = append(args, key, values[index])
		}
		recordIntegrationCommand(args)
	}
	return value, err
}

func (e *integrationTrackingExecutor) keyValueMSetNX(ctx context.Context, keys []string, values []any) (any, error) {
	bulk, ok := e.inner.(keyValueMSetNXExecutor)
	if !ok {
		return nil, errors.New("integration executor lost native MSETNX capability")
	}
	value, err := bulk.keyValueMSetNX(ctx, keys, values)
	if err == nil {
		args := make([]any, 1, 1+2*len(keys))
		args[0] = "MSETNX"
		for index, key := range keys {
			args = append(args, key, values[index])
		}
		recordIntegrationCommand(args)
	}
	return value, err
}

func (e *integrationTrackingExecutor) keyValueDel(ctx context.Context, keys []string) (any, error) {
	bulk, ok := e.inner.(keyValueDelExecutor)
	if !ok {
		return nil, errors.New("integration executor lost native DEL capability")
	}
	value, err := bulk.keyValueDel(ctx, keys)
	if err == nil {
		recordIntegrationCommand(keyListCommandArgs("DEL", keys))
	}
	return value, err
}

func (e *integrationTrackingExecutor) keyValueExists(ctx context.Context, keys []string) (any, error) {
	bulk, ok := e.inner.(keyValueExistsExecutor)
	if !ok {
		return nil, errors.New("integration executor lost native EXISTS capability")
	}
	value, err := bulk.keyValueExists(ctx, keys)
	if err == nil {
		recordIntegrationCommand(keyListCommandArgs("EXISTS", keys))
	}
	return value, err
}

func (e *integrationTrackingExecutor) acquireCommandSession(ctx context.Context, keys ...any) (commandSession, error) {
	provider, ok := e.inner.(commandSessionProvider)
	if !ok {
		return nil, errors.New("integration executor lost native command-session capability")
	}
	session, err := provider.acquireCommandSession(ctx, keys...)
	if err != nil {
		return nil, err
	}
	return &integrationTrackingSession{inner: session}, nil
}

type integrationTrackingSession struct {
	inner commandSession
}

func (s *integrationTrackingSession) Do(ctx context.Context, args ...any) (any, error) {
	value, err := s.inner.Do(ctx, args...)
	if err == nil {
		recordIntegrationCommand(args)
	}
	return value, err
}

func (s *integrationTrackingSession) Abort(err error) { s.inner.Abort(err) }
func (s *integrationTrackingSession) Release()        { s.inner.Release() }

func recordSuccessfulIntegrationPipelineCommands(commands [][]any, values []any) {
	if len(values) != len(commands) {
		return
	}
	for index, value := range values {
		if _, failed := value.(error); !failed {
			recordIntegrationCommand(commands[index])
		}
	}
}

func newIntegrationTrackedClient(addr string, codec Codec) *Client {
	exec := NewNativeExecutor(addr)
	client := NewClientWithExecutor(&integrationTrackingExecutor{inner: exec}, WithCodec(codec))
	client.closer = exec.Close
	return client
}

func recordIntegrationCommand(args []any) {
	key := integrationCommandKey(args)
	if key == "" {
		return
	}
	integrationCommandCoverage.Lock()
	integrationCommandCoverage.seen[key] = struct{}{}
	integrationCommandCoverage.Unlock()
}

func skipIntegrationCommandCoverage(reason string, commands ...string) {
	integrationCommandCoverage.Lock()
	defer integrationCommandCoverage.Unlock()
	for _, command := range commands {
		integrationCommandCoverage.skipped[command] = reason
	}
}

func integrationCommandKey(args []any) string {
	for len(args) > 1 && strings.EqualFold(asString(args[0]), "COMMAND_EXEC") {
		args = args[1:]
	}
	if len(args) == 0 {
		return ""
	}
	command := strings.ToUpper(asString(args[0]))
	if command == "GEOSEARCHSTORE" {
		for _, arg := range args[1:] {
			if strings.EqualFold(asString(arg), "STOREDIST") {
				return "GEOSEARCHSTORE STOREDIST"
			}
		}
	}
	switch command {
	case "ACL", "CLIENT", "COMMAND", "CONFIG", "MEMORY", "MODULE", "OBJECT", "PUBSUB", "SLOWLOG", "XGROUP", "XINFO":
		if len(args) > 1 {
			return command + " " + strings.ToUpper(asString(args[1]))
		}
	}
	return command
}

var (
	_ keyValueBulkExecutor     = (*integrationTrackingExecutor)(nil)
	_ keyValueMSetNXExecutor   = (*integrationTrackingExecutor)(nil)
	_ keyValueDelExecutor      = (*integrationTrackingExecutor)(nil)
	_ keyValueExistsExecutor   = (*integrationTrackingExecutor)(nil)
	_ pipelineExecutor         = (*integrationTrackingExecutor)(nil)
	_ detailedPipelineExecutor = (*integrationTrackingExecutor)(nil)
	_ commandSessionProvider   = (*integrationTrackingExecutor)(nil)
)

func TestMain(m *testing.M) {
	code := m.Run()
	if code == 0 && shouldCheckIntegrationCommandCoverage() {
		if missing := missingIntegrationCommands(); len(missing) > 0 {
			fmt.Fprintf(os.Stderr, "integration command coverage missing %d commands:\n%s\n", len(missing), strings.Join(missing, "\n"))
			code = 1
		}
	}
	os.Exit(code)
}

func TestMissingIntegrationCommandsTreatsSkippedCommandsAsMissingWhenStrict(t *testing.T) {
	seen := map[string]struct{}{
		"PING":        {},
		"FLOW.SEARCH": {},
	}
	skipped := map[string]string{
		"FLOW.SEARCH": "old server image",
	}
	expected := []string{"PING", "FLOW.SEARCH"}

	if missing := missingIntegrationCommandsFrom(expected, seen, skipped, false); len(missing) != 0 {
		t.Fatalf("non-strict coverage should allow skipped commands, got %v", missing)
	}

	missing := missingIntegrationCommandsFrom(expected, seen, skipped, true)
	if !reflect.DeepEqual(missing, []string{"FLOW.SEARCH"}) {
		t.Fatalf("strict coverage should report skipped commands, got %v", missing)
	}
}

type integrationBulkStub struct {
	doCalls   int
	bulkCalls int
	bulkErr   error
}

func (e *integrationBulkStub) Do(context.Context, ...any) (any, error) {
	e.doCalls++
	return nil, errors.New("generic execution should not be used")
}

func (e *integrationBulkStub) keyValueMGet(context.Context, []string) (any, error) {
	e.bulkCalls++
	if e.bulkErr != nil {
		return nil, e.bulkErr
	}
	return []any{[]byte("value")}, nil
}

func (e *integrationBulkStub) keyValueMSet(context.Context, []string, []any) (any, error) {
	e.bulkCalls++
	if e.bulkErr != nil {
		return nil, e.bulkErr
	}
	return []byte("OK"), nil
}

func (e *integrationBulkStub) keyValueMSetNX(context.Context, []string, []any) (any, error) {
	e.bulkCalls++
	if e.bulkErr != nil {
		return nil, e.bulkErr
	}
	return int64(1), nil
}

func TestIntegrationTrackingExecutorPreservesBulkPathAndRecordsOnlySuccess(t *testing.T) {
	integrationCommandCoverage.Lock()
	delete(integrationCommandCoverage.seen, "MGET")
	integrationCommandCoverage.Unlock()

	inner := &integrationBulkStub{}
	client := NewClientWithExecutor(&integrationTrackingExecutor{inner: inner})
	values, err := client.KV().MGet(context.Background(), "key")
	if err != nil || len(values) != 1 || asString(values[0]) != "value" {
		t.Fatalf("tracked MGET = %#v, %v", values, err)
	}
	if inner.bulkCalls != 1 || inner.doCalls != 0 {
		t.Fatalf("bulk calls = %d, generic calls = %d", inner.bulkCalls, inner.doCalls)
	}
	integrationCommandCoverage.Lock()
	_, recorded := integrationCommandCoverage.seen["MGET"]
	delete(integrationCommandCoverage.seen, "MGET")
	integrationCommandCoverage.Unlock()
	if !recorded {
		t.Fatal("successful bulk MGET was not recorded")
	}

	inner.bulkErr = errors.New("server rejected MGET")
	if _, err := client.KV().MGet(context.Background(), "key"); err == nil {
		t.Fatal("expected failed bulk MGET")
	}
	integrationCommandCoverage.Lock()
	_, recorded = integrationCommandCoverage.seen["MGET"]
	integrationCommandCoverage.Unlock()
	if recorded {
		t.Fatal("failed bulk MGET was counted as covered")
	}
}

func TestIntegrationCommandKeyTracksAffineAndGeoStoreDistVariants(t *testing.T) {
	if got := integrationCommandKey([]any{"COMMAND_EXEC", "GET", "key"}); got != "GET" {
		t.Fatalf("affine command key = %q; want GET", got)
	}
	if got := integrationCommandKey([]any{"GEOSEARCHSTORE", "dst", "src", "FROMMEMBER", "m", "BYRADIUS", 1, "km", "STOREDIST"}); got != "GEOSEARCHSTORE STOREDIST" {
		t.Fatalf("STOREDIST command key = %q", got)
	}
}

func shouldCheckIntegrationCommandCoverage() bool {
	if os.Getenv("FERRICSTORE_SKIP_COMMAND_COVERAGE") == "1" {
		return false
	}
	run := ""
	if flag := flag.Lookup("test.run"); flag != nil {
		run = flag.Value.String()
	}
	return run == ""
}

func missingIntegrationCommands() []string {
	integrationCommandCoverage.Lock()
	defer integrationCommandCoverage.Unlock()

	return missingIntegrationCommandsFrom(
		expectedIntegrationCommands(),
		integrationCommandCoverage.seen,
		integrationCommandCoverage.skipped,
		strictIntegrationCommandCoverage(),
	)
}

func missingIntegrationCommandsFrom(expected []string, seen map[string]struct{}, skipped map[string]string, strict bool) []string {
	var missing []string
	for _, command := range expected {
		if _, skipped := skipped[command]; skipped {
			if strict {
				missing = append(missing, command)
			}
			continue
		}
		if _, ok := seen[command]; !ok {
			missing = append(missing, command)
		}
	}
	sort.Strings(missing)
	return missing
}

func strictIntegrationCommandCoverage() bool {
	return os.Getenv("FERRICSTORE_STRICT_COMMAND_COVERAGE") == "1"
}

func expectedIntegrationCommands() []string {
	return []string{
		"ACL DELUSER",
		"ACL GETUSER",
		"ACL LIST",
		"ACL SAVE",
		"ACL SETUSER",
		"APPEND",
		"BF.ADD",
		"BF.CARD",
		"BF.EXISTS",
		"BF.INFO",
		"BF.MADD",
		"BF.MEXISTS",
		"BF.RESERVE",
		"BGSAVE",
		"BITCOUNT",
		"BITOP",
		"BITPOS",
		"BLMOVE",
		"BLMPOP",
		"BLPOP",
		"BRPOP",
		"CAS",
		"CF.ADD",
		"CF.ADDNX",
		"CF.COUNT",
		"CF.DEL",
		"CF.EXISTS",
		"CF.INFO",
		"CF.MEXISTS",
		"CF.RESERVE",
		"CLUSTER.DEMOTE",
		"CLUSTER.FAILOVER",
		"CLUSTER.HEALTH",
		"CLUSTER.JOIN",
		"CLUSTER.KEYSLOT",
		"CLUSTER.LEAVE",
		"CLUSTER.PROMOTE",
		"CLUSTER.ROLE",
		"CLUSTER.SLOTS",
		"CLUSTER.STATS",
		"CLUSTER.STATUS",
		"CLIENT INFO",
		"CLIENT SETNAME",
		"CMS.INCRBY",
		"CMS.INFO",
		"CMS.INITBYDIM",
		"CMS.INITBYPROB",
		"CMS.MERGE",
		"CMS.QUERY",
		"COMMAND COUNT",
		"COMMAND DOCS",
		"COMMAND GETKEYS",
		"COMMAND INFO",
		"COMMAND LIST",
		"CONFIG GET",
		"CONFIG RESETSTAT",
		"CONFIG REWRITE",
		"CONFIG SET",
		"COPY",
		"DBSIZE",
		"DECR",
		"DECRBY",
		"DEL",
		"ECHO",
		"EXISTS",
		"EXPIRE",
		"EXPIREAT",
		"EXPIRETIME",
		"EXTEND",
		"FERRICSTORE.BLOBGC",
		"FERRICSTORE.CONFIG",
		"FERRICSTORE.DOCTOR",
		"FERRICSTORE.HOTNESS",
		"FERRICSTORE.KEY_INFO",
		"FERRICSTORE.METRICS",
		"FETCH_OR_COMPUTE",
		"FETCH_OR_COMPUTE_ERROR",
		"FETCH_OR_COMPUTE_RESULT",
		"FLOW.APPROVAL.APPROVE",
		"FLOW.APPROVAL.GET",
		"FLOW.APPROVAL.LIST",
		"FLOW.APPROVAL.REJECT",
		"FLOW.APPROVAL.REQUEST",
		"FLOW.ATTRIBUTES",
		"FLOW.ATTRIBUTE_VALUES",
		"FLOW.BUDGET.COMMIT",
		"FLOW.BUDGET.GET",
		"FLOW.BUDGET.LIST",
		"FLOW.BUDGET.RELEASE",
		"FLOW.BUDGET.RESERVE",
		"FLOW.BY_CORRELATION",
		"FLOW.BY_PARENT",
		"FLOW.BY_ROOT",
		"FLOW.CANCEL",
		"FLOW.CANCEL_MANY",
		"FLOW.CLAIM_DUE",
		"FLOW.COMPLETE",
		"FLOW.COMPLETE_MANY",
		"FLOW.CREATE",
		"FLOW.CREATE_MANY",
		"FLOW.CIRCUIT.CLOSE",
		"FLOW.CIRCUIT.GET",
		"FLOW.CIRCUIT.OPEN",
		"FLOW.EFFECT.COMPENSATE",
		"FLOW.EFFECT.CONFIRM",
		"FLOW.EFFECT.FAIL",
		"FLOW.EFFECT.GET",
		"FLOW.EFFECT.RESERVE",
		"FLOW.EXTEND_LEASE",
		"FLOW.FAIL",
		"FLOW.FAIL_MANY",
		"FLOW.FAILURES",
		"FLOW.GET",
		"FLOW.GOVERNANCE.LEDGER",
		"FLOW.GOVERNANCE.OVERVIEW",
		"FLOW.HISTORY",
		"FLOW.INFO",
		"FLOW.LIST",
		"FLOW.LIMIT.GET",
		"FLOW.LIMIT.LEASE",
		"FLOW.LIMIT.LIST",
		"FLOW.LIMIT.RELEASE",
		"FLOW.LIMIT.SPEND",
		"FLOW.POLICY.GET",
		"FLOW.POLICY.SET",
		"FLOW.RECLAIM",
		"FLOW.RETENTION_CLEANUP",
		"FLOW.RETRY",
		"FLOW.RETRY_MANY",
		"FLOW.REWIND",
		"FLOW.RUN_STEPS_MANY",
		"FLOW.SCHEDULE.CREATE",
		"FLOW.SCHEDULE.DELETE",
		"FLOW.SCHEDULE.FIRE",
		"FLOW.SCHEDULE.FIRE_DUE",
		"FLOW.SCHEDULE.GET",
		"FLOW.SCHEDULE.LIST",
		"FLOW.SCHEDULE.PAUSE",
		"FLOW.SCHEDULE.RESUME",
		"FLOW.SEARCH",
		"FLOW.SIGNAL",
		"FLOW.SPAWN_CHILDREN",
		"FLOW.START_AND_CLAIM",
		"FLOW.STATS",
		"FLOW.STEP_CONTINUE",
		"FLOW.STUCK",
		"FLOW.TERMINALS",
		"FLOW.TRANSITION",
		"FLOW.TRANSITION_MANY",
		"FLOW.VALUE.MGET",
		"FLOW.VALUE.PUT",
		"FLUSHALL",
		"FLUSHDB",
		"GEOADD",
		"GEODIST",
		"GEOHASH",
		"GEOPOS",
		"GEOSEARCH",
		"GEOSEARCHSTORE",
		"GEOSEARCHSTORE STOREDIST",
		"GET",
		"GETBIT",
		"GETDEL",
		"GETEX",
		"GETRANGE",
		"GETSET",
		"HDEL",
		"HEXISTS",
		"HEXPIRE",
		"HEXPIRETIME",
		"HGET",
		"HGETALL",
		"HGETDEL",
		"HGETEX",
		"HINCRBY",
		"HINCRBYFLOAT",
		"HKEYS",
		"HLEN",
		"HMGET",
		"HPERSIST",
		"HPEXPIRE",
		"HPEXPIRETIME",
		"HPTTL",
		"HRANDFIELD",
		"HSCAN",
		"HSET",
		"HSETEX",
		"HSETNX",
		"HSTRLEN",
		"HTTL",
		"HVALS",
		"INCR",
		"INCRBY",
		"INCRBYFLOAT",
		"INFO",
		"KEYS",
		"LASTSAVE",
		"LINDEX",
		"LINSERT",
		"LLEN",
		"LMOVE",
		"LOCK",
		"LPOP",
		"LPOS",
		"LPUSH",
		"LRANGE",
		"LREM",
		"LSET",
		"LTRIM",
		"MEMORY USAGE",
		"MGET",
		"MODULE LIST",
		"MSET",
		"MSETNX",
		"OBJECT ENCODING",
		"OBJECT HELP",
		"OBJECT REFCOUNT",
		"PERSIST",
		"PEXPIRE",
		"PEXPIREAT",
		"PEXPIRETIME",
		"PFADD",
		"PFCOUNT",
		"PFMERGE",
		"PING",
		"PSETEX",
		"PTTL",
		"PSUBSCRIBE",
		"PUBLISH",
		"PUNSUBSCRIBE",
		"PUBSUB CHANNELS",
		"PUBSUB NUMPAT",
		"PUBSUB NUMSUB",
		"RANDOMKEY",
		"RATELIMIT.ADD",
		"RENAME",
		"RENAMENX",
		"RPOP",
		"RPOPLPUSH",
		"RPUSH",
		"SADD",
		"SAVE",
		"SCAN",
		"SCARD",
		"SDIFF",
		"SDIFFSTORE",
		"SELECT",
		"SET",
		"SETBIT",
		"SETEX",
		"SETNX",
		"SETRANGE",
		"SINTER",
		"SINTERCARD",
		"SINTERSTORE",
		"SISMEMBER",
		"SLOWLOG GET",
		"SLOWLOG LEN",
		"SLOWLOG RESET",
		"SMEMBERS",
		"SMISMEMBER",
		"SMOVE",
		"SPOP",
		"SRANDMEMBER",
		"SREM",
		"SSCAN",
		"STRLEN",
		"SUNION",
		"SUNIONSTORE",
		"SUBSCRIBE",
		"SUBSCRIBE_EVENTS",
		"TDIGEST.ADD",
		"TDIGEST.BYRANK",
		"TDIGEST.BYREVRANK",
		"TDIGEST.CDF",
		"TDIGEST.CREATE",
		"TDIGEST.INFO",
		"TDIGEST.MAX",
		"TDIGEST.MERGE",
		"TDIGEST.MIN",
		"TDIGEST.QUANTILE",
		"TDIGEST.RANK",
		"TDIGEST.RESET",
		"TDIGEST.REVRANK",
		"TDIGEST.TRIMMED_MEAN",
		"TOPK.ADD",
		"TOPK.COUNT",
		"TOPK.INCRBY",
		"TOPK.INFO",
		"TOPK.LIST",
		"TOPK.QUERY",
		"TOPK.RESERVE",
		"TTL",
		"TYPE",
		"UNLINK",
		"UNLOCK",
		"UNSUBSCRIBE",
		"UNSUBSCRIBE_EVENTS",
		"WAIT",
		"WAITAOF",
		"XACK",
		"XADD",
		"XDEL",
		"XGROUP CREATE",
		"XINFO STREAM",
		"XLEN",
		"XRANGE",
		"XREAD",
		"XREADGROUP",
		"XREVRANGE",
		"XTRIM",
		"ZADD",
		"ZCARD",
		"ZCOUNT",
		"ZINCRBY",
		"ZMSCORE",
		"ZPOPMAX",
		"ZPOPMIN",
		"ZRANDMEMBER",
		"ZRANGE",
		"ZRANGEBYSCORE",
		"ZRANK",
		"ZREM",
		"ZREVRANGE",
		"ZREVRANGEBYSCORE",
		"ZREVRANK",
		"ZSCAN",
		"ZSCORE",
	}
}
