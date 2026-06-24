//go:build integration

package ferricstore

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
)

var integrationCommandCoverage = struct {
	sync.Mutex
	seen map[string]struct{}
}{
	seen: map[string]struct{}{},
}

type integrationTrackingExecutor struct {
	inner Executor
}

func (e *integrationTrackingExecutor) Do(ctx context.Context, args ...any) (any, error) {
	recordIntegrationCommand(args)
	return e.inner.Do(ctx, args...)
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

func integrationCommandKey(args []any) string {
	if len(args) == 0 {
		return ""
	}
	command := strings.ToUpper(asString(args[0]))
	switch command {
	case "COMMAND", "CONFIG", "MEMORY", "MODULE", "OBJECT", "PUBSUB", "SLOWLOG", "XGROUP", "XINFO":
		if len(args) > 1 {
			return command + " " + strings.ToUpper(asString(args[1]))
		}
	}
	return command
}

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

	var missing []string
	for _, command := range expectedIntegrationCommands() {
		if _, ok := integrationCommandCoverage.seen[command]; !ok {
			missing = append(missing, command)
		}
	}
	sort.Strings(missing)
	return missing
}

func expectedIntegrationCommands() []string {
	return []string{
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
		"FLOW.EXTEND_LEASE",
		"FLOW.FAIL",
		"FLOW.FAIL_MANY",
		"FLOW.FAILURES",
		"FLOW.GET",
		"FLOW.HISTORY",
		"FLOW.INFO",
		"FLOW.LIST",
		"FLOW.POLICY.GET",
		"FLOW.POLICY.SET",
		"FLOW.RECLAIM",
		"FLOW.RETENTION_CLEANUP",
		"FLOW.RETRY",
		"FLOW.RETRY_MANY",
		"FLOW.REWIND",
		"FLOW.SIGNAL",
		"FLOW.SPAWN_CHILDREN",
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
		"JSON.DEL",
		"JSON.GET",
		"JSON.SET",
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
		"PUBLISH",
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
