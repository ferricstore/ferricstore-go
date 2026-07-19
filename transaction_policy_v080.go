package ferricstore

import (
	"fmt"
	"strings"
)

// FerricStore 0.8 executes MULTI queues inside one shard-local transaction.
// Request/process-scoped commands cannot participate in that transaction and
// are rejected by the server. Mirror that stable contract before touching the
// affine connection so a local mistake does not poison the active MULTI.
func validateV080TransactionCommand(args []any) error {
	args = topologyCommandArgs(args)
	if len(args) == 0 {
		return fmt.Errorf("empty command is not supported inside FerricStore 0.8 transactions")
	}
	name := commandName(args)
	if v080TransactionLocalNoKeyCommand(name) {
		return nil
	}
	if v080TransactionRequestCommand(name) {
		return fmt.Errorf("%s is not supported inside FerricStore 0.8 transactions", name)
	}
	if _, keyRouted := topologyCommandPolicies[name]; keyRouted {
		return nil
	}
	return fmt.Errorf("%s is not supported inside FerricStore 0.8 transactions", name)
}

func v080TransactionLocalNoKeyCommand(name string) bool {
	switch name {
	case "PING", "ECHO", "CLUSTER.KEYSLOT":
		return true
	default:
		return false
	}
}

func v080TransactionRequestCommand(name string) bool {
	for _, prefix := range []string{"BF.", "CF.", "CMS.", "TOPK.", "TDIGEST.", "FLOW."} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	switch name {
	case "AUTH", "HELLO", "CLIENT", "QUIT", "RESET", "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH",
		"SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PUBLISH", "PUBSUB",
		"BLPOP", "BRPOP", "BLMOVE", "BLMPOP",
		"XADD", "XLEN", "XRANGE", "XREVRANGE", "XREAD", "XTRIM", "XDEL", "XINFO", "XGROUP", "XREADGROUP", "XACK",
		"KEY_INFO", "FERRICSTORE.KEY_INFO", "CAS", "LOCK", "UNLOCK", "EXTEND", "RATELIMIT.ADD",
		"FETCH_OR_COMPUTE", "FETCH_OR_COMPUTE_RESULT", "FETCH_OR_COMPUTE_ERROR",
		"HRANDFIELD", "SRANDMEMBER", "SPOP", "ZRANDMEMBER", "RANDOMKEY", "ROUTE", "ROUTE_BATCH":
		return true
	default:
		return false
	}
}
