package ferricstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

func (c *Client) Type(ctx context.Context, key string) (string, error) {
	value, err := c.typedReply(ctx, "TYPE", key)
	return responseString(value, err)
}

func (c *Client) RandomKey(ctx context.Context) (string, error) {
	value, err := c.typedReply(ctx, "RANDOMKEY")
	return responseString(value, err)
}

func (c *Client) Scan(ctx context.Context, cursor any, match string, count *int) (any, error) {
	normalizedCursor, err := normalizeScanCursor(cursor, false)
	if err != nil {
		return nil, err
	}
	if err := validateScanCount(count); err != nil {
		return nil, err
	}
	args := []any{"SCAN", normalizedCursor}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	appendScanCount(&args, count)
	return c.typedReply(ctx, args...)
}

func (c *Client) DBSize(ctx context.Context) (int64, error) {
	value, err := c.typedReply(ctx, "DBSIZE")
	return nonNegativeInt64Response("DBSIZE", value, err)
}

func (c *Client) Keys(ctx context.Context, pattern string) ([]string, error) {
	value, err := c.typedReply(ctx, "KEYS", pattern)
	return stringArray(value, err)
}

func (c *Client) Unlink(ctx context.Context, keys ...string) (int64, error) {
	return c.keyWrite(ctx, "UNLINK", keys...)
}

func (c *Client) Delete(ctx context.Context, keys ...string) (int64, error) {
	return c.keyWrite(ctx, "DEL", keys...)
}

func (c *Client) keyWrite(ctx context.Context, command string, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	args := []any{command}
	for _, key := range keys {
		args = append(args, key)
	}
	value, err := c.typedReply(ctx, args...)
	return keyValueCountResponse(command, len(keys), value, err)
}

func (c *Client) Rename(ctx context.Context, key, newKey string) error {
	return c.typedStatus(ctx, "RENAME", key, newKey)
}

func (c *Client) RenameNX(ctx context.Context, key, newKey string) (bool, error) {
	value, err := c.typedReply(ctx, "RENAMENX", key, newKey)
	return responseBool(value, err)
}

func (c *Client) Copy(ctx context.Context, source, destination string, replace bool) (bool, error) {
	args := []any{"COPY", source, destination}
	if replace {
		args = append(args, "REPLACE")
	}
	value, err := c.typedReply(ctx, args...)
	return responseBool(value, err)
}

func (c *Client) Ping(ctx context.Context, message ...string) (string, error) {
	if len(message) > 1 {
		return "", errors.New("PING accepts at most one message")
	}
	args := []any{"PING"}
	if len(message) > 0 {
		args = append(args, message[0])
	}
	value, err := c.typedReply(ctx, args...)
	return responseString(value, err)
}

func (c *Client) Echo(ctx context.Context, message string) (string, error) {
	value, err := c.typedReply(ctx, "ECHO", message)
	return responseString(value, err)
}

func (c *Client) ServerInfo(ctx context.Context, section ...string) (map[string]any, error) {
	if len(section) > 1 {
		return nil, errors.New("INFO accepts at most one section")
	}
	args := []any{"INFO"}
	if len(section) > 0 && section[0] != "" {
		args = append(args, section[0])
	}
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	return kvResponse(value)
}

func (c *Client) FlushDB(ctx context.Context, mode string) error {
	args := []any{"FLUSHDB"}
	if mode != "" {
		args = append(args, mode)
	}
	return c.typedStatus(ctx, args...)
}

func (c *Client) FlushAll(ctx context.Context, mode string) error {
	args := []any{"FLUSHALL"}
	if mode != "" {
		args = append(args, mode)
	}
	return c.typedStatus(ctx, args...)
}

func (c *Client) CommandInfo(ctx context.Context, names ...string) (any, error) {
	args := []any{"COMMAND", "INFO"}
	for _, name := range names {
		args = append(args, name)
	}
	return c.typedReply(ctx, args...)
}

func (c *Client) CommandCount(ctx context.Context) (int64, error) {
	value, err := c.typedReply(ctx, "COMMAND", "COUNT")
	return responseInt64(value, err)
}

func (c *Client) CommandList(ctx context.Context) ([]string, error) {
	value, err := c.typedReply(ctx, "COMMAND", "LIST")
	return stringArray(value, err)
}

func (c *Client) CommandDocs(ctx context.Context, names ...string) (any, error) {
	args := []any{"COMMAND", "DOCS"}
	for _, name := range names {
		args = append(args, name)
	}
	return c.typedReply(ctx, args...)
}

func (c *Client) CommandGetKeys(ctx context.Context, command ...any) (any, error) {
	args := []any{"COMMAND", "GETKEYS"}
	args = append(args, command...)
	return c.typedReply(ctx, args...)
}

func (c *Client) ConfigGet(ctx context.Context, pattern string) (any, error) {
	return c.typedReply(ctx, "CONFIG", "GET", pattern)
}

func (c *Client) ConfigSet(ctx context.Context, parameter, value string) error {
	return c.typedStatus(ctx, "CONFIG", "SET", parameter, value)
}

func (c *Client) ConfigResetStat(ctx context.Context) error {
	return c.typedStatus(ctx, "CONFIG", "RESETSTAT")
}

func (c *Client) ConfigRewrite(ctx context.Context) error {
	return c.typedStatus(ctx, "CONFIG", "REWRITE")
}

func (c *Client) ClientSetName(ctx context.Context, name string) error {
	return c.typedStatus(ctx, "CLIENT", "SETNAME", name)
}

func (c *Client) ClientInfo(ctx context.Context) (map[string]any, error) {
	value, err := c.typedReply(ctx, "CLIENT", "INFO")
	if err != nil {
		return nil, err
	}
	return kvResponse(value)
}

func (c *Client) ACL(ctx context.Context, subcommand string, args ...any) (any, error) {
	command := []any{"ACL", subcommand}
	command = append(command, args...)
	return c.typedReply(ctx, command...)
}

func (c *Client) ACLSetUser(ctx context.Context, username string, rules ...string) error {
	args := make([]any, 0, 3+len(rules))
	args = append(args, "ACL", "SETUSER", username)
	for _, rule := range rules {
		args = append(args, rule)
	}
	return c.typedStatus(ctx, args...)
}

func (c *Client) ACLDelUser(ctx context.Context, username string) (int64, error) {
	value, err := c.ACL(ctx, "DELUSER", username)
	return responseInt64(value, err)
}

func (c *Client) ACLGetUser(ctx context.Context, username string) (map[string]any, error) {
	value, err := c.ACL(ctx, "GETUSER", username)
	if err != nil {
		return nil, err
	}
	return nativeMap(value)
}

func (c *Client) ACLList(ctx context.Context) ([]string, error) {
	value, err := c.ACL(ctx, "LIST")
	return stringArray(value, err)
}

func (c *Client) ACLSave(ctx context.Context) error {
	return c.typedStatus(ctx, "ACL", "SAVE")
}

func (c *Client) ACLWhoAmI(ctx context.Context) (string, error) {
	value, err := c.ACL(ctx, "WHOAMI")
	return responseString(value, err)
}

func (c *Client) ACLLoad(ctx context.Context) error {
	return c.typedStatus(ctx, "ACL", "LOAD")
}

func (c *Client) Capabilities(ctx context.Context) (map[string]any, error) {
	value, err := c.typedReply(ctx, "FERRICSTORE.CAPABILITIES")
	if err != nil {
		return nil, err
	}
	return nativeMap(normalizeAdminResponse(value))
}

func (c *Client) EnsureNamespace(ctx context.Context, prefix string, attrs map[string]any) (any, error) {
	args := []any{"FERRICSTORE.NAMESPACE", "ENSURE", prefix}
	args = append(args, managementPairArgs(attrs)...)
	value, err := c.typedReply(ctx, args...)
	return normalizeAdminResponse(value), err
}

func (c *Client) GetNamespace(ctx context.Context, prefix string) (any, error) {
	value, err := c.typedReply(ctx, "FERRICSTORE.NAMESPACE", "GET", prefix)
	return normalizeAdminResponse(value), err
}

func (c *Client) ListNamespaces(ctx context.Context) (any, error) {
	value, err := c.typedReply(ctx, "FERRICSTORE.NAMESPACE", "LIST")
	return normalizeAdminResponse(value), err
}

func (c *Client) DeleteNamespace(ctx context.Context, prefix string) (any, error) {
	value, err := c.typedReply(ctx, "FERRICSTORE.NAMESPACE", "DELETE", prefix)
	return normalizeAdminResponse(value), err
}

func (c *Client) SetQuota(ctx context.Context, namespace string, quotaSpec map[string]any) (any, error) {
	args := []any{"FERRICSTORE.QUOTA", "SET", namespace}
	args = append(args, managementPairArgs(quotaSpec)...)
	value, err := c.typedReply(ctx, args...)
	return normalizeAdminResponse(value), err
}

func (c *Client) GetQuota(ctx context.Context, namespace string) (any, error) {
	value, err := c.typedReply(ctx, "FERRICSTORE.QUOTA", "GET", namespace)
	return normalizeAdminResponse(value), err
}

func (c *Client) QuotaUsage(ctx context.Context, namespace string) (any, error) {
	value, err := c.typedReply(ctx, "FERRICSTORE.QUOTA", "USAGE", namespace)
	return normalizeAdminResponse(value), err
}

func (c *Client) ClusterInfo(ctx context.Context) (map[string]any, error) {
	value, err := c.typedReply(ctx, "FERRICSTORE.TELEMETRY", "CLUSTER_INFO")
	if err != nil {
		return nil, err
	}
	return nativeMap(normalizeAdminResponse(value))
}

func (c *Client) NamespaceUsage(ctx context.Context, prefix string) (map[string]any, error) {
	value, err := c.typedReply(ctx, "FERRICSTORE.TELEMETRY", "NAMESPACE_USAGE", prefix)
	if err != nil {
		return nil, err
	}
	return nativeMap(normalizeAdminResponse(value))
}

func (c *Client) FlowQuery(ctx context.Context, attrs map[string]any) ([]any, error) {
	args := []any{"FERRICSTORE.TELEMETRY", "FLOW_QUERY"}
	args = append(args, managementPairArgs(attrs)...)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	return adminArrayResponse(value)
}

func (c *Client) FlowHistory(ctx context.Context, id string, attrs map[string]any) ([]any, error) {
	args := []any{"FERRICSTORE.TELEMETRY", "FLOW_HISTORY", id}
	args = append(args, managementPairArgs(attrs)...)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	return adminArrayResponse(value)
}

func (c *Client) InvocationDefinitionPut(ctx context.Context, definition any, opt RequestContextOptions) (any, error) {
	definitionArg, err := jsonCommandArg(definition)
	if err != nil {
		return nil, err
	}
	value, err := c.typedReply(ctx, commandWithRequestContext("INVOCATION.DEFINITION.PUT", []any{definitionArg}, opt.RequestContext)...)
	return normalizeAdminResponse(value), err
}

func (c *Client) InvocationDefinitionGet(ctx context.Context, name string, opt RequestContextOptions) (any, error) {
	value, err := c.typedReply(ctx, commandWithRequestContext("INVOCATION.DEFINITION.GET", []any{name}, opt.RequestContext)...)
	return normalizeAdminResponse(value), err
}

func (c *Client) InvocationDefinitionList(ctx context.Context, opt RequestContextOptions) ([]any, error) {
	value, err := c.typedReply(ctx, commandWithRequestContext("INVOCATION.DEFINITION.LIST", nil, opt.RequestContext)...)
	if err != nil {
		return nil, err
	}
	return adminArrayResponse(value)
}

func (c *Client) InvocationCreate(ctx context.Context, name string, attrs map[string]any, opt InvocationCreateOptions) (any, error) {
	envelope := map[string]any{"attrs": attrs}
	if opt.Context != nil {
		envelope["context"] = opt.Context
	}
	if opt.IdempotencyKey != "" {
		envelope["idempotency_key"] = opt.IdempotencyKey
	}
	envelopeArg, err := jsonCommandArg(envelope)
	if err != nil {
		return nil, err
	}
	value, err := c.typedReply(ctx, commandWithRequestContext("INVOCATION.CREATE", []any{name, envelopeArg}, opt.RequestContext)...)
	return normalizeAdminResponse(value), err
}

func (c *Client) InvocationGet(ctx context.Context, id string, opt RequestContextOptions) (any, error) {
	value, err := c.typedReply(ctx, commandWithRequestContext("INVOCATION.GET", []any{id}, opt.RequestContext)...)
	return normalizeAdminResponse(value), err
}

func (c *Client) InvocationPartitionList(ctx context.Context, name string, opt InvocationPartitionListOptions) ([]any, error) {
	args := []any{name}
	appendOpt(&args, "SCOPE", opt.Scope)
	value, err := c.typedReply(ctx, commandWithRequestContext("INVOCATION.PARTITION.LIST", args, opt.RequestContext)...)
	if err != nil {
		return nil, err
	}
	return adminArrayResponse(value)
}

func managementPairArgs(pairs map[string]any) []any {
	if len(pairs) == 0 {
		return nil
	}
	keys := make([]string, 0, len(pairs))
	for key, value := range pairs {
		if value != nil {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		return strings.ToUpper(keys[i]) < strings.ToUpper(keys[j])
	})
	args := make([]any, 0, len(keys)*2)
	for _, key := range keys {
		args = append(args, strings.ToUpper(key), pairs[key])
	}
	return args
}

func commandWithRequestContext(command string, args []any, requestContext *RequestContext) []any {
	out := []any{command}
	out = append(out, args...)
	if requestContext != nil {
		out = appendNativeRequestContext(out, requestContext)
	}
	return out
}

func jsonCommandArg(value any) (string, error) {
	if text, ok := value.(string); ok {
		return text, nil
	}
	if bytes, ok := value.([]byte); ok {
		return string(bytes), nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func normalizeAdminResponse(value any) any {
	switch v := value.(type) {
	case []byte:
		return string(v)
	case []any:
		out := make([]any, len(v))
		for index, item := range v {
			out[index] = normalizeAdminResponse(item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = normalizeAdminResponse(item)
		}
		return out
	case map[interface{}]interface{}:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[asString(key)] = normalizeAdminResponse(item)
		}
		return out
	default:
		return value
	}
}

func adminArrayResponse(value any) ([]any, error) {
	value = normalizeAdminResponse(value)
	if value == nil {
		return nil, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected admin array response, got %T", value)
	}
	return append([]any(nil), items...), nil
}

func (c *Client) SlowLogGet(ctx context.Context, count *int) (any, error) {
	args := []any{"SLOWLOG", "GET"}
	if count != nil {
		args = append(args, *count)
	}
	return c.typedReply(ctx, args...)
}

func (c *Client) SlowLogLen(ctx context.Context) (int64, error) {
	value, err := c.typedReply(ctx, "SLOWLOG", "LEN")
	return responseInt64(value, err)
}

func (c *Client) SlowLogReset(ctx context.Context) error {
	return c.typedStatus(ctx, "SLOWLOG", "RESET")
}

func (c *Client) Select(ctx context.Context, db int) error {
	return c.typedStatus(ctx, "SELECT", db)
}

func (c *Client) Wait(ctx context.Context, replicas, timeoutMS int64) (int64, error) {
	value, err := c.typedReply(ctx, "WAIT", replicas, timeoutMS)
	return responseInt64(value, err)
}

func (c *Client) WaitAOF(ctx context.Context, local, replicas, timeoutMS int64) (any, error) {
	return c.typedReply(ctx, "WAITAOF", local, replicas, timeoutMS)
}

func (c *Client) Object(ctx context.Context, args ...any) (any, error) {
	command := append([]any{"OBJECT"}, args...)
	return c.typedReply(ctx, command...)
}

func (c *Client) ObjectRefCount(ctx context.Context, key string) (int64, error) {
	value, err := c.typedReply(ctx, "OBJECT", "REFCOUNT", key)
	return responseInt64(value, err)
}

func (c *Client) ObjectHelp(ctx context.Context) (any, error) {
	return c.typedReply(ctx, "OBJECT", "HELP")
}

func (c *Client) Publish(ctx context.Context, channel, message string) (int64, error) {
	value, err := c.typedReply(ctx, "PUBLISH", channel, message)
	return responseInt64(value, err)
}

func (c *Client) Subscribe(ctx context.Context, channels ...string) (any, error) {
	args := []any{"SUBSCRIBE"}
	for _, channel := range channels {
		args = append(args, channel)
	}
	return c.typedReply(ctx, args...)
}

func (c *Client) Unsubscribe(ctx context.Context, channels ...string) (any, error) {
	args := []any{"UNSUBSCRIBE"}
	for _, channel := range channels {
		args = append(args, channel)
	}
	return c.typedReply(ctx, args...)
}

func (c *Client) PSubscribe(ctx context.Context, patterns ...string) (any, error) {
	args := []any{"PSUBSCRIBE"}
	for _, pattern := range patterns {
		args = append(args, pattern)
	}
	return c.typedReply(ctx, args...)
}

func (c *Client) PUnsubscribe(ctx context.Context, patterns ...string) (any, error) {
	args := []any{"PUNSUBSCRIBE"}
	for _, pattern := range patterns {
		args = append(args, pattern)
	}
	return c.typedReply(ctx, args...)
}

func (c *Client) PubSubChannels(ctx context.Context, pattern string) ([]string, error) {
	args := []any{"PUBSUB", "CHANNELS"}
	if pattern != "" {
		args = append(args, pattern)
	}
	value, err := c.typedReply(ctx, args...)
	return stringArray(value, err)
}

func (c *Client) PubSubNumSub(ctx context.Context, channels ...string) (map[string]int64, error) {
	args := []any{"PUBSUB", "NUMSUB"}
	for _, channel := range channels {
		args = append(args, channel)
	}
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected PUBSUB NUMSUB response array, got %T", value)
	}
	if len(items)%2 != 0 {
		return nil, fmt.Errorf("expected PUBSUB NUMSUB channel/count pairs, got %d items", len(items))
	}
	out := make(map[string]int64, len(items)/2)
	for i := 0; i < len(items); i += 2 {
		if items[i] == nil {
			return nil, fmt.Errorf("invalid PUBSUB NUMSUB channel at pair %d: response is nil", i/2)
		}
		channel, err := responseString(items[i], nil)
		if err != nil {
			return nil, fmt.Errorf("invalid PUBSUB NUMSUB channel at pair %d: %w", i/2, err)
		}
		count, err := responseInt64(items[i+1], nil)
		if err != nil {
			return nil, fmt.Errorf("invalid PUBSUB NUMSUB count for %q: %w", channel, err)
		}
		out[channel] = count
	}
	return out, nil
}

func (c *Client) PubSubNumPat(ctx context.Context) (int64, error) {
	value, err := c.typedReply(ctx, "PUBSUB", "NUMPAT")
	return responseInt64(value, err)
}

func (c *Client) Save(ctx context.Context) error {
	return c.typedStatus(ctx, "SAVE")
}

func (c *Client) BgSave(ctx context.Context) error {
	return c.typedExpectedStatus(ctx, "Background saving started", "BGSAVE")
}

func (c *Client) LastSave(ctx context.Context) (int64, error) {
	value, err := c.typedReply(ctx, "LASTSAVE")
	return responseInt64(value, err)
}

func (c *Client) Memory(ctx context.Context, args ...any) (any, error) {
	command := append([]any{"MEMORY"}, args...)
	return c.typedReply(ctx, command...)
}

func (c *Client) Module(ctx context.Context, args ...any) (any, error) {
	command := append([]any{"MODULE"}, args...)
	return c.typedReply(ctx, command...)
}

func (c *Client) FerricStoreDoctor(ctx context.Context, args ...any) (any, error) {
	command := append([]any{"FERRICSTORE.DOCTOR"}, args...)
	return c.typedReply(ctx, command...)
}
