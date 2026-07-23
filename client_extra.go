package ferricstore

import (
	"context"
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

func (c *Client) Scan(ctx context.Context, cursor int64, match string, count *int) (any, error) {
	return c.scanCursor(ctx, cursor, match, count)
}

// ScanCursor continues SCAN using an opaque cursor returned by FerricStore.
// It accepts integer cursors as well as server cursor strings and byte slices.
func (c *Client) ScanCursor(ctx context.Context, cursor any, match string, count *int) (any, error) {
	return c.scanCursor(ctx, cursor, match, count)
}

func (c *Client) scanCursor(ctx context.Context, cursor any, match string, count *int) (any, error) {
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
	value, err := c.typedReply(ctx, args...)
	return decodeKeyScan(value, err)
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
	return nonNegativeInt64Response("COMMAND COUNT", value, err)
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
	return clientInfoResponse(value)
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
	return boundedCountResponse("ACL DELUSER", 1, value, err)
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
	return normalizedAdminMap(value)
}

func (c *Client) EnsureNamespace(ctx context.Context, prefix string, attrs map[string]any) (any, error) {
	args := []any{"FERRICSTORE.NAMESPACE", "ENSURE", prefix}
	pairs, err := managementPairArgs(attrs)
	if err != nil {
		return nil, err
	}
	args = append(args, pairs...)
	value, err := c.typedReply(ctx, args...)
	return normalizeAdminResult(value, err)
}

func (c *Client) GetNamespace(ctx context.Context, prefix string) (any, error) {
	value, err := c.typedReply(ctx, "FERRICSTORE.NAMESPACE", "GET", prefix)
	return normalizeAdminResult(value, err)
}

func (c *Client) ListNamespaces(ctx context.Context) (any, error) {
	value, err := c.typedReply(ctx, "FERRICSTORE.NAMESPACE", "LIST")
	return normalizeAdminResult(value, err)
}

func (c *Client) DeleteNamespace(ctx context.Context, prefix string) (any, error) {
	value, err := c.typedReply(ctx, "FERRICSTORE.NAMESPACE", "DELETE", prefix)
	return normalizeAdminResult(value, err)
}

func (c *Client) SetQuota(ctx context.Context, namespace string, quotaSpec map[string]any) (any, error) {
	args := []any{"FERRICSTORE.QUOTA", "SET", namespace}
	pairs, err := managementPairArgs(quotaSpec)
	if err != nil {
		return nil, err
	}
	args = append(args, pairs...)
	value, err := c.typedReply(ctx, args...)
	return normalizeAdminResult(value, err)
}

func (c *Client) GetQuota(ctx context.Context, namespace string) (any, error) {
	value, err := c.typedReply(ctx, "FERRICSTORE.QUOTA", "GET", namespace)
	return normalizeAdminResult(value, err)
}

func (c *Client) QuotaUsage(ctx context.Context, namespace string) (any, error) {
	value, err := c.typedReply(ctx, "FERRICSTORE.QUOTA", "USAGE", namespace)
	return normalizeAdminResult(value, err)
}

func (c *Client) ClusterInfo(ctx context.Context) (map[string]any, error) {
	value, err := c.typedReply(ctx, "FERRICSTORE.TELEMETRY", "CLUSTER_INFO")
	if err != nil {
		return nil, err
	}
	return normalizedAdminMap(value)
}

func (c *Client) NamespaceUsage(ctx context.Context, prefix string) (map[string]any, error) {
	value, err := c.typedReply(ctx, "FERRICSTORE.TELEMETRY", "NAMESPACE_USAGE", prefix)
	if err != nil {
		return nil, err
	}
	return normalizedAdminMap(value)
}

func (c *Client) TelemetryFlowQuery(ctx context.Context, attrs map[string]any) ([]any, error) {
	args := []any{"FERRICSTORE.TELEMETRY", "FLOW_QUERY"}
	pairs, err := managementPairArgs(attrs)
	if err != nil {
		return nil, err
	}
	args = append(args, pairs...)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	return adminArrayResponse(value)
}

func (c *Client) FlowHistory(ctx context.Context, id string, attrs map[string]any) ([]any, error) {
	args := []any{"FERRICSTORE.TELEMETRY", "FLOW_HISTORY", id}
	pairs, err := managementPairArgs(attrs)
	if err != nil {
		return nil, err
	}
	args = append(args, pairs...)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	return adminArrayResponse(value)
}

func managementPairArgs(pairs map[string]any) ([]any, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	values := make(map[string]any, len(pairs))
	for key, value := range pairs {
		if value == nil {
			continue
		}
		key = strings.ToUpper(strings.TrimSpace(key))
		if key == "" {
			return nil, errors.New("management option name must be non-empty")
		}
		if _, exists := values[key]; exists {
			return nil, fmt.Errorf("management option %q is duplicated after normalization", key)
		}
		values[key] = value
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	args := make([]any, 0, len(keys)*2)
	for _, key := range keys {
		args = append(args, key, values[key])
	}
	return args, nil
}
