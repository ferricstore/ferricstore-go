//go:build integration

package ferricstore

import (
	"strings"
	"testing"
)

func TestIntegrationAdminMetadataAndExpectedErrors(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(StringCodec{})
	defer client.Close()

	runID := integrationSuffix("admin")
	prefix := "go-sdk:admin:" + runID + ":"
	key := prefix + "key"

	if err := client.KV().Set(ctx, key, "value"); err != nil {
		t.Fatal(err)
	}
	requireValue(t, must[any](t)(client.ConfigGet(ctx, "*")))
	if err := client.ConfigSet(ctx, "loglevel", "notice"); err != nil {
		t.Fatal(err)
	}
	if err := client.ConfigResetStat(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.ConfigRewrite(ctx); err != nil {
		t.Fatal(err)
	}
	requireValue(t, must[any](t)(client.SlowLogGet(ctx, Int(10))))
	requireNonNegative(t, must[int64](t)(client.SlowLogLen(ctx)))
	if err := client.SlowLogReset(ctx); err != nil {
		t.Fatal(err)
	}
	requireNonNegative(t, must[int64](t)(client.LastSave(ctx)))
	if err := client.Save(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.BgSave(ctx); err != nil {
		t.Fatal(err)
	}
	requireValue(t, must[any](t)(client.Module(ctx, "LIST")))
	requireValue(t, must[any](t)(client.FerricStoreBlobGC(ctx)))
	requireNonNegative(t, must[int64](t)(client.Publish(ctx, prefix+"channel", "message")))
	requireValue(t, must[[]string](t)(client.PubSubChannels(ctx, prefix+"*")))
	if subs := must[map[string]int64](t)(client.PubSubNumSub(ctx, prefix+"channel")); len(subs) == 0 {
		t.Fatalf("expected pubsub numsub response, got %#v", subs)
	}
	requireNonNegative(t, must[int64](t)(client.PubSubNumPat(ctx)))

	aclUser := strings.NewReplacer(":", "_", "-", "_").Replace("go_sdk_" + runID)
	if err := client.ACLSetUser(ctx, aclUser, "on", ">secret", "+@all"); err != nil {
		t.Fatal(err)
	}
	requireMap(t, must[map[string]any](t)(client.ACLGetUser(ctx, aclUser)))
	if users := must[[]string](t)(client.ACLList(ctx)); !containsRuleForUser(users, aclUser) {
		t.Fatalf("expected ACL LIST to include %q, got %#v", aclUser, users)
	}
	requireNonNegative(t, must[int64](t)(client.ACLDelUser(ctx, aclUser)))
	if err := client.ACLSave(ctx); err != nil {
		t.Fatal(err)
	}

	requireRecognizedCommandError(t, client.Select(ctx, 0), "SELECT", 0)
	_, err := client.ClusterJoin(ctx, "127.0.0.1:1", false)
	requireRecognizedCommandError(t, err, "CLUSTER.JOIN", "127.0.0.1:1")
	_, err = client.ClusterLeave(ctx)
	requireRecognizedCommandError(t, err, "CLUSTER.LEAVE")
	_, err = client.ClusterFailover(ctx, 0, "missing-node")
	requireRecognizedCommandError(t, err, "CLUSTER.FAILOVER", 0, "missing-node")
	_, err = client.ClusterPromote(ctx, "missing-node")
	requireRecognizedCommandError(t, err, "CLUSTER.PROMOTE", "missing-node")
	_, err = client.ClusterDemote(ctx, "missing-node")
	requireRecognizedCommandError(t, err, "CLUSTER.DEMOTE", "missing-node")

	if err := client.FlushDB(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if err := client.FlushAll(ctx, ""); err != nil {
		t.Fatal(err)
	}
}
