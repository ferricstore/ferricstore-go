//go:build integration

package ferricstore

import "testing"

func TestIntegrationTypedStoreFamilies(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(StringCodec{})
	defer client.Close()

	runID := integrationSuffix("store")
	prefix := "go-sdk:store:" + runID + ":"
	defer cleanupPrefix(t, ctx, client, prefix)

	assertStringCommands(t, ctx, client, prefix)
	assertHashCommands(t, ctx, client, prefix)
	assertListSetSortedSetCommands(t, ctx, client, prefix)
	assertStreamBitmapHllGeoCommands(t, ctx, client, prefix, runID)
}
