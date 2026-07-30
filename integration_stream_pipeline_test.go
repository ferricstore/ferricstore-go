//go:build integration

package ferricstore

import "testing"

func TestIntegrationCompactStreamPipelineSpansTopics(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(StringCodec{})
	defer client.Close()
	prefix := "go-sdk:stream-pipeline:" + integrationSuffix("live") + ":"
	first := prefix + "{a}:first"
	second := prefix + "{b}:second"
	defer cleanupPrefix(t, ctx, client, prefix)

	results := must[[]any](t)(client.Pipeline(ctx, [][]any{
		{"XADD", first, "*", "field", "one"},
		{"XADD", second, "*", "field", "two"},
		{"XADD", first, "*", "field", "three"},
	}))
	if len(results) != 3 {
		t.Fatalf("multi-topic XADD pipeline returned %d results", len(results))
	}
	for index, result := range results {
		if asString(result) == "" {
			t.Fatalf("multi-topic XADD result %d is empty: %#v", index, result)
		}
	}
	requireInt64(t, must[int64](t)(client.Stream().Len(ctx, first)), 2)
	requireInt64(t, must[int64](t)(client.Stream().Len(ctx, second)), 1)
}
