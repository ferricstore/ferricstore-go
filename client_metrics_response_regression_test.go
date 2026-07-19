package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestFerricStoreMetricsParsesLabeledPrometheusSamples(t *testing.T) {
	response := "# HELP ferric_reads_total Reads\n" +
		"ferric_reads_total{node=\"host:6379\",kind=\"cold read\"} 12.5 1700000000\n" +
		"ferric_queue_depth 3\n"
	want := map[string]any{
		"ferric_reads_total{node=\"host:6379\",kind=\"cold read\"}": 12.5,
		"ferric_queue_depth": int64(3),
	}
	got, err := NewClientWithExecutor(&fakeExecutor{value: response}).FerricStoreMetrics(context.Background())
	if err != nil {
		t.Fatalf("FERRICSTORE.METRICS: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("metrics = %#v, want %#v", got, want)
	}
}

func TestFerricStoreMetricsAcceptsCommentOnlyPrometheusExposition(t *testing.T) {
	response := "# HELP ferric_reads_total Reads\n" +
		"# TYPE ferric_reads_total counter\n" +
		"# EOF\n"
	got, err := NewClientWithExecutor(&fakeExecutor{value: response}).FerricStoreMetrics(context.Background())
	if err != nil {
		t.Fatalf("FERRICSTORE.METRICS: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("metrics = %#v, want non-nil empty map", got)
	}
}

func TestFerricStoreMetricsRejectsMalformedSamples(t *testing.T) {
	for _, response := range []any{
		"metric_without_value\n",
		"metric{label=\"unterminated} 1\n",
		"metric value trailing unexpected fields\n",
	} {
		client := NewClientWithExecutor(&fakeExecutor{value: response})
		if _, err := client.FerricStoreMetrics(context.Background()); err == nil {
			t.Fatalf("accepted malformed metrics response %#v", response)
		}
	}
}

func TestServerInfoRejectsDuplicateOrMalformedFields(t *testing.T) {
	for _, response := range []any{
		"# Server\nrole:leader\nrole:follower\n",
		"# Server\nmalformed\n",
	} {
		client := NewClientWithExecutor(&fakeExecutor{value: response})
		if _, err := client.ServerInfo(context.Background()); err == nil {
			t.Fatalf("accepted malformed INFO response %#v", response)
		}
	}
}
