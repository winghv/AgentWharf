package postgres

import (
	"strings"
	"testing"
	"time"
)

func TestMetricsAreLabelFreeAndDoNotExposeContent(t *testing.T) {
	metrics := NewMetrics()
	started := time.Now().Add(-time.Millisecond)
	metrics.ObserveAppend(started, 3, 128)
	metrics.ObserveReplay(started)
	metrics.ObserveHistory(started)
	metrics.ObserveProjection()
	output := metrics.Prometheus(nil)
	for _, forbidden := range []string{"session-id", "prompt", "tool output", "token", "secret", "path"} {
		if strings.Contains(strings.ToLower(output), forbidden) {
			t.Fatalf("metrics contained forbidden value %q: %s", forbidden, output)
		}
	}
	snapshot := metrics.Snapshot()
	if snapshot.AppendCount != 1 || snapshot.ReplayCount != 1 || snapshot.HistoryCount != 1 || snapshot.ProjectionCount != 1 {
		t.Fatalf("unexpected metric snapshot: %+v", snapshot)
	}
	if snapshot.SessionEventBytes != 128 || snapshot.SessionEventRows != 3 {
		t.Fatalf("unexpected event counters: %+v", snapshot)
	}
}
