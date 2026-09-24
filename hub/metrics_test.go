package hub

import (
	"strings"
	"testing"
	"time"
)

func TestHubMetricsAreLabelFree(t *testing.T) {
	metrics := NewHubMetrics()
	started := time.Now().Add(-time.Millisecond)
	metrics.ObserveHandshake(started)
	metrics.ObserveReplay(started)
	metrics.ObserveHistory(started)
	metrics.ObserveAppend(started)
	metrics.ObserveProjection(started)
	metrics.ObserveFanout(started)
	metrics.IncSlowWrite()
	metrics.IncBufferOverflow()
	output := metrics.Snapshot().Prometheus()
	for _, forbidden := range []string{"session-id", "prompt", "tool output", "token", "secret", "path"} {
		if strings.Contains(strings.ToLower(output), forbidden) {
			t.Fatalf("metrics contained forbidden value %q: %s", forbidden, output)
		}
	}
	snapshot := metrics.Snapshot()
	if snapshot.Handshakes != 1 || snapshot.Replays != 1 || snapshot.Histories != 1 || snapshot.Appends != 1 || snapshot.Projections != 1 || snapshot.Fanouts != 1 || snapshot.SlowWrites != 1 || snapshot.BufferOverflows != 1 {
		t.Fatalf("unexpected metric snapshot: %+v", snapshot)
	}
}
