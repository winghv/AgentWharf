package hub

import (
	"fmt"
	"sync/atomic"
	"time"
)

// HubMetrics is intentionally process-scoped and label-free. Session IDs,
// payloads, paths, credentials and user content never enter these counters.
type HubMetrics struct {
	handshakes      atomic.Uint64
	replays         atomic.Uint64
	histories       atomic.Uint64
	appends         atomic.Uint64
	projections     atomic.Uint64
	fanouts         atomic.Uint64
	slowWrites      atomic.Uint64
	bufferOverflows atomic.Uint64
	handshakeNs     atomic.Uint64
	replayNs        atomic.Uint64
	historyNs       atomic.Uint64
	appendNs        atomic.Uint64
	projectionNs    atomic.Uint64
	fanoutNs        atomic.Uint64
}

type HubMetricSnapshot struct {
	Handshakes, Replays, Histories, Appends, Projections, Fanouts      uint64
	SlowWrites, BufferOverflows                                        uint64
	HandshakeNs, ReplayNs, HistoryNs, AppendNs, ProjectionNs, FanoutNs uint64
}

func NewHubMetrics() *HubMetrics { return &HubMetrics{} }

func (m *HubMetrics) observe(counter, duration *atomic.Uint64, started time.Time) {
	if m == nil {
		return
	}
	counter.Add(1)
	if duration != nil && !started.IsZero() {
		duration.Add(uint64(time.Since(started)))
	}
}
func (m *HubMetrics) ObserveHandshake(started time.Time) {
	m.observe(&m.handshakes, &m.handshakeNs, started)
}
func (m *HubMetrics) ObserveReplay(started time.Time) { m.observe(&m.replays, &m.replayNs, started) }
func (m *HubMetrics) ObserveHistory(started time.Time) {
	m.observe(&m.histories, &m.historyNs, started)
}
func (m *HubMetrics) ObserveAppend(started time.Time) { m.observe(&m.appends, &m.appendNs, started) }
func (m *HubMetrics) ObserveProjection(started time.Time) {
	m.observe(&m.projections, &m.projectionNs, started)
}
func (m *HubMetrics) ObserveFanout(started time.Time) { m.observe(&m.fanouts, &m.fanoutNs, started) }
func (m *HubMetrics) IncSlowWrite() {
	if m != nil {
		m.slowWrites.Add(1)
	}
}
func (m *HubMetrics) IncBufferOverflow() {
	if m != nil {
		m.bufferOverflows.Add(1)
	}
}

func (m *HubMetrics) Snapshot() HubMetricSnapshot {
	if m == nil {
		return HubMetricSnapshot{}
	}
	return HubMetricSnapshot{
		Handshakes: m.handshakes.Load(), Replays: m.replays.Load(), Histories: m.histories.Load(),
		Appends: m.appends.Load(), Projections: m.projections.Load(), Fanouts: m.fanouts.Load(),
		SlowWrites: m.slowWrites.Load(), BufferOverflows: m.bufferOverflows.Load(),
		HandshakeNs: m.handshakeNs.Load(), ReplayNs: m.replayNs.Load(), HistoryNs: m.historyNs.Load(),
		AppendNs: m.appendNs.Load(), ProjectionNs: m.projectionNs.Load(), FanoutNs: m.fanoutNs.Load(),
	}
}

func (s HubMetricSnapshot) Prometheus() string {
	return fmt.Sprintf(`# HELP agentwharf_hub_handshakes_total WebSocket handshake attempts.
# TYPE agentwharf_hub_handshakes_total counter
agentwharf_hub_handshakes_total %d
# HELP agentwharf_hub_replays_total Replay attempts.
# TYPE agentwharf_hub_replays_total counter
agentwharf_hub_replays_total %d
# HELP agentwharf_hub_history_requests_total History page requests.
# TYPE agentwharf_hub_history_requests_total counter
agentwharf_hub_history_requests_total %d
# HELP agentwharf_hub_appends_total Durable append batches.
# TYPE agentwharf_hub_appends_total counter
agentwharf_hub_appends_total %d
# HELP agentwharf_hub_projection_total Projection updates.
# TYPE agentwharf_hub_projection_total counter
agentwharf_hub_projection_total %d
# HELP agentwharf_hub_fanouts_total Event fanout operations.
# TYPE agentwharf_hub_fanouts_total counter
agentwharf_hub_fanouts_total %d
# HELP agentwharf_hub_slow_writes_total Slow client writes.
# TYPE agentwharf_hub_slow_writes_total counter
agentwharf_hub_slow_writes_total %d
# HELP agentwharf_hub_replay_duration_nanoseconds_total Cumulative replay duration.
# TYPE agentwharf_hub_replay_duration_nanoseconds_total counter
agentwharf_hub_replay_duration_nanoseconds_total %d
# HELP agentwharf_hub_history_duration_nanoseconds_total Cumulative history duration.
# TYPE agentwharf_hub_history_duration_nanoseconds_total counter
agentwharf_hub_history_duration_nanoseconds_total %d
# HELP agentwharf_hub_append_duration_nanoseconds_total Cumulative append duration.
# TYPE agentwharf_hub_append_duration_nanoseconds_total counter
agentwharf_hub_append_duration_nanoseconds_total %d
`, s.Handshakes, s.Replays, s.Histories, s.Appends, s.Projections, s.Fanouts, s.SlowWrites, s.ReplayNs, s.HistoryNs, s.AppendNs)
}
