package postgres

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Metrics records bounded Store timings and byte counts. It never accepts a
// session ID, query text, payload, path, credential, or user-content label.
type Metrics struct {
	queryNs           atomic.Uint64
	transactionNs     atomic.Uint64
	poolWaitNs        atomic.Uint64
	appendCount       atomic.Uint64
	replayCount       atomic.Uint64
	historyCount      atomic.Uint64
	projectionCount   atomic.Uint64
	sessionEventBytes atomic.Uint64
	sessionEventRows  atomic.Uint64
	walBytes          atomic.Uint64
	ioBytes           atomic.Uint64
	sessionEventsSize atomic.Uint64
}

type MetricSnapshot struct {
	QueryNs, TransactionNs, PoolWaitNs                      uint64
	AppendCount, ReplayCount, HistoryCount, ProjectionCount uint64
	SessionEventBytes, SessionEventRows                     uint64
	WALBytes, IOBytes, SessionEventsSize                    uint64
}

func NewMetrics() *Metrics { return &Metrics{} }

func (m *Metrics) ObserveQuery(started time.Time) {
	if m != nil {
		m.queryNs.Add(uint64(time.Since(started)))
	}
}
func (m *Metrics) ObserveTransaction(started time.Time) {
	if m != nil {
		m.transactionNs.Add(uint64(time.Since(started)))
	}
}
func (m *Metrics) ObservePoolWait(duration time.Duration) {
	if m != nil && duration > 0 {
		m.poolWaitNs.Add(uint64(duration))
	}
}
func (m *Metrics) ObserveAppend(started time.Time, rows int, bytes uint64) {
	if m == nil {
		return
	}
	m.appendCount.Add(1)
	m.transactionNs.Add(uint64(time.Since(started)))
	m.sessionEventRows.Add(uint64(maxInt(rows, 0)))
	m.sessionEventBytes.Add(bytes)
}
func (m *Metrics) ObserveReplay(started time.Time) {
	if m != nil {
		m.replayCount.Add(1)
		m.queryNs.Add(uint64(time.Since(started)))
	}
}
func (m *Metrics) ObserveHistory(started time.Time) {
	if m != nil {
		m.historyCount.Add(1)
		m.queryNs.Add(uint64(time.Since(started)))
	}
}
func (m *Metrics) ObserveProjection() {
	if m != nil {
		m.projectionCount.Add(1)
	}
}
func (m *Metrics) ObserveDatabaseStats(walBytes, ioBytes, sessionEventsSize uint64) {
	if m == nil {
		return
	}
	m.walBytes.Store(walBytes)
	m.ioBytes.Store(ioBytes)
	m.sessionEventsSize.Store(sessionEventsSize)
}

func (m *Metrics) Snapshot() MetricSnapshot {
	if m == nil {
		return MetricSnapshot{}
	}
	return MetricSnapshot{
		QueryNs: m.queryNs.Load(), TransactionNs: m.transactionNs.Load(), PoolWaitNs: m.poolWaitNs.Load(),
		AppendCount: m.appendCount.Load(), ReplayCount: m.replayCount.Load(), HistoryCount: m.historyCount.Load(),
		ProjectionCount: m.projectionCount.Load(), SessionEventBytes: m.sessionEventBytes.Load(), SessionEventRows: m.sessionEventRows.Load(),
		WALBytes: m.walBytes.Load(), IOBytes: m.ioBytes.Load(), SessionEventsSize: m.sessionEventsSize.Load(),
	}
}

func (m *Metrics) CollectDatabaseStats(ctx context.Context, pool *pgxpool.Pool) error {
	if m == nil || pool == nil {
		return nil
	}
	var walBytes, ioBytes, relationBytes int64
	row := pool.QueryRow(ctx, `SELECT
		COALESCE((SELECT wal_bytes FROM pg_stat_wal), 0),
		COALESCE((SELECT SUM(blks_read) * 8192 FROM pg_stat_database), 0),
		COALESCE(pg_total_relation_size('public.session_events'), 0)`)
	if err := row.Scan(&walBytes, &ioBytes, &relationBytes); err != nil {
		return fmt.Errorf("collect postgres database stats: %w", err)
	}
	m.ObserveDatabaseStats(uint64(maxInt64(walBytes)), uint64(maxInt64(ioBytes)), uint64(maxInt64(relationBytes)))
	return nil
}
func (m *Metrics) Prometheus(pool *pgxpool.Pool) string {
	s := m.Snapshot()
	poolWaitCount := uint64(0)
	poolMax := int32(0)
	poolAcquired := int32(0)
	if pool != nil {
		stat := pool.Stat()
		poolWaitCount = uint64(stat.EmptyAcquireCount())
		poolMax = stat.MaxConns()
		poolAcquired = stat.AcquiredConns()
	}
	return fmt.Sprintf(`# HELP agentwharf_postgres_query_duration_nanoseconds_total Cumulative query duration.
# TYPE agentwharf_postgres_query_duration_nanoseconds_total counter
agentwharf_postgres_query_duration_nanoseconds_total %d
# HELP agentwharf_postgres_transaction_duration_nanoseconds_total Cumulative transaction duration.
# TYPE agentwharf_postgres_transaction_duration_nanoseconds_total counter
agentwharf_postgres_transaction_duration_nanoseconds_total %d
# HELP agentwharf_postgres_pool_wait_nanoseconds_total Cumulative pool wait duration.
# TYPE agentwharf_postgres_pool_wait_nanoseconds_total counter
agentwharf_postgres_pool_wait_nanoseconds_total %d
# HELP agentwharf_postgres_pool_empty_acquire_total Pool acquisitions that waited.
# TYPE agentwharf_postgres_pool_empty_acquire_total counter
agentwharf_postgres_pool_empty_acquire_total %d
# HELP agentwharf_postgres_pool_max_connections Configured pool maximum.
# TYPE agentwharf_postgres_pool_max_connections gauge
agentwharf_postgres_pool_max_connections %d
# HELP agentwharf_postgres_pool_acquired_connections Current acquired connections.
# TYPE agentwharf_postgres_pool_acquired_connections gauge
agentwharf_postgres_pool_acquired_connections %d
# HELP agentwharf_postgres_append_batches_total Durable append batches.
# TYPE agentwharf_postgres_append_batches_total counter
agentwharf_postgres_append_batches_total %d
# HELP agentwharf_postgres_replay_queries_total Replay queries.
# TYPE agentwharf_postgres_replay_queries_total counter
agentwharf_postgres_replay_queries_total %d
# HELP agentwharf_postgres_history_queries_total History queries.
# TYPE agentwharf_postgres_history_queries_total counter
agentwharf_postgres_history_queries_total %d
# HELP agentwharf_postgres_projection_updates_total Projection updates.
# TYPE agentwharf_postgres_projection_updates_total counter
agentwharf_postgres_projection_updates_total %d
# HELP agentwharf_postgres_session_event_bytes_total Durable event payload bytes observed.
# TYPE agentwharf_postgres_session_event_bytes_total counter
agentwharf_postgres_session_event_bytes_total %d
# HELP agentwharf_postgres_session_event_rows_total Durable event rows observed.
# TYPE agentwharf_postgres_session_event_rows_total counter
agentwharf_postgres_session_event_rows_total %d
# HELP agentwharf_postgres_session_events_relation_bytes Current session_events relation size.
# TYPE agentwharf_postgres_session_events_relation_bytes gauge
agentwharf_postgres_session_events_relation_bytes %d
# HELP agentwharf_postgres_wal_bytes Current WAL bytes reported by PostgreSQL.
# TYPE agentwharf_postgres_wal_bytes gauge
agentwharf_postgres_wal_bytes %d
# HELP agentwharf_postgres_io_bytes Current PostgreSQL IO bytes reported by the collector.
# TYPE agentwharf_postgres_io_bytes gauge
agentwharf_postgres_io_bytes %d
`, s.QueryNs, s.TransactionNs, s.PoolWaitNs, poolWaitCount, poolMax, poolAcquired, s.AppendCount, s.ReplayCount, s.HistoryCount, s.ProjectionCount, s.SessionEventBytes, s.SessionEventRows, s.SessionEventsSize, s.WALBytes, s.IOBytes)
}

func maxInt64(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}
func maxInt(value, floor int) int {
	if value < floor {
		return floor
	}
	return value
}
