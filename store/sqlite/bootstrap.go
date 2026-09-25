package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/winghv/agentwharf/store"
)

var _ store.BootstrapStore = (*Store)(nil)

func (s *Store) Bootstrap(ctx context.Context, sessionID string, throughSeq int64) ([]store.Event, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin bootstrap read: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	events := make([]store.Event, 0, len(store.BootstrapEventTypes()))
	for _, eventType := range store.BootstrapEventTypes() {
		var event store.Event
		var eventTimeMS int64
		err := tx.QueryRowContext(ctx, `
SELECT session_id, seq, type, payload, event_time_ms
FROM session_events
WHERE session_id = ? AND type = ? AND seq <= ?
ORDER BY seq DESC LIMIT 1`, sessionID, eventType, throughSeq).Scan(
			&event.SessionID, &event.Seq, &event.Type, &event.Payload, &eventTimeMS,
		)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read bootstrap event: %w", err)
		}
		event.Time = time.UnixMilli(eventTimeMS)
		events = append(events, event)
	}
	sort.Slice(events, func(i, j int) bool { return events[i].Seq < events[j].Seq })
	return events, nil
}
