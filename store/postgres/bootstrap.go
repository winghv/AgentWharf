package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/winghv/agentwharf/store"
)

var _ store.BootstrapStore = (*Store)(nil)

func (s *Store) Bootstrap(ctx context.Context, sessionID string, throughSeq int64) ([]store.Event, error) {
	if s.pool == nil {
		return nil, errors.New("postgres event store pool is nil")
	}
	rows, err := s.pool.Query(ctx, `
SELECT event.session_id, event.seq, event.type, event.payload, event.created_at
FROM unnest($2::text[]) AS wanted(type)
CROSS JOIN LATERAL (
    SELECT session_id, seq, type, payload, created_at
    FROM session_events
    WHERE session_id = $1 AND type = wanted.type AND seq <= $3
    ORDER BY seq DESC LIMIT 1
) AS event
ORDER BY event.seq`, sessionID, store.BootstrapEventTypes(), throughSeq)
	if err != nil {
		return nil, fmt.Errorf("read bootstrap events: %w", err)
	}
	defer rows.Close()
	events := make([]store.Event, 0, len(store.BootstrapEventTypes()))
	for rows.Next() {
		var event store.Event
		var payload []byte
		if err := rows.Scan(&event.SessionID, &event.Seq, &event.Type, &payload, &event.Time); err != nil {
			return nil, fmt.Errorf("scan bootstrap event: %w", err)
		}
		event.Payload = compactJSON(payload)
		events = append(events, event)
	}
	return events, rows.Err()
}
