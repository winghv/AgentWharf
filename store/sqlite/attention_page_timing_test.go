package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
)

func TestAttentionSummaryPageRechecksExpiryAtQueryTime(t *testing.T) {
	ctx := context.Background()
	page, err := Open(ctx, filepath.Join(t.TempDir(), "attention-page-timing.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = page.Close() })
	if _, err := page.Append(ctx, "ses_timing", []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := page.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "ses_timing", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	snapshotMS := time.Now().Add(-3 * time.Second).UnixMilli()
	page.attentionPageNow = func(ctx context.Context, db *sql.DB) (int64, error) {
		if _, err := db.ExecContext(ctx, `UPDATE session_adapter_connections SET active_credential_expires_at_ms = created_at_ms - 1000, created_at_ms = created_at_ms - 2000 WHERE session_id = 'ses_timing'`); err != nil {
			return 0, err
		}
		return snapshotMS, nil
	}
	result, err := page.AttentionSummaryPage(ctx, store.AttentionSummaryPageRequest{Limit: 1})
	if err != nil || len(result.Summaries) != 0 {
		t.Fatalf("expired-at-query page = %+v, %v", result, err)
	}
	if got := result.SnapshotAt.UnixMilli(); got != snapshotMS {
		t.Fatalf("SnapshotAt = %d, want conservative pre-query %d", got, snapshotMS)
	}
}
