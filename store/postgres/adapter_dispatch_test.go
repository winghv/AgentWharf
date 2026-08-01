package postgres_test

import (
	"context"
	"hash/fnv"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/postgres"
)

func TestAppendAdapterEventsRevalidatesInsidePostgresTransaction(t *testing.T) {
	harness := newPostgresConnectionHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const sessionID = "ses_dispatch_atomic"
	if _, err := harness.pool.Exec(ctx, `INSERT INTO agent_sessions (id) VALUES ($1)`, sessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{
		SessionID: sessionID, ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	connection, err := harness.AcceptAdapterHello(ctx, sessionID, store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	grant, err := harness.AllocateAdapterGrantFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	admission := store.AdapterConnectionAdmission{CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch, AcceptedFence: connection.AcceptedFence, GrantFence: grant}
	pending := []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"working"}`)}}
	if seq, err := harness.AppendAdapterEvents(ctx, sessionID, admission, pending); err != nil || seq != 1 {
		t.Fatalf("initial AppendAdapterEvents() = %d, %v", seq, err)
	}
	lockTx, err := harness.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lockTx.Rollback(ctx)
	var locked int
	if err := lockTx.QueryRow(ctx, `SELECT 1 FROM session_adapter_connections WHERE session_id=$1 FOR UPDATE`, sessionID).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, err := harness.AppendAdapterEvents(ctx, sessionID, admission, pending); result <- err }()
	select {
	case err := <-result:
		t.Fatalf("stale append escaped authority row lock: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := lockTx.Exec(ctx, `UPDATE session_adapter_connections SET active_credential_generation=2, credential_generation_high_watermark=2 WHERE session_id=$1`, sessionID); err != nil {
		t.Fatal(err)
	}
	if err := lockTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err == nil {
		t.Fatal("stale admission appended after generation replacement")
	}
	if latest, err := harness.LatestSeq(ctx, sessionID); err != nil || latest != 1 {
		t.Fatalf("latest seq after stale append = %d, %v", latest, err)
	}
}

func TestAppendAdapterEventsCommitsTerminalAttentionAndFencesAuthority(t *testing.T) {
	for _, event := range []struct {
		name    string
		pending store.PendingEvent
		outcome string
	}{
		{name: "ended", pending: store.PendingEvent{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"ended"}`)}, outcome: "ended"},
		{name: "error state", pending: store.PendingEvent{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"error"}`)}, outcome: "error"},
		{name: "error event", pending: store.PendingEvent{Type: "session.error", Time: time.Now(), Payload: []byte(`{"ignored":"by attention"}`)}, outcome: "error"},
	} {
		event := event
		t.Run(event.name, func(t *testing.T) {
			harness := newPostgresConnectionHarness(t)
			ctx := context.Background()
			sessionID := "ses_dispatch_terminal_" + strings.ReplaceAll(event.name, " ", "_")
			if _, err := harness.pool.Exec(ctx, `INSERT INTO agent_sessions (id) VALUES ($1)`, sessionID); err != nil {
				t.Fatal(err)
			}
			if _, err := harness.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{
				SessionID: sessionID, ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour),
			}); err != nil {
				t.Fatal(err)
			}
			connection, err := harness.AcceptAdapterHello(ctx, sessionID, store.AdapterHello{CredentialGeneration: 1})
			if err != nil {
				t.Fatal(err)
			}
			grant, err := harness.AllocateAdapterGrantFence(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if seq, err := harness.AppendAdapterEvents(ctx, sessionID, store.AdapterConnectionAdmission{
				CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch, AcceptedFence: connection.AcceptedFence, GrantFence: grant,
			}, []store.PendingEvent{event.pending}); err != nil || seq != 1 {
				t.Fatalf("terminal AppendAdapterEvents() = %d, %v", seq, err)
			}
			summaries, err := harness.AttentionSnapshot(ctx, []string{sessionID})
			if err != nil || len(summaries) != 1 || summaries[0].TerminalOutcome == nil || *summaries[0].TerminalOutcome != event.outcome {
				t.Fatalf("terminal attention summary = %+v, %v", summaries, err)
			}
			persisted, err := harness.AdapterConnection(ctx, sessionID)
			if err != nil || persisted.RevokedAt == nil || persisted.TerminalAt == nil {
				t.Fatalf("terminal adapter connection = %+v, %v", persisted, err)
			}
			if latest, err := harness.LatestSeq(ctx, sessionID); err != nil || latest != 1 {
				t.Fatalf("terminal latest sequence = %d, %v", latest, err)
			}
		})
	}
}

func TestAppendAdapterEventsRejectsTerminalAttentionWithTail(t *testing.T) {
	for _, event := range []struct {
		name     string
		terminal store.PendingEvent
	}{
		{name: "ended", terminal: store.PendingEvent{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"ended"}`)}},
		{name: "error state", terminal: store.PendingEvent{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"error"}`)}},
		{name: "error event", terminal: store.PendingEvent{Type: "session.error", Time: time.Now(), Payload: []byte(`{"ignored":"by attention"}`)}},
	} {
		event := event
		t.Run(event.name, func(t *testing.T) {
			harness := newPostgresConnectionHarness(t)
			ctx := context.Background()
			sessionID := "ses_dispatch_terminal_tail_" + strings.ReplaceAll(event.name, " ", "_")
			if _, err := harness.pool.Exec(ctx, `INSERT INTO agent_sessions (id) VALUES ($1)`, sessionID); err != nil {
				t.Fatal(err)
			}
			if _, err := harness.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{
				SessionID: sessionID, ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour),
			}); err != nil {
				t.Fatal(err)
			}
			connection, err := harness.AcceptAdapterHello(ctx, sessionID, store.AdapterHello{CredentialGeneration: 1})
			if err != nil {
				t.Fatal(err)
			}
			grant, err := harness.AllocateAdapterGrantFence(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := harness.AppendAdapterEvents(ctx, sessionID, store.AdapterConnectionAdmission{
				CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch, AcceptedFence: connection.AcceptedFence, GrantFence: grant,
			}, []store.PendingEvent{event.terminal, {Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"ready"}`)}}); err == nil {
				t.Fatal("terminal Adapter batch with tail event committed")
			}
			if latest, err := harness.LatestSeq(ctx, sessionID); err != nil || latest != 0 {
				t.Fatalf("terminal tail latest sequence = %d, %v", latest, err)
			}
			if summaries, err := harness.AttentionSnapshot(ctx, []string{sessionID}); err != nil || len(summaries) != 0 {
				t.Fatalf("terminal tail attention summary = %+v, %v", summaries, err)
			}
			persisted, err := harness.AdapterConnection(ctx, sessionID)
			if err != nil || persisted.RevokedAt != nil || persisted.TerminalAt != nil {
				t.Fatalf("terminal tail adapter connection = %+v, %v", persisted, err)
			}
		})
	}
}

func TestAppendAdapterEventsLocksAuthorityBeforeEventStream(t *testing.T) {
	harness := newPostgresConnectionHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const sessionID = "ses_dispatch_lock_order"
	if _, err := harness.pool.Exec(ctx, `INSERT INTO agent_sessions (id) VALUES ($1)`, sessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{
		SessionID: sessionID, ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	connection, err := harness.AcceptAdapterHello(ctx, sessionID, store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	grant, err := harness.AllocateAdapterGrantFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tracer := newQueryStartSignal("FROM session_adapter_connections WHERE session_id=$1")
	harness.pool.Close()
	harness.pool = openPool(t, harness.dsn, harness.schemaName, tracer)
	harness.Store = postgres.New(harness.pool)
	blocker := openPool(t, harness.dsn, harness.schemaName, nil)
	t.Cleanup(blocker.Close)
	authorityTx, err := blocker.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer authorityTx.Rollback(context.Background())
	var locked int
	if err := authorityTx.QueryRow(ctx, `SELECT 1 FROM session_adapter_connections WHERE session_id=$1 FOR UPDATE`, sessionID).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, appendErr := harness.AppendAdapterEvents(ctx, sessionID, store.AdapterConnectionAdmission{
			CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch,
			AcceptedFence: connection.AcceptedFence, GrantFence: grant,
		}, []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"working"}`)}})
		result <- appendErr
	}()
	select {
	case <-tracer.started:
	case <-ctx.Done():
		t.Fatal("adapter append did not start authority validation")
	}
	streamTx, err := blocker.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var acquired bool
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(sessionID))
	if err := streamTx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, int64(hash.Sum64())).Scan(&acquired); err != nil {
		t.Fatal(err)
	}
	if err := streamTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if !acquired {
		_ = authorityTx.Rollback(context.Background())
		<-result
		t.Fatal("adapter append acquired the event-stream lock before the authority row")
	}
	if err := authorityTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatalf("adapter append failed after ordered locks released: %v", err)
	}
}

func TestAdapterAdmissionTransactionLocksAuthorityThroughCallback(t *testing.T) {
	harness := newPostgresConnectionHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const sessionID = "ses_dispatch_ephemeral_lock"
	if _, err := harness.pool.Exec(ctx, `INSERT INTO agent_sessions (id) VALUES ($1)`, sessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{
		SessionID: sessionID, ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	connection, err := harness.AcceptAdapterHello(ctx, sessionID, store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	grant, err := harness.AllocateAdapterGrantFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	transaction := make(chan error, 1)
	go func() {
		transaction <- harness.WithAdapterConnectionTransaction(ctx, func(tx store.AdapterConnectionStore) error {
			validator, ok := tx.(interface {
				ValidateAdapterEffectAdmission(context.Context, string, store.AdapterConnectionAdmission) (store.AdapterConnection, error)
			})
			if !ok {
				return context.Canceled
			}
			if _, err := validator.ValidateAdapterEffectAdmission(ctx, sessionID, store.AdapterConnectionAdmission{
				CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch,
				AcceptedFence: connection.AcceptedFence, GrantFence: grant,
			}); err != nil {
				return err
			}
			close(started)
			<-release
			return nil
		})
	}()
	<-started
	mutated := make(chan error, 1)
	go func() {
		_, err := harness.pool.Exec(ctx, `UPDATE session_adapter_connections SET revoked_at=clock_timestamp() WHERE session_id=$1`, sessionID)
		mutated <- err
	}()
	select {
	case err := <-mutated:
		t.Fatalf("revoke escaped ephemeral authority lock: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-transaction; err != nil {
		t.Fatalf("admission transaction: %v", err)
	}
	if err := <-mutated; err != nil {
		t.Fatalf("revoke after callback: %v", err)
	}
}

func TestAppendAdapterEventsRejectsExpiryAfterStreamLockWait(t *testing.T) {
	harness := newPostgresConnectionHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const sessionID = "ses_dispatch_expiry"
	if _, err := harness.pool.Exec(ctx, `INSERT INTO agent_sessions (id) VALUES ($1)`, sessionID); err != nil {
		t.Fatal(err)
	}
	expiresAt := time.Now().Add(500 * time.Millisecond)
	if _, err := harness.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{
		SessionID: sessionID, ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: expiresAt,
	}); err != nil {
		t.Fatal(err)
	}
	connection, err := harness.AcceptAdapterHello(ctx, sessionID, store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	grant, err := harness.AllocateAdapterGrantFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := harness.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx)
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(sessionID))
	if _, err := blocker.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(hash.Sum64())); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := harness.AppendAdapterEvents(ctx, sessionID, store.AdapterConnectionAdmission{
			CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch,
			AcceptedFence: connection.AcceptedFence, GrantFence: grant,
		}, []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"working"}`)}})
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("append did not wait for the event stream lock: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	time.Sleep(time.Until(expiresAt) + 50*time.Millisecond)
	if err := blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err == nil {
		t.Fatal("append committed after authority expired while waiting for the stream lock")
	}
	if latest, err := harness.LatestSeq(ctx, sessionID); err != nil || latest != 0 {
		t.Fatalf("latest seq after expired append = %d, %v", latest, err)
	}
}

func TestAppendAdapterEventsRejectsExpiryAfterInsertBeforeCommit(t *testing.T) {
	harness := newPostgresConnectionHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const sessionID = "ses_dispatch_insert_expiry"
	if _, err := harness.pool.Exec(ctx, `CREATE FUNCTION slow_adapter_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.5); RETURN NEW; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.pool.Exec(ctx, `CREATE TRIGGER slow_adapter_event BEFORE INSERT ON session_events FOR EACH ROW EXECUTE FUNCTION slow_adapter_event()`); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.pool.Exec(ctx, `INSERT INTO agent_sessions (id) VALUES ($1)`, sessionID); err != nil {
		t.Fatal(err)
	}
	expiresAt := time.Now().Add(300 * time.Millisecond)
	if _, err := harness.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{
		SessionID: sessionID, ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: expiresAt,
	}); err != nil {
		t.Fatal(err)
	}
	connection, err := harness.AcceptAdapterHello(ctx, sessionID, store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	grant, err := harness.AllocateAdapterGrantFence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = harness.AppendAdapterEvents(ctx, sessionID, store.AdapterConnectionAdmission{
		CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch,
		AcceptedFence: connection.AcceptedFence, GrantFence: grant,
	}, []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"working"}`)}})
	if err == nil {
		t.Fatal("append committed after authority expired during insert")
	}
	if !strings.Contains(err.Error(), "adapter authority lost") {
		t.Fatalf("append failed before final authority revalidation: %v", err)
	}
	if latest, err := harness.LatestSeq(ctx, sessionID); err != nil || latest != 0 {
		t.Fatalf("latest seq after insert expiry = %d, %v", latest, err)
	}
}
