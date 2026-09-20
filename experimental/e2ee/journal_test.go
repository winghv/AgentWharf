package e2ee

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	_ "modernc.org/sqlite"
)

func openJournal(t *testing.T, path string) (*CommandJournal, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA synchronous=FULL; PRAGMA busy_timeout=2000;`); err != nil {
		t.Fatal(err)
	}
	journal, err := NewCommandJournal(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return journal, db
}

func TestJournalRestartReplayAndRoles(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "endpoint.sqlite")
	journal, db := openJournal(t, path)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	command := Context{"command", "session", "controller", "epoch_1", "command_1", "session.send"}
	grants := []DeviceGrant{{"controller", public, true}, {"reader", public, false}}
	if err := journal.ReplaceGrants(ctx, command.Session, command.KeyID, 0, grants); err != nil {
		t.Fatal(err)
	}
	envelope, err := Seal(command, key, private, []byte("synthetic private content"))
	if err != nil {
		t.Fatal(err)
	}
	admission, content, err := journal.Admit(ctx, command, key, envelope)
	if err != nil || !admission.Execute || string(content) != "synthetic private content" {
		t.Fatalf("admission: %+v %v", admission, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	journal, db = openJournal(t, path)
	admission, content, err = journal.Admit(ctx, command, key, envelope)
	if err != nil || admission.Execute || admission.State != "outcome_unknown" || content != nil {
		t.Fatalf("restart replay: %+v %v", admission, err)
	}
	if err := journal.Finish(ctx, command.Session, command.MessageID, "completed"); err != nil {
		t.Fatal(err)
	}
	if err := journal.Finish(ctx, command.Session, command.MessageID, "completed"); err != nil {
		t.Fatal(err)
	}
	if err := journal.Finish(ctx, command.Session, command.MessageID, "outcome_unknown"); !errors.Is(err, ErrConflict) {
		t.Fatal("rewrote terminal result", err)
	}
	admission, _, err = journal.Admit(ctx, command, key, envelope)
	if err != nil || admission.Execute || admission.State != "completed" {
		t.Fatalf("completed replay: %+v %v", admission, err)
	}
	altered, err := Seal(command, key, private, []byte("different content"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := journal.Admit(ctx, command, key, altered); !errors.Is(err, ErrConflict) {
		t.Fatal("same ID different content", err)
	}
	for _, sender := range []string{"reader", "platform"} {
		denied := command
		denied.Sender = sender
		denied.MessageID = "denied"
		sealed, err := Seal(denied, key, private, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := journal.Admit(ctx, denied, key, sealed); !errors.Is(err, ErrUnauthorized) {
			t.Fatal("reader/platform admitted", err)
		}
	}
	if err := journal.ReplaceGrants(ctx, "session", "epoch_2", 1, []DeviceGrant{{"reader", public, false}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := journal.Admit(ctx, command, key, envelope); !errors.Is(err, ErrEpochStale) {
		t.Fatal("old epoch admitted", err)
	}
	if err := journal.ReplaceGrants(ctx, "session", "epoch_3", 1, grants); !errors.Is(err, ErrConflict) {
		t.Fatal("stale membership accepted", err)
	}
	if err := journal.ReplaceGrants(ctx, "session", "epoch_1", 2, grants); !errors.Is(err, ErrConflict) {
		t.Fatal("reused old key epoch", err)
	}
	revoked := command
	revoked.KeyID = "epoch_2"
	revoked.MessageID = "revoked"
	sealed, err := Seal(revoked, key, private, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := journal.Admit(ctx, revoked, key, sealed); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("revoked sender admitted", err)
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM e2ee_local_commands`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("unexpected records: %d %v", count, err)
	}
}

func TestJournalConcurrentAdmissionAndFailure(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "endpoint.sqlite")
	journal, db := openJournal(t, path)
	second, _ := openJournal(t, path)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	command := Context{"launch", "session", "device", "epoch_1", "launch_1", "session.send"}
	if err := journal.ReplaceGrants(ctx, "session", "epoch_1", 0, []DeviceGrant{{"device", public, true}}); err != nil {
		t.Fatal(err)
	}
	envelope, err := Seal(command, key, private, []byte("synthetic"))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var executed atomic.Int32
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			target := journal
			if i%2 == 1 {
				target = second
			}
			admission, _, err := target.Admit(ctx, command, key, envelope)
			if err != nil {
				t.Errorf("concurrent admission: %v", err)
				return
			}
			if admission.Execute {
				executed.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if executed.Load() != 1 {
		t.Fatalf("executions=%d", executed.Load())
	}
	if _, err := db.Exec(`CREATE TRIGGER fail_command BEFORE INSERT ON e2ee_local_commands BEGIN SELECT RAISE(ABORT, 'synthetic'); END`); err != nil {
		t.Fatal(err)
	}
	command.MessageID = "failure"
	envelope, err = Seal(command, key, private, nil)
	if err != nil {
		t.Fatal(err)
	}
	admission, plaintext, err := journal.Admit(ctx, command, key, envelope)
	if !errors.Is(err, ErrJournal) || admission.Execute || plaintext != nil {
		t.Fatal("failed commit allowed execution", err)
	}
	var count int
	if err := db.QueryRow(`SELECT command_count FROM e2ee_local_sessions WHERE session = 'session'`).Scan(&count); err != nil || count != 1 {
		t.Fatal("counter not rolled back", count, err)
	}
	if _, err := db.Exec(`DROP TRIGGER fail_command; UPDATE e2ee_local_sessions SET command_count = 4096`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := journal.Admit(ctx, command, key, envelope); !errors.Is(err, ErrCapacity) {
		t.Fatal("unbounded journal", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := journal.Admit(cancelled, command, key, envelope); !errors.Is(err, ErrJournal) {
		t.Fatal("cancellation ignored", err)
	}
}
