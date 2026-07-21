package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/sqlite"
	"github.com/winghv/agentwharf/store/storetest"
)

func TestEventStoreContract(t *testing.T) {
	storetest.Contract(t, func(t *testing.T) store.EventStore {
		return openStore(t, filepath.Join(t.TempDir(), "events.db"))
	})
}

func TestAttentionSummaryStoreContract(t *testing.T) {
	var _ store.AttentionSummaryStore = (*sqlite.Store)(nil)
}

func TestAttentionSummaryPageIsReadOnlyAndKeysetBounded(t *testing.T) {
	attention := openStore(t, filepath.Join(t.TempDir(), "attention-page.db"))
	ctx := context.Background()
	for _, sessionID := range []string{"ses_page_a", "ses_page_b", "ses_page_c"} {
		if _, err := attention.Append(ctx, sessionID, []store.PendingEvent{{Type: "session.state", Time: testTime(1), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
			t.Fatalf("seed %s: %v", sessionID, err)
		}
	}
	page, err := attention.AttentionSummaryPage(ctx, store.AttentionSummaryPageRequest{Limit: 2})
	if err != nil || page.SnapshotAt.IsZero() || len(page.Summaries) != 2 || page.Summaries[0].SessionID != "ses_page_a" || page.NextAfterSessionID == nil || *page.NextAfterSessionID != "ses_page_b" {
		t.Fatalf("first page = %+v, %v", page, err)
	}
	next, err := attention.AttentionSummaryPage(ctx, store.AttentionSummaryPageRequest{AfterSessionID: *page.NextAfterSessionID, Limit: 2})
	if err != nil || len(next.Summaries) != 1 || next.Summaries[0].SessionID != "ses_page_c" || next.NextAfterSessionID != nil {
		t.Fatalf("second page = %+v, %v", next, err)
	}
	if _, err := attention.AttentionSummaryPage(ctx, store.AttentionSummaryPageRequest{Limit: 0}); err == nil {
		t.Fatal("zero page limit was accepted")
	}
}

func TestWarmAttachStoreContract(t *testing.T) {
	var _ store.WarmAttachStore = (*sqlite.Store)(nil)
	storetest.WarmAttachContract(t, storetest.WarmAttachHarness{
		Open: func(t *testing.T) store.WarmAttachStore {
			path := filepath.Join(t.TempDir(), "warm-contract.db")
			harness := &sqliteWarmAttachHarness{Store: openStore(t, path), path: path}
			ctx := context.Background()
			if _, err := harness.Append(ctx, "ses_target", []store.PendingEvent{{Type: "session.state", Time: testTime(1), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
				t.Fatal(err)
			}
			if _, err := harness.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "ses_bootstrap", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}); err != nil {
				t.Fatal(err)
			}
			if _, err := harness.AcceptAdapterHello(ctx, "ses_bootstrap", store.AdapterHello{CredentialGeneration: 1}); err != nil {
				t.Fatal(err)
			}
			if fence, err := harness.AllocateAdapterGrantFence(ctx); err != nil || fence != 2 {
				t.Fatalf("warm grant fence = %d, %v", fence, err)
			}
			return harness
		},
		Fail: func(t *testing.T, warm store.WarmAttachStore, failure storetest.WarmAttachFailure) {
			table := map[storetest.WarmAttachFailure]string{storetest.WarmAttachFailureAttempt: "session_attach_attempts", storetest.WarmAttachFailureAttachment: "session_attachments", storetest.WarmAttachFailureOutbox: "session_pending_commands", storetest.WarmAttachFailureSummary: "session_attention_summaries"}[failure]
			if table == "" {
				t.Fatalf("unknown warm failure %q", failure)
			}
			db := openRawSQLite(t, warm.(*sqliteWarmAttachHarness).path)
			if _, err := db.Exec(`CREATE TRIGGER warm_attach_failpoint BEFORE INSERT ON ` + table + ` BEGIN SELECT RAISE(ABORT, 'warm attach failpoint'); END`); err != nil {
				t.Fatal(err)
			}
		},
		Expire: func(t *testing.T, warm store.WarmAttachStore) {
			db := openRawSQLite(t, warm.(*sqliteWarmAttachHarness).path)
			if _, err := db.Exec(`UPDATE session_attachments SET expires_at_ns = (created_at_ms - 1000) * 1000000 + 1, created_at_ms = created_at_ms - 1000 WHERE attach_id = 'att_warm'`); err != nil {
				t.Fatal(err)
			}
		},
		Absent: func(t *testing.T, warm store.WarmAttachStore, _ store.WarmAttachRequest) {
			db := openRawSQLite(t, warm.(*sqliteWarmAttachHarness).path)
			if _, err := db.Exec(`DROP TRIGGER IF EXISTS warm_attach_failpoint`); err != nil {
				t.Fatal(err)
			}
			var attempts, attachments, commands, references int
			if err := db.QueryRow(`SELECT (SELECT COUNT(*) FROM session_attach_attempts WHERE attach_id = 'att_warm'), (SELECT COUNT(*) FROM session_attachments WHERE attach_id = 'att_warm'), (SELECT COUNT(*) FROM session_pending_commands WHERE cmd_id = 'cmd_warm'), (SELECT COUNT(*) FROM session_events WHERE session_id = 'ses_target' AND seq > 1)`).Scan(&attempts, &attachments, &commands, &references); err != nil {
				t.Fatal(err)
			}
			if attempts+attachments+commands+references != 0 {
				t.Fatalf("warm rollback left %d/%d/%d/%d", attempts, attachments, commands, references)
			}
			var connections int
			if err := db.QueryRow(`SELECT COUNT(*) FROM session_adapter_connections WHERE session_id = 'ses_target'`).Scan(&connections); err != nil || connections != 0 {
				t.Fatalf("warm rollback left target credential connections=%d err=%v", connections, err)
			}
		},
	})
}

type sqliteWarmAttachHarness struct {
	*sqlite.Store
	path string
}

func TestWarmAttachCommitDuplicateAndExpiry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "warm-attach.db")
	warm := openStore(t, path)
	ctx := context.Background()
	if _, err := warm.Append(ctx, "ses_warm_target", []store.PendingEvent{{Type: "session.state", Time: testTime(1), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
		t.Fatalf("seed warm target: %v", err)
	}
	if _, err := warm.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "ses_warm_bootstrap", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatalf("seed warm bootstrap: %v", err)
	}
	connection, err := warm.AcceptAdapterHello(ctx, "ses_warm_bootstrap", store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatalf("accept warm bootstrap: %v", err)
	}
	grantFence, err := warm.AllocateAdapterGrantFence(ctx)
	if err != nil {
		t.Fatalf("allocate warm grant fence: %v", err)
	}
	expiresAt := time.Now().Add(10 * time.Second)
	request := store.WarmAttachRequest{
		Attempt:            store.AttachAttemptRequest{Identity: store.AttachAttemptIdentity{JTIHash: [32]byte{1}, AttachID: "att_warm", BootstrapSessionID: "ses_warm_bootstrap", TargetSessionID: "ses_warm_target", Provider: "claude-code"}, Fingerprint: store.AttachAttemptFingerprint{Domain: "agentwharf.attach-request.v1", Version: 1, Digest: [32]byte{2}, KeyVersion: 1}, ExpiresAt: time.Now().Add(time.Minute), Outcome: store.AttachAttemptAccepted, IssuedCredentialGeneration: int64Pointer(1)},
		Attachment:         store.AttachmentCreate{Identity: store.AttachmentIdentity{AttachID: "att_warm", BootstrapSessionID: "ses_warm_bootstrap", TargetSessionID: "ses_warm_target", TargetCredentialLineageRef: "lineage_warm"}, ExpiresAt: expiresAt},
		TargetActivation:   store.WarmAttachTargetActivation{Generation: 1, ExpiresAt: expiresAt},
		BootstrapAdmission: store.AdapterConnectionAdmission{CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch, AcceptedFence: connection.AcceptedFence, GrantFence: grantFence},
		FirstDelivery:      store.WarmAttachFirstDelivery{CommandID: "cmd_warm", ReferenceID: "ref_warm", ReferenceDigest: [32]byte{3}, ExpiresAt: expiresAt},
	}
	commit, err := warm.CommitWarmAttach(ctx, request)
	if err != nil || commit.Duplicate || commit.Attachment.Status != store.AttachmentJoinPending || commit.Summary.Blocker == nil || commit.Summary.Blocker.Kind != store.AttentionBlockerQueued {
		t.Fatalf("commit warm attach = %+v, %v", commit, err)
	}
	target, err := warm.AdapterConnection(ctx, "ses_warm_target")
	if err != nil || target.ConnectionEpoch != 0 || target.AcceptedFence != 0 || target.ActiveCredentialGeneration != request.TargetActivation.Generation || target.CredentialGenerationHighWatermark != request.TargetActivation.Generation || target.ActiveCredentialExpiresAt.UnixMilli() != request.TargetActivation.ExpiresAt.UnixMilli() || target.RevokedAt != nil || target.TerminalAt != nil {
		t.Fatalf("commit target activation = %+v, %v", target, err)
	}
	if err := warm.ValidateWarmAttachTargetActivation(ctx, "ses_warm_target", request.TargetActivation); err != nil {
		t.Fatalf("validate current warm target activation: %v", err)
	}
	retry, err := warm.CommitWarmAttach(ctx, request)
	if err != nil || !retry.Duplicate || retry.Attempt.Identity != commit.Attempt.Identity || retry.Attachment.Identity != commit.Attachment.Identity || retry.Attachment.DeliveryVersion != commit.Attachment.DeliveryVersion || retry.Outbox.EventSeq != commit.Outbox.EventSeq || retry.Outbox.CommandID != commit.Outbox.CommandID || retry.Outbox.ReferenceID != commit.Outbox.ReferenceID || retry.Summary.SessionID != commit.Summary.SessionID || retry.Summary.SummaryVersion != commit.Summary.SummaryVersion {
		t.Fatalf("retry warm attach = %+v, %v", retry, err)
	}
	changed := request
	changed.FirstDelivery.ReferenceDigest[0]++
	if _, err := warm.CommitWarmAttach(ctx, changed); err == nil {
		t.Fatal("changed warm attach retry succeeded")
	}
	db := openRawSQLite(t, path)
	if _, err := db.Exec(`UPDATE session_adapter_connections SET active_credential_expires_at_ms = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) - 1, created_at_ms = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) - 2000, updated_at_ms = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) - 1000 WHERE session_id = 'ses_warm_target'`); err != nil {
		t.Fatalf("expire target activation: %v", err)
	}
	if err := warm.ValidateWarmAttachTargetActivation(ctx, "ses_warm_target", request.TargetActivation); err == nil {
		t.Fatal("expired target activation passed Store-clock recheck")
	}
	if _, err := db.Exec(`UPDATE session_attachments SET expires_at_ns = (created_at_ms - 1000) * 1000000 + 1, created_at_ms = created_at_ms - 1000 WHERE attach_id = 'att_warm'`); err != nil {
		t.Fatalf("expire warm attach: %v", err)
	}
	expired, err := warm.ExpireWarmAttach(ctx, "att_warm", 0)
	if err != nil || expired.Attachment.Status != store.AttachmentReauthorizationRequired || expired.Summary.Blocker == nil || expired.Summary.Blocker.Kind != store.AttentionBlockerReauthorizationRequired {
		t.Fatalf("expire warm attach = %+v, %v", expired, err)
	}
}

func TestWarmAttachRejectsMissingOrIncompleteTargetWithoutWrites(t *testing.T) {
	for _, target := range []string{"ses_warm_missing", "ses_warm_incomplete"} {
		t.Run(target, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "warm-attach-reject.db")
			warm := openStore(t, path)
			ctx := context.Background()
			if target == "ses_warm_incomplete" {
				db := openRawSQLite(t, path)
				if _, err := db.Exec(`INSERT INTO session_attention_summaries (session_id, latest_seq, state, summary_version, projection_state, created_at_ms, updated_at_ms) VALUES (?, 0, 'starting', 0, 'incomplete', 1, 1)`, target); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := warm.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "ses_warm_bootstrap", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}); err != nil {
				t.Fatal(err)
			}
			connection, err := warm.AcceptAdapterHello(ctx, "ses_warm_bootstrap", store.AdapterHello{CredentialGeneration: 1})
			if err != nil {
				t.Fatal(err)
			}
			grant, err := warm.AllocateAdapterGrantFence(ctx)
			if err != nil {
				t.Fatal(err)
			}
			expires := time.Now().Add(10 * time.Second)
			request := store.WarmAttachRequest{Attempt: store.AttachAttemptRequest{Identity: store.AttachAttemptIdentity{JTIHash: [32]byte{9}, AttachID: "att_reject", BootstrapSessionID: "ses_warm_bootstrap", TargetSessionID: target, Provider: "claude-code"}, Fingerprint: store.AttachAttemptFingerprint{Domain: "agentwharf.attach-request.v1", Version: 1, Digest: [32]byte{8}, KeyVersion: 1}, ExpiresAt: time.Now().Add(time.Minute), Outcome: store.AttachAttemptAccepted, IssuedCredentialGeneration: int64Pointer(1)}, Attachment: store.AttachmentCreate{Identity: store.AttachmentIdentity{AttachID: "att_reject", BootstrapSessionID: "ses_warm_bootstrap", TargetSessionID: target, TargetCredentialLineageRef: "lineage_reject"}, ExpiresAt: expires}, TargetActivation: store.WarmAttachTargetActivation{Generation: 1, ExpiresAt: expires}, BootstrapAdmission: store.AdapterConnectionAdmission{CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch, AcceptedFence: connection.AcceptedFence, GrantFence: grant}, FirstDelivery: store.WarmAttachFirstDelivery{CommandID: "cmd_reject", ReferenceID: "ref_reject", ReferenceDigest: [32]byte{7}, ExpiresAt: expires}}
			if _, err := warm.CommitWarmAttach(ctx, request); err == nil {
				t.Fatal("untrusted target committed warm attach")
			}
			db := openRawSQLite(t, path)
			var attempts, attachments, events, commands int
			if err := db.QueryRow(`SELECT (SELECT COUNT(*) FROM session_attach_attempts WHERE attach_id = 'att_reject'), (SELECT COUNT(*) FROM session_attachments WHERE attach_id = 'att_reject'), (SELECT COUNT(*) FROM session_events WHERE session_id = ?), (SELECT COUNT(*) FROM session_pending_commands WHERE cmd_id = 'cmd_reject')`, target).Scan(&attempts, &attachments, &events, &commands); err != nil {
				t.Fatal(err)
			}
			if attempts+attachments+events+commands != 0 {
				t.Fatalf("rejected warm attach wrote attempts=%d attachments=%d events=%d commands=%d", attempts, attachments, events, commands)
			}
		})
	}
}

func TestWarmAttachRejectsFullAdmissionTupleAndTerminalTarget(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(t *testing.T, warm *sqlite.Store, path string, request *store.WarmAttachRequest)
	}{
		{name: "wrong_credential_generation", mutate: func(_ *testing.T, _ *sqlite.Store, _ string, request *store.WarmAttachRequest) {
			request.BootstrapAdmission.CredentialGeneration++
		}},
		{name: "grant_does_not_advance_fence", mutate: func(_ *testing.T, _ *sqlite.Store, _ string, request *store.WarmAttachRequest) {
			request.BootstrapAdmission.GrantFence = request.BootstrapAdmission.AcceptedFence
		}},
		{name: "stale_hello_epoch", mutate: func(t *testing.T, warm *sqlite.Store, _ string, _ *store.WarmAttachRequest) {
			if _, err := warm.AcceptAdapterHello(context.Background(), "ses_warm_bootstrap", store.AdapterHello{CredentialGeneration: 1}); err != nil {
				t.Fatalf("advance bootstrap hello: %v", err)
			}
		}},
		{name: "revoked_bootstrap", mutate: func(t *testing.T, _ *sqlite.Store, path string, _ *store.WarmAttachRequest) {
			db := openRawSQLite(t, path)
			now := time.Now().UnixMilli()
			if _, err := db.Exec(`UPDATE session_adapter_connections SET revoked_at_ms = ?, updated_at_ms = ? WHERE session_id = 'ses_warm_bootstrap'`, now, now); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "expired_bootstrap", mutate: func(t *testing.T, _ *sqlite.Store, path string, _ *store.WarmAttachRequest) {
			db := openRawSQLite(t, path)
			now := time.Now().UnixMilli()
			if _, err := db.Exec(`UPDATE session_adapter_connections SET created_at_ms = ?, active_credential_expires_at_ms = ?, updated_at_ms = ? WHERE session_id = 'ses_warm_bootstrap'`, now-int64((2*time.Minute)/time.Millisecond), now-1, now); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "terminal_target", mutate: func(t *testing.T, warm *sqlite.Store, _ string, _ *store.WarmAttachRequest) {
			if _, err := warm.Append(context.Background(), "ses_warm_target", []store.PendingEvent{{Type: "session.state", Time: time.Now(), Payload: []byte(`{"state":"ended"}`)}}); err != nil {
				t.Fatalf("end warm target: %v", err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "warm-attach-auth.db")
			warm, request := newSQLiteWarmAttach(t, path)
			test.mutate(t, warm, path, &request)
			if _, err := warm.CommitWarmAttach(context.Background(), request); err == nil {
				t.Fatal("fenced warm attach committed")
			}
			assertSQLiteWarmAttachRowsAbsent(t, path, request)
		})
	}
}

func TestWarmAttachWALRetryReopenExpiryAndCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "warm-attach-wal.db")
	first, request := newSQLiteWarmAttach(t, path)
	second := openStore(t, path)

	start := make(chan struct{})
	type result struct {
		commit store.WarmAttachCommit
		err    error
	}
	results := make(chan result, 2)
	var group sync.WaitGroup
	for _, warm := range []*sqlite.Store{first, second} {
		group.Add(1)
		go func(warm *sqlite.Store) {
			defer group.Done()
			<-start
			commit, err := warm.CommitWarmAttach(context.Background(), request)
			results <- result{commit: commit, err: err}
		}(warm)
	}
	close(start)
	group.Wait()
	close(results)
	var committed, duplicate store.WarmAttachCommit
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent cross-handle warm attach: %v", result.err)
		}
		if result.commit.Duplicate {
			duplicate = result.commit
		} else {
			committed = result.commit
		}
	}
	if committed.Attachment.Identity != request.Attachment.Identity || duplicate.Attachment.Identity != committed.Attachment.Identity || !duplicate.Duplicate {
		t.Fatalf("cross-handle warm attach outcomes committed=%+v duplicate=%+v", committed, duplicate)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first warm store: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("close second warm store: %v", err)
	}

	reopened := openStore(t, path)
	retry, err := reopened.CommitWarmAttach(context.Background(), request)
	if err != nil || !retry.Duplicate || retry.Attempt.Identity != committed.Attempt.Identity || retry.Outbox != committed.Outbox || !reflect.DeepEqual(retry.Summary, committed.Summary) {
		t.Fatalf("reopened exact retry = %+v, %v; want %+v", retry, err, committed)
	}
	assertSQLiteWarmAttachDurableRows(t, path, request, committed)
	db := openRawSQLite(t, path)
	if _, err := db.Exec(`UPDATE session_attachments SET expires_at_ns = (created_at_ms - 1000) * 1000000 + 1, created_at_ms = created_at_ms - 1000 WHERE attach_id = ?`, request.Attachment.Identity.AttachID); err != nil {
		t.Fatalf("expire reopened warm attachment: %v", err)
	}
	expired, err := reopened.ExpireWarmAttach(context.Background(), request.Attachment.Identity.AttachID, committed.Attachment.DeliveryVersion)
	if err != nil || expired.Attachment.Status != store.AttachmentReauthorizationRequired || expired.Summary.Blocker == nil || expired.Summary.Blocker.Kind != store.AttentionBlockerReauthorizationRequired {
		t.Fatalf("reopened warm expiry = %+v, %v", expired, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close expired warm store: %v", err)
	}

	final := openStore(t, path)
	finalSummary, err := final.AttentionSnapshot(context.Background(), []string{request.Attachment.Identity.TargetSessionID})
	if err != nil || len(finalSummary) != 1 || !reflect.DeepEqual(finalSummary[0], expired.Summary) {
		t.Fatalf("reopened expired summary = %+v, %v; want %+v", finalSummary, err, expired.Summary)
	}
	db = openRawSQLite(t, path)
	if _, err := db.Exec(`PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("allow controlled corrupt warm outbox fixture: %v", err)
	}
	if _, err := db.Exec(`UPDATE session_pending_commands SET type = 'corrupt' WHERE cmd_id = ?`, request.FirstDelivery.CommandID); err != nil {
		t.Fatalf("corrupt durable warm outbox: %v", err)
	}
	if _, err := final.CommitWarmAttach(context.Background(), request); err == nil {
		t.Fatal("corrupt warm-attach retry unexpectedly succeeded")
	}
	assertSQLiteWarmAttachDurableRows(t, path, request, committed)
}

func TestWarmAttachCredentialReceiptSurvivesReopenAndFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "warm-attach-credential-receipt.db")
	warm, request := newSQLiteWarmAttach(t, path)
	committed, err := warm.CommitWarmAttach(context.Background(), request)
	if err != nil {
		t.Fatalf("commit warm attach: %v", err)
	}
	claimed, err := warm.UpdateAttachment(context.Background(), committed.Attachment.Identity.AttachID, committed.Attachment.DeliveryVersion, store.AttachmentUpdate{
		Status: store.AttachmentJoinPending, DeliveryState: store.AttachmentDeliveryReceived, ExpiresAt: committed.Attachment.ExpiresAt,
	})
	if err != nil || claimed.Attachment.DeliveryState != store.AttachmentDeliveryReceived {
		t.Fatalf("claim credential receipt = %+v, %v", claimed, err)
	}
	if err := warm.Close(); err != nil {
		t.Fatalf("close claimed receipt store: %v", err)
	}
	reopened := openStore(t, path)
	duplicate, err := reopened.CommitWarmAttach(context.Background(), request)
	if err != nil || !duplicate.Duplicate || duplicate.Attachment.DeliveryState != store.AttachmentDeliveryReceived {
		t.Fatalf("reopened credential receipt = %+v, %v", duplicate, err)
	}
	db := openRawSQLite(t, path)
	if _, err := db.Exec(`UPDATE session_attachments SET expires_at_ns = (created_at_ms - 1000) * 1000000 + 1, created_at_ms = created_at_ms - 1000 WHERE attach_id = ?`, duplicate.Attachment.Identity.AttachID); err != nil {
		t.Fatalf("expire claimed credential receipt: %v", err)
	}
	expired, err := reopened.ExpireWarmAttach(context.Background(), duplicate.Attachment.Identity.AttachID, duplicate.Attachment.DeliveryVersion)
	if err != nil || expired.Attachment.Status != store.AttachmentReauthorizationRequired || expired.Attachment.DeliveryState != store.AttachmentDeliveryOutcomeUnknown || expired.Summary.Blocker == nil || expired.Summary.Blocker.Kind != store.AttentionBlockerOutcomeUnknown || expired.Summary.Blocker.Operation == nil || *expired.Summary.Blocker.Operation != "credential_handoff" {
		t.Fatalf("expire claimed credential receipt = %+v, %v", expired, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close failed receipt store: %v", err)
	}
	final := openStore(t, path)
	defer final.Close()
	retry, err := final.CommitWarmAttach(context.Background(), request)
	if err != nil || !retry.Duplicate || retry.Attachment.Status != store.AttachmentReauthorizationRequired || retry.Attachment.DeliveryState != store.AttachmentDeliveryOutcomeUnknown {
		t.Fatalf("final credential receipt = %+v, %v", retry, err)
	}
}

func newSQLiteWarmAttach(t *testing.T, path string) (*sqlite.Store, store.WarmAttachRequest) {
	t.Helper()
	warm := openStore(t, path)
	ctx := context.Background()
	if _, err := warm.Append(ctx, "ses_warm_target", []store.PendingEvent{{Type: "session.state", Time: testTime(1), Payload: []byte(`{"state":"ready"}`)}}); err != nil {
		t.Fatalf("seed warm target: %v", err)
	}
	if _, err := warm.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: "ses_warm_bootstrap", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatalf("seed warm bootstrap: %v", err)
	}
	connection, err := warm.AcceptAdapterHello(ctx, "ses_warm_bootstrap", store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatalf("accept warm bootstrap: %v", err)
	}
	grantFence, err := warm.AllocateAdapterGrantFence(ctx)
	if err != nil {
		t.Fatalf("allocate warm grant fence: %v", err)
	}
	expiresAt := time.Now().Add(10 * time.Second)
	return warm, store.WarmAttachRequest{
		Attempt:            store.AttachAttemptRequest{Identity: store.AttachAttemptIdentity{JTIHash: [32]byte{4}, AttachID: "att_warm", BootstrapSessionID: "ses_warm_bootstrap", TargetSessionID: "ses_warm_target", Provider: "claude-code"}, Fingerprint: store.AttachAttemptFingerprint{Domain: "agentwharf.attach-request.v1", Version: 1, Digest: [32]byte{5}, KeyVersion: 1}, ExpiresAt: time.Now().Add(time.Minute), Outcome: store.AttachAttemptAccepted, IssuedCredentialGeneration: int64Pointer(1)},
		Attachment:         store.AttachmentCreate{Identity: store.AttachmentIdentity{AttachID: "att_warm", BootstrapSessionID: "ses_warm_bootstrap", TargetSessionID: "ses_warm_target", TargetCredentialLineageRef: "lineage_warm"}, ExpiresAt: expiresAt},
		TargetActivation:   store.WarmAttachTargetActivation{Generation: 1, ExpiresAt: expiresAt},
		BootstrapAdmission: store.AdapterConnectionAdmission{CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch, AcceptedFence: connection.AcceptedFence, GrantFence: grantFence},
		FirstDelivery:      store.WarmAttachFirstDelivery{CommandID: "cmd_warm", ReferenceID: "ref_warm", ReferenceDigest: [32]byte{6}, ExpiresAt: expiresAt},
	}
}

func assertSQLiteWarmAttachRowsAbsent(t *testing.T, path string, request store.WarmAttachRequest) {
	t.Helper()
	db := openRawSQLite(t, path)
	var attempts, attachments, commands, references int
	if err := db.QueryRow(`SELECT (SELECT COUNT(*) FROM session_attach_attempts WHERE attach_id = ?), (SELECT COUNT(*) FROM session_attachments WHERE attach_id = ?), (SELECT COUNT(*) FROM session_pending_commands WHERE cmd_id = ?), (SELECT COUNT(*) FROM session_events WHERE session_id = ? AND payload = ?)`, request.Attempt.Identity.AttachID, request.Attachment.Identity.AttachID, request.FirstDelivery.CommandID, request.Attachment.Identity.TargetSessionID, sqliteWarmAttachPayloadForTest(request.FirstDelivery)).Scan(&attempts, &attachments, &commands, &references); err != nil {
		t.Fatal(err)
	}
	if attempts+attachments+commands+references != 0 {
		t.Fatalf("fenced warm attach left rows attempts=%d attachments=%d commands=%d references=%d", attempts, attachments, commands, references)
	}
}

func assertSQLiteWarmAttachDurableRows(t *testing.T, path string, request store.WarmAttachRequest, committed store.WarmAttachCommit) {
	t.Helper()
	db := openRawSQLite(t, path)
	var attempts, attachments, commands, references int
	if err := db.QueryRow(`SELECT (SELECT COUNT(*) FROM session_attach_attempts WHERE attach_id = ?), (SELECT COUNT(*) FROM session_attachments WHERE attach_id = ?), (SELECT COUNT(*) FROM session_pending_commands WHERE cmd_id = ?), (SELECT COUNT(*) FROM session_events WHERE session_id = ? AND seq = ? AND payload = ?)`, request.Attempt.Identity.AttachID, request.Attachment.Identity.AttachID, request.FirstDelivery.CommandID, request.Attachment.Identity.TargetSessionID, committed.Outbox.EventSeq, sqliteWarmAttachPayloadForTest(request.FirstDelivery)).Scan(&attempts, &attachments, &commands, &references); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || attachments != 1 || commands != 1 || references != 1 {
		t.Fatalf("warm attach durable rows = %d/%d/%d/%d", attempts, attachments, commands, references)
	}
}

func sqliteWarmAttachPayloadForTest(delivery store.WarmAttachFirstDelivery) []byte {
	return []byte(fmt.Sprintf(`{"role":"user","reference_id":"%s","reference_digest":"%x"}`, delivery.ReferenceID, delivery.ReferenceDigest))
}

func int64Pointer(value int64) *int64 { return &value }

func TestAttentionSnapshotPersistsEventsLedgerAndTerminalFence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	attention := openStore(t, path)
	ctx := context.Background()
	if _, err := attention.Append(ctx, "ses_attention", []store.PendingEvent{
		{Type: "session.state", Time: testTime(1), Payload: []byte(`{"state":"starting"}`)},
		{Type: "session.state", Time: testTime(2), Payload: []byte(`{"state":"working"}`)},
		{Type: "session.message", Time: testTime(3), Payload: []byte(`{"role":"agent"}`)},
	}); err != nil {
		t.Fatalf("append attention events: %v", err)
	}
	summary, err := attention.AttentionSnapshot(ctx, []string{"ses_attention", "ses_missing"})
	if err != nil || len(summary) != 1 || summary[0].LatestSeq != 3 || summary[0].State != "busy" ||
		summary[0].LatestChangeSeq == nil || *summary[0].LatestChangeSeq != 2 || summary[0].LastDurableEventAt == nil ||
		summary[0].StateOfProjection != store.AttentionProjectionComplete {
		t.Fatalf("event attention snapshot = %+v, %v", summary, err)
	}
	if err := attention.Close(); err != nil {
		t.Fatalf("close attention store: %v", err)
	}
	attention = openStore(t, path)
	reopened, err := attention.AttentionSnapshot(ctx, []string{"ses_attention"})
	if err != nil || len(reopened) != 1 || !reflect.DeepEqual(reopened[0], summary[0]) {
		t.Fatalf("reopened attention snapshot = %+v, %v; want %+v", reopened, err, summary)
	}
}

func TestAttentionSnapshotProjectsClientLedgerAttachmentAndTerminal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	attention := openStore(t, path)
	seedCommandAuthorities(t, path)
	ctx := context.Background()
	request := store.PendingCommandRequest{CommandID: "cmd_attention", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
	if _, err := attention.CommitPendingCommand(ctx, "ses_command_1", store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}, store.PendingEvent{Type: "session.message", Time: testTime(1), Payload: []byte(`{"role":"user"}`)}, request); err != nil {
		t.Fatalf("commit attention command: %v", err)
	}
	summary, err := attention.AttentionSnapshot(ctx, []string{"ses_command_1"})
	if err != nil || len(summary) != 1 || summary[0].LatestSeq != 1 || summary[0].SummaryVersion != 1 || summary[0].LastClientCommandAt == nil || summary[0].Blocker != nil {
		t.Fatalf("command attention snapshot = %+v, %v", summary, err)
	}
	attachment := store.AttachmentCreate{Identity: store.AttachmentIdentity{AttachID: "att_attention", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_command_1", TargetCredentialLineageRef: "lineage_attention"}, ExpiresAt: time.Now().Add(10 * time.Second)}
	if _, err := attention.CreateAttachment(ctx, attachment); err != nil {
		t.Fatalf("create attention attachment: %v", err)
	}
	summary, err = attention.AttentionSnapshot(ctx, []string{"ses_command_1"})
	if err != nil || len(summary) != 1 || summary[0].SummaryVersion != 2 || summary[0].Blocker == nil || summary[0].Blocker.Kind != store.AttentionBlockerQueued || summary[0].LastClientCommandAt == nil {
		t.Fatalf("attachment attention snapshot = %+v, %v", summary, err)
	}
	if _, err := attention.Append(ctx, "ses_command_1", []store.PendingEvent{{Type: "session.state", Time: testTime(2), Payload: []byte(`{"state":"ended"}`)}}); err != nil {
		t.Fatalf("append terminal attention event: %v", err)
	}
	summary, err = attention.AttentionSnapshot(ctx, []string{"ses_command_1"})
	if err != nil || len(summary) != 1 || summary[0].TerminalOutcome == nil || *summary[0].TerminalOutcome != "ended" {
		t.Fatalf("terminal attention snapshot = %+v, %v", summary, err)
	}
	connection, err := attention.AdapterConnection(ctx, "ses_command_1")
	if err != nil || connection.TerminalAt == nil || connection.RevokedAt == nil {
		t.Fatalf("terminal attention fence = %+v, %v", connection, err)
	}
}

func TestAttentionSnapshotProjectsUnknownCommandOutcome(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	attention := openStore(t, path)
	seedCommandAuthorities(t, path)
	ctx := context.Background()
	authority := store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
	request := store.PendingCommandRequest{CommandID: "cmd_attention_unknown", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
	if _, err := attention.CommitPendingCommand(ctx, "ses_command_1", authority, store.PendingEvent{Type: "session.message", Time: testTime(1), Payload: []byte(`{"role":"user"}`)}, request); err != nil {
		t.Fatalf("commit attention command: %v", err)
	}
	if _, err := attention.ClaimPendingCommand(ctx, "ses_command_1", authority, request.CommandID); err != nil {
		t.Fatalf("claim attention command: %v", err)
	}
	if _, err := attention.ResolvePendingCommand(ctx, "ses_command_1", authority, request.CommandID, store.PendingCommandOutcomeUnknown); err != nil {
		t.Fatalf("resolve unknown attention command: %v", err)
	}
	summary, err := attention.AttentionSnapshot(ctx, []string{"ses_command_1"})
	if err != nil || len(summary) != 1 || summary[0].SummaryVersion != 2 || summary[0].Blocker == nil || summary[0].Blocker.Kind != store.AttentionBlockerOutcomeUnknown || summary[0].Blocker.Operation == nil || *summary[0].Blocker.Operation != "command" {
		t.Fatalf("unknown command attention snapshot = %+v, %v", summary, err)
	}
}

func TestAttentionBackfillCheckpointFailsClosedAndBatchesLegacySessions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	attention := openStore(t, path)
	ctx := context.Background()
	if _, err := attention.CreateAttachment(ctx, store.AttachmentCreate{Identity: store.AttachmentIdentity{
		AttachID: "att_legacy", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_legacy_attachment", TargetCredentialLineageRef: "lineage_legacy",
	}, ExpiresAt: time.Now().Add(10 * time.Second)}); err != nil {
		t.Fatalf("seed legacy attachment: %v", err)
	}
	connection, err := attention.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{
		SessionID: "ses_legacy_connection", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("seed legacy connection: %v", err)
	}
	connection, err = attention.AcceptAdapterHello(ctx, connection.SessionID, store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatalf("accept legacy connection hello: %v", err)
	}
	grantFence, err := attention.AllocateAdapterGrantFence(ctx)
	if err != nil {
		t.Fatalf("allocate legacy connection grant fence: %v", err)
	}
	if err := attention.Close(); err != nil {
		t.Fatalf("close seed store: %v", err)
	}
	db := openRawSQLite(t, path)
	if _, err := db.Exec(`DELETE FROM session_attention_migration`); err != nil {
		t.Fatalf("clear migration marker: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM session_attention_summaries`); err != nil {
		t.Fatalf("clear forward summaries: %v", err)
	}
	for index := 0; index < 257; index++ {
		sessionID := fmt.Sprintf("ses_legacy_%03d", index)
		if _, err := db.Exec(`INSERT INTO session_events (session_id, seq, type, payload, event_time_ms, created_at_ms) VALUES (?, 1, 'session.state', '{"state":"ready"}', 1, 1)`, sessionID); err != nil {
			t.Fatalf("seed legacy session %d: %v", index, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy fixture: %v", err)
	}
	attention = openStore(t, path)
	snapshot, err := attention.AttentionSnapshot(ctx, []string{"ses_legacy_000", "ses_legacy_attachment", "ses_legacy_connection"})
	if err != nil || len(snapshot) != 3 || snapshot[0].StateOfProjection != store.AttentionProjectionIncomplete ||
		snapshot[1].StateOfProjection != store.AttentionProjectionIncomplete || snapshot[2].StateOfProjection != store.AttentionProjectionIncomplete {
		t.Fatalf("pending attention snapshot = %+v, %v", snapshot, err)
	}
	if _, err := attention.ValidateAdapterAdmission(ctx, connection.SessionID, store.AdapterConnectionAdmission{
		CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch, AcceptedFence: connection.AcceptedFence, GrantFence: grantFence,
	}); err == nil {
		t.Fatal("pending attention migration admitted legacy adapter")
	}
	first, err := attention.BackfillAttentionBatch(ctx, 256)
	if err != nil || first.Processed != 256 || first.Done || first.Checkpoint != "ses_legacy_255" {
		t.Fatalf("first attention backfill = %+v, %v", first, err)
	}
	if err := attention.Close(); err != nil {
		t.Fatalf("close checkpointed store: %v", err)
	}
	attention = openStore(t, path)
	t.Cleanup(func() { _ = attention.Close() })
	second, err := attention.BackfillAttentionBatch(ctx, 256)
	if err != nil || second.Processed != 3 || !second.Done || second.Checkpoint != "ses_legacy_connection" {
		t.Fatalf("second attention backfill = %+v, %v", second, err)
	}
	snapshot, err = attention.AttentionSnapshot(ctx, []string{"ses_legacy_000", "ses_legacy_attachment", "ses_legacy_connection"})
	if err != nil || len(snapshot) != 3 || snapshot[0].StateOfProjection != store.AttentionProjectionIncomplete ||
		snapshot[1].StateOfProjection != store.AttentionProjectionIncomplete || snapshot[2].StateOfProjection != store.AttentionProjectionIncomplete {
		t.Fatalf("backfilled attention snapshot = %+v, %v", snapshot, err)
	}
	fenced, err := attention.AdapterConnection(ctx, connection.SessionID)
	if err != nil || fenced.RevokedAt == nil || fenced.TerminalAt == nil {
		t.Fatalf("backfilled legacy connection fence = %+v, %v", fenced, err)
	}
}

func TestAttentionMigrationRecoversSchemaCreatedBeforeMarker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	db := openRawSQLite(t, path)
	if _, err := db.Exec(`CREATE TABLE session_events (
    id INTEGER PRIMARY KEY, session_id TEXT NOT NULL, seq INTEGER NOT NULL, type TEXT NOT NULL,
    payload BLOB NOT NULL, event_time_ms INTEGER NOT NULL, created_at_ms INTEGER NOT NULL
)`); err != nil {
		t.Fatalf("seed crash-after-schema fixture: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close crash-after-schema fixture: %v", err)
	}
	attention := openStore(t, path)
	ctx := context.Background()
	snapshot, err := attention.AttentionSnapshot(ctx, []string{"ses_crash_marker"})
	if err != nil || len(snapshot) != 1 || snapshot[0].StateOfProjection != store.AttentionProjectionIncomplete {
		t.Fatalf("crash-after-schema snapshot = %+v, %v", snapshot, err)
	}
	result, err := attention.BackfillAttentionBatch(ctx, 1)
	if err != nil || result.Processed != 0 || !result.Done || result.Checkpoint != "" {
		t.Fatalf("crash-after-schema backfill = %+v, %v", result, err)
	}
}

func TestAttentionMigrationMarkerUsesActiveAdapterTransaction(t *testing.T) {
	attention := openStore(t, filepath.Join(t.TempDir(), "events.db"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	connection, err := attention.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{
		SessionID: "ses_marker_tx", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("initialize marker transaction connection: %v", err)
	}
	connection, err = attention.AcceptAdapterHello(ctx, connection.SessionID, store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatalf("accept marker transaction hello: %v", err)
	}
	grantFence, err := attention.AllocateAdapterGrantFence(ctx)
	if err != nil {
		t.Fatalf("allocate marker transaction grant fence: %v", err)
	}
	admission := store.AdapterConnectionAdmission{CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch, AcceptedFence: connection.AcceptedFence, GrantFence: grantFence}
	if err := attention.WithAdapterConnectionTransaction(ctx, func(current store.AdapterConnectionStore) error {
		_, err := current.(*sqlite.Store).ValidateAdapterEffectAdmission(ctx, connection.SessionID, admission)
		return err
	}); err != nil {
		t.Fatalf("validate marker effect admission: %v", err)
	}
	if _, err := attention.AppendAdapterEvents(ctx, connection.SessionID, admission, []store.PendingEvent{
		{Type: "session.state", Time: testTime(1), Payload: []byte(`{"state":"ready"}`)},
	}); err != nil {
		t.Fatalf("append marker transaction event: %v", err)
	}
}

func TestAttentionBackfillPreservesLivePendingSummary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	attention := openStore(t, path)
	if err := attention.Close(); err != nil {
		t.Fatalf("close live pending seed store: %v", err)
	}
	db := openRawSQLite(t, path)
	if _, err := db.Exec(`DELETE FROM session_attention_migration; DELETE FROM session_attention_summaries; INSERT INTO session_events (session_id, seq, type, payload, event_time_ms, created_at_ms) VALUES ('ses_live_pending', 1, 'session.state', '{"state":"ready"}', 1, 1)`); err != nil {
		t.Fatalf("seed live pending legacy event: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close live pending legacy fixture: %v", err)
	}
	attention = openStore(t, path)
	ctx := context.Background()
	if _, err := attention.Append(ctx, "ses_live_pending", []store.PendingEvent{{Type: "session.state", Time: testTime(2), Payload: []byte(`{"state":"ended"}`)}}); err != nil {
		t.Fatalf("append live pending terminal: %v", err)
	}
	if _, err := attention.CreateAttachment(ctx, store.AttachmentCreate{Identity: store.AttachmentIdentity{
		AttachID: "att_live_pending", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_live_pending", TargetCredentialLineageRef: "lineage_live_pending",
	}, ExpiresAt: time.Now().Add(10 * time.Second)}); err != nil {
		t.Fatalf("create live pending attachment: %v", err)
	}
	result, err := attention.BackfillAttentionBatch(ctx, 1)
	if err != nil || result.Processed != 1 || !result.Done {
		t.Fatalf("backfill live pending summary = %+v, %v", result, err)
	}
	if err := attention.Close(); err != nil {
		t.Fatalf("close backfilled live pending store: %v", err)
	}
	attention = openStore(t, path)
	t.Cleanup(func() { _ = attention.Close() })
	snapshot, err := attention.AttentionSnapshot(ctx, []string{"ses_live_pending"})
	if err != nil || len(snapshot) != 1 || snapshot[0].LatestSeq != 2 || snapshot[0].TerminalOutcome == nil || *snapshot[0].TerminalOutcome != "ended" ||
		snapshot[0].Blocker == nil || snapshot[0].Blocker.Kind != store.AttentionBlockerQueued || snapshot[0].LastDurableEventAt == nil ||
		snapshot[0].StateOfProjection != store.AttentionProjectionIncomplete {
		t.Fatalf("backfilled live pending snapshot = %+v, %v", snapshot, err)
	}
}

func TestAttentionSnapshotProjectsProposedTerminal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	attention := openStore(t, path)
	seedProposalAuthorities(t, path)
	ctx := context.Background()
	receipt, err := attention.CommitProposedEvent(ctx, "ses_proposal_1", store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}, store.ProposedEventRequest{
		ProposalID: "proposal_attention_terminal",
		Event:      store.PendingEvent{Type: "session.state", Time: testTime(1), Payload: []byte(`{"state":"ended"}`)},
	})
	if err != nil || receipt.Seq != 1 {
		t.Fatalf("commit terminal proposal = %+v, %v", receipt, err)
	}
	summary, err := attention.AttentionSnapshot(ctx, []string{"ses_proposal_1"})
	if err != nil || len(summary) != 1 || summary[0].TerminalOutcome == nil || *summary[0].TerminalOutcome != "ended" || summary[0].StateOfProjection != store.AttentionProjectionComplete {
		t.Fatalf("terminal proposal attention snapshot = %+v, %v", summary, err)
	}
	connection, err := attention.AdapterConnection(ctx, "ses_proposal_1")
	if err != nil || connection.TerminalAt == nil || connection.RevokedAt == nil {
		t.Fatalf("terminal proposal fence = %+v, %v", connection, err)
	}
}

func TestAttentionSnapshotProjectsPermissionAndValidatesInput(t *testing.T) {
	attention := openStore(t, filepath.Join(t.TempDir(), "events.db"))
	ctx := context.Background()
	for _, sessionIDs := range [][]string{nil, {"ses_attention", "ses_attention"}} {
		if _, err := attention.AttentionSnapshot(ctx, sessionIDs); err == nil {
			t.Fatalf("invalid attention snapshot %v succeeded", sessionIDs)
		}
	}
	if _, err := attention.Append(ctx, "ses_attention_permission", []store.PendingEvent{
		{Type: "session.state", Time: testTime(1), Payload: []byte(`{"state":"ready"}`)},
		{Type: "permission.request", Time: testTime(2), Payload: []byte(`{"request_id":"permission_1"}`)},
	}); err != nil {
		t.Fatalf("append attention permission request: %v", err)
	}
	summary, err := attention.AttentionSnapshot(ctx, []string{"ses_attention_permission"})
	if err != nil || len(summary) != 1 || summary[0].Permission == nil || summary[0].Permission.ID != "permission_1" || summary[0].Permission.Status != store.AttentionPermissionPending {
		t.Fatalf("permission request attention snapshot = %+v, %v", summary, err)
	}
	if _, err := attention.Append(ctx, "ses_attention_permission", []store.PendingEvent{{Type: "permission.decision", Time: testTime(3), Payload: []byte(`{"request_id":"permission_1","decision":"approve"}`)}}); err != nil {
		t.Fatalf("append attention permission decision: %v", err)
	}
	summary, err = attention.AttentionSnapshot(ctx, []string{"ses_attention_permission"})
	if err != nil || len(summary) != 1 || summary[0].Permission != nil {
		t.Fatalf("permission decision attention snapshot = %+v, %v", summary, err)
	}
}

func TestAttentionSnapshotConcurrentAppend(t *testing.T) {
	attention := openStore(t, filepath.Join(t.TempDir(), "events.db"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const events = 24
	appendDone := make(chan error, 1)
	go func() {
		for index := 0; index < events; index++ {
			state := "ready"
			if index%2 == 1 {
				state = "working"
			}
			if _, err := attention.Append(ctx, "ses_attention_concurrent", []store.PendingEvent{{Type: "session.state", Time: testTime(int64(index + 1)), Payload: []byte(`{"state":"` + state + `"}`)}}); err != nil {
				appendDone <- err
				return
			}
		}
		appendDone <- nil
	}()
	for {
		select {
		case err := <-appendDone:
			if err != nil {
				t.Fatalf("concurrent attention append: %v", err)
			}
			goto complete
		default:
			if _, err := attention.AttentionSnapshot(ctx, []string{"ses_attention_concurrent"}); err != nil {
				t.Fatalf("concurrent attention snapshot: %v", err)
			}
		}
	}

complete:
	summary, err := attention.AttentionSnapshot(context.Background(), []string{"ses_attention_concurrent"})
	if err != nil || len(summary) != 1 || summary[0].LatestSeq != events || summary[0].State != "busy" || summary[0].StateOfProjection != store.AttentionProjectionComplete {
		t.Fatalf("final concurrent attention snapshot = %+v, %v", summary, err)
	}
}

func TestHistoryStoreContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	storetest.HistoryContract(t, storetest.HistoryHarness{
		Open: func(t *testing.T) store.HistoryStore {
			return openStore(t, path)
		},
		Reopen: func(t *testing.T, current store.HistoryStore) store.HistoryStore {
			t.Helper()
			if err := current.(*sqlite.Store).Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			return openStore(t, path)
		},
		PruneBefore: func(t *testing.T, _ store.HistoryStore, sessionID string, beforeSeq int64) {
			t.Helper()
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatalf("open prune database: %v", err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Fatalf("close prune database: %v", err)
				}
			})
			if _, err := db.ExecContext(context.Background(), `
				DELETE FROM session_events WHERE session_id = ? AND seq < ?
			`, sessionID, beforeSeq); err != nil {
				t.Fatalf("prune history: %v", err)
			}
		},
	})
}

func TestPendingCommandStoreContract(t *testing.T) {
	storetest.PendingCommandContract(t, storetest.PendingCommandHarness{
		Open: func(t *testing.T) store.CommandLedgerStore {
			t.Helper()
			path := filepath.Join(t.TempDir(), "events.db")
			harness := &sqliteCommandHarness{Store: openStore(t, path), path: path}
			seedCommandAuthorities(t, path)
			return harness
		},
		Reopen: func(t *testing.T, current store.CommandLedgerStore) store.CommandLedgerStore {
			t.Helper()
			harness := current.(*sqliteCommandHarness)
			if err := harness.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			harness.Store = openStore(t, harness.path)
			return harness
		},
		Authority: func(t *testing.T, _ store.CommandLedgerStore) store.CommandAuthority {
			t.Helper()
			return store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
		},
		Invalidate: invalidateCommandAuthority,
	})
}

func TestProposalStoreContract(t *testing.T) {
	storetest.ProposalContract(t, storetest.ProposalHarness{
		Open: func(t *testing.T) store.ProposedEventStore {
			t.Helper()
			path := filepath.Join(t.TempDir(), "events.db")
			harness := &sqliteProposalHarness{Store: openStore(t, path), path: path}
			seedProposalAuthorities(t, path)
			return harness
		},
		Reopen: func(t *testing.T, current store.ProposedEventStore) store.ProposedEventStore {
			t.Helper()
			harness := current.(*sqliteProposalHarness)
			if err := harness.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			harness.Store = openStore(t, harness.path)
			return harness
		},
		Authority: func(t *testing.T, _ store.ProposedEventStore) store.CommandAuthority {
			t.Helper()
			return store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
		},
		Invalidate: invalidateProposalAuthority,
	})
}

func TestConnectionStoreContract(t *testing.T) {
	storetest.ConnectionContract(t, storetest.ConnectionHarness{
		Open: func(t *testing.T) store.AdapterConnectionStore {
			t.Helper()
			path := filepath.Join(t.TempDir(), "events.db")
			return &sqliteConnectionHarness{Store: openStore(t, path), path: path}
		},
		Invalidate: invalidateAdapterConnection,
	})
}

func TestAttachAttemptStoreContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	storetest.AttachAttemptContract(t, storetest.AttachAttemptHarness{
		Open: func(t *testing.T) store.AttachAttemptStore {
			t.Helper()
			return openStore(t, path)
		},
		Reopen: func(t *testing.T, current store.AttachAttemptStore) store.AttachAttemptStore {
			t.Helper()
			if err := current.(*sqlite.Store).Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			return openStore(t, path)
		},
	})
}

func TestWorkspaceLeaseStoreContract(t *testing.T) {
	storetest.WorkspaceLeaseContract(t, storetest.WorkspaceLeaseHarness{
		Open: func(t *testing.T) store.WorkspaceLeaseStore {
			t.Helper()
			return newSQLiteWorkspaceLeaseHarness(t, filepath.Join(t.TempDir(), "events.db"))
		},
		Reopen: func(t *testing.T, current store.WorkspaceLeaseStore) store.WorkspaceLeaseStore {
			t.Helper()
			harness := current.(*sqliteWorkspaceLeaseHarness)
			if err := harness.Close(); err != nil {
				t.Fatalf("close workspace lease store: %v", err)
			}
			harness.Store = openStore(t, harness.path)
			return harness
		},
		Invalidate: func(t *testing.T, current store.WorkspaceLeaseStore, _ store.WorkspaceLeaseKey, _ store.WorkspaceLeaseOwner, kind storetest.WorkspaceLeaseAuthorityFailure) {
			harness := current.(*sqliteWorkspaceLeaseHarness)
			statement := map[storetest.WorkspaceLeaseAuthorityFailure]string{
				storetest.WorkspaceLeaseAuthoritySuperseded: "UPDATE session_adapter_connections SET connection_epoch = 2",
				storetest.WorkspaceLeaseAuthorityRevoked:    "UPDATE session_adapter_connections SET revoked_at_ms = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)",
				storetest.WorkspaceLeaseAuthorityExpired:    "UPDATE session_adapter_connections SET created_at_ms = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) - 60000, active_credential_expires_at_ms = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) - 1000",
				storetest.WorkspaceLeaseAuthorityTerminal:   "UPDATE session_adapter_connections SET terminal_at_ms = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)",
				storetest.WorkspaceLeaseAttachmentExpired:   "UPDATE session_attachments SET created_at_ms = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) - 60000, expires_at_ns = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) - 1000) * 1000000",
				storetest.WorkspaceLeaseAttachmentCanceled:  "UPDATE session_attachments SET status = 'canceled', expires_at_ns = NULL",
			}[kind]
			if _, err := openRawSQLite(t, harness.path).ExecContext(context.Background(), statement); err != nil {
				t.Fatalf("invalidate workspace authority %s: %v", kind, err)
			}
		},
	})
}

func TestWorkspaceLeaseRollsBackAndRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	harness := newSQLiteWorkspaceLeaseHarness(t, path)
	reserve := store.WorkspaceLeaseReserve{Key: store.WorkspaceLeaseKey{9}, Owner: store.WorkspaceLeaseOwner{WorkerID: "worker_rollback", SessionID: "ses_workspace", ConnectionEpoch: 1, CredentialGeneration: 1, LeaseID: "lease_rollback"}, ExpiresAt: time.Now().Add(time.Minute)}
	db := openRawSQLite(t, path)
	if _, err := db.ExecContext(context.Background(), `CREATE TRIGGER fail_workspace_lease BEFORE INSERT ON session_workspace_leases BEGIN SELECT RAISE(ABORT, 'workspace lease failpoint'); END`); err != nil {
		t.Fatalf("create workspace lease failpoint: %v", err)
	}
	if _, err := harness.ReserveWorkspaceLease(context.Background(), reserve); err == nil {
		t.Fatal("ReserveWorkspaceLease() survived failpoint")
	}
	if _, err := harness.WorkspaceLease(context.Background(), reserve.Key); err == nil {
		t.Fatal("rollback left a workspace lease")
	}
	if _, err := db.ExecContext(context.Background(), `DROP TRIGGER fail_workspace_lease`); err != nil {
		t.Fatalf("drop workspace lease failpoint: %v", err)
	}
	if _, err := harness.ReserveWorkspaceLease(context.Background(), reserve); err != nil {
		t.Fatalf("reserve workspace lease: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = ON; UPDATE session_workspace_leases SET status = 'invalid' WHERE workspace_key = ?`, reserve.Key[:]); err != nil {
		t.Fatalf("corrupt workspace lease: %v", err)
	}
	if _, err := harness.WorkspaceLease(context.Background(), reserve.Key); err == nil {
		t.Fatal("WorkspaceLease() accepted corrupt row")
	}
}

func TestWorkspaceLeaseRejectsExpiredStartAndReleasesQuarantine(t *testing.T) {
	t.Run("reservation expiry", func(t *testing.T) {
		harness := newSQLiteWorkspaceLeaseHarness(t, filepath.Join(t.TempDir(), "events.db"))
		reserve := store.WorkspaceLeaseReserve{Key: store.WorkspaceLeaseKey{10}, Owner: store.WorkspaceLeaseOwner{WorkerID: "worker_expiry", SessionID: "ses_workspace", ConnectionEpoch: 1, CredentialGeneration: 1, LeaseID: "lease_expiry"}, ExpiresAt: time.Now().Add(20 * time.Millisecond)}
		lease, err := harness.ReserveWorkspaceLease(context.Background(), reserve)
		if err != nil {
			t.Fatalf("reserve expiring workspace lease: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
		if _, err := harness.RecordWorkspaceStartReceived(context.Background(), reserve.Key, lease.Version, reserve.Owner); err == nil {
			t.Fatal("expired reservation entered start_received")
		}
	})
	t.Run("child scope expiry", func(t *testing.T) {
		harness := newSQLiteWorkspaceLeaseHarness(t, filepath.Join(t.TempDir(), "events.db"))
		reserve := store.WorkspaceLeaseReserve{Key: store.WorkspaceLeaseKey{11}, ChildScope: &store.WorkspaceLeaseChildScope{ParentKey: store.WorkspaceLeaseKey{1}, CapabilityDigest: [32]byte{2}, ExpiresAt: time.Now().Add(20 * time.Millisecond)}, Owner: store.WorkspaceLeaseOwner{WorkerID: "worker_child_expiry", SessionID: "ses_workspace", ConnectionEpoch: 1, CredentialGeneration: 1, LeaseID: "lease_child_expiry"}, ExpiresAt: time.Now().Add(time.Minute)}
		lease, err := harness.ReserveWorkspaceLease(context.Background(), reserve)
		if err != nil {
			t.Fatalf("reserve child workspace lease: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
		if _, err := harness.RecordWorkspaceStartReceived(context.Background(), reserve.Key, lease.Version, reserve.Owner); err == nil {
			t.Fatal("expired child scope entered start_received")
		}
	})
	t.Run("quarantine release", func(t *testing.T) {
		harness := newSQLiteWorkspaceLeaseHarness(t, filepath.Join(t.TempDir(), "events.db"))
		reserve := store.WorkspaceLeaseReserve{Key: store.WorkspaceLeaseKey{12}, Owner: store.WorkspaceLeaseOwner{WorkerID: "worker_quarantine", SessionID: "ses_workspace", ConnectionEpoch: 1, CredentialGeneration: 1, LeaseID: "lease_quarantine"}, ExpiresAt: time.Now().Add(time.Minute)}
		lease, err := harness.ReserveWorkspaceLease(context.Background(), reserve)
		if err != nil {
			t.Fatalf("reserve quarantined workspace lease: %v", err)
		}
		if _, err := harness.TerminateAdapterConnectionBeforeHello(context.Background(), "ses_workspace", store.AdapterConnectionPreHelloTermination{ExpectedActiveCredentialGeneration: 1}); err == nil {
			t.Fatal("terminate after hello unexpectedly succeeded")
		}
		db := openRawSQLite(t, harness.path)
		if _, err := db.ExecContext(context.Background(), "UPDATE session_adapter_connections SET revoked_at_ms = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"); err != nil {
			t.Fatalf("revoke workspace authority: %v", err)
		}
		quarantined, err := harness.QuarantineWorkspaceLease(context.Background(), reserve.Key, lease.Version)
		if err != nil {
			t.Fatalf("quarantine workspace lease: %v", err)
		}
		if _, err := harness.ReleaseWorkspaceLeaseAfterQuiescence(context.Background(), reserve.Key, quarantined.Version, reserve.Owner); err != nil {
			t.Fatalf("release quarantined workspace lease: %v", err)
		}
	})
}

type sqliteWorkspaceLeaseHarness struct {
	*sqlite.Store
	path string
}

func newSQLiteWorkspaceLeaseHarness(t *testing.T, path string) *sqliteWorkspaceLeaseHarness {
	t.Helper()
	harness := &sqliteWorkspaceLeaseHarness{Store: openStore(t, path), path: path}
	if _, err := harness.InitializeAdapterConnection(context.Background(), store.AdapterConnectionInitialize{SessionID: "ses_workspace", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("initialize workspace authority: %v", err)
	}
	if _, err := harness.AcceptAdapterHello(context.Background(), "ses_workspace", store.AdapterHello{CredentialGeneration: 1}); err != nil {
		t.Fatalf("accept workspace authority hello: %v", err)
	}
	db := openRawSQLite(t, path)
	if _, err := db.ExecContext(context.Background(), `INSERT INTO session_attachments (attach_id, bootstrap_session_id, target_session_id, status, delivery_state, queue_reason, expires_at_ns, blocking_session_id, target_credential_lineage_ref, created_at_ms, updated_at_ms) VALUES ('att_workspace', 'ses_workspace_blocker', 'ses_workspace', 'queued', 'pending', 'workspace_busy', ?, 'ses_workspace_blocker', 'lineage_workspace', ?, ?)`, time.Now().Add(time.Hour).UnixNano(), time.Now().UnixMilli(), time.Now().UnixMilli()); err != nil {
		t.Fatalf("seed workspace attachment: %v", err)
	}
	return harness
}

func TestAttachAttemptRollsBackAndRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	attempts := openStore(t, path)
	request := store.AttachAttemptRequest{
		Identity:    store.AttachAttemptIdentity{JTIHash: [32]byte{9}, AttachID: "att_rollback", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target", Provider: "claude-code"},
		Fingerprint: store.AttachAttemptFingerprint{Domain: "agentwharf.attach-request.v1", Version: 1, Digest: [32]byte{8}, KeyVersion: 1},
		ExpiresAt:   time.Now().Add(time.Minute), Outcome: store.AttachAttemptAccepted,
	}
	generation := int64(1)
	request.IssuedCredentialGeneration = &generation
	db := openRawSQLite(t, path)
	if _, err := db.ExecContext(context.Background(), `CREATE TRIGGER fail_attach_attempt BEFORE INSERT ON session_attach_attempts BEGIN SELECT RAISE(ABORT, 'attach attempt failpoint'); END`); err != nil {
		t.Fatalf("create attach attempt failpoint: %v", err)
	}
	if _, err := attempts.CommitAttachAttempt(context.Background(), request); err == nil {
		t.Fatal("CommitAttachAttempt() survived failpoint")
	}
	if _, err := attempts.AttachAttempt(context.Background(), request.Identity.JTIHash); err == nil {
		t.Fatal("rollback left an attach attempt")
	}
	if _, err := db.ExecContext(context.Background(), `DROP TRIGGER fail_attach_attempt`); err != nil {
		t.Fatalf("drop attach attempt failpoint: %v", err)
	}
	if _, err := attempts.CommitAttachAttempt(context.Background(), request); err != nil {
		t.Fatalf("CommitAttachAttempt() error = %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = ON; UPDATE session_attach_attempts SET fingerprint_digest = zeroblob(31)`); err != nil {
		t.Fatalf("corrupt attach attempt row: %v", err)
	}
	if _, err := attempts.AttachAttempt(context.Background(), request.Identity.JTIHash); err == nil {
		t.Fatal("AttachAttempt() accepted corrupt row")
	}
}

func TestAttachAttemptRechecksExpiryAfterWriterWait(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	attempts := openStore(t, path)
	db := openRawSQLite(t, path)
	lock, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open writer lock: %v", err)
	}
	t.Cleanup(func() { _ = lock.Close() })
	if _, err := lock.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("begin writer lock: %v", err)
	}
	request := store.AttachAttemptRequest{
		Identity:    store.AttachAttemptIdentity{JTIHash: [32]byte{10}, AttachID: "att_expiry_wait", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target", Provider: "claude-code"},
		Fingerprint: store.AttachAttemptFingerprint{Domain: "agentwharf.attach-request.v1", Version: 1, Digest: [32]byte{9}, KeyVersion: 1},
		ExpiresAt:   time.Now().Add(50 * time.Millisecond), Outcome: store.AttachAttemptAccepted,
	}
	generation := int64(1)
	request.IssuedCredentialGeneration = &generation
	result := make(chan error, 1)
	go func() {
		_, err := attempts.CommitAttachAttempt(context.Background(), request)
		result <- err
	}()
	time.Sleep(75 * time.Millisecond)
	if _, err := lock.ExecContext(context.Background(), `COMMIT`); err != nil {
		t.Fatalf("release writer lock: %v", err)
	}
	if err := <-result; err == nil {
		t.Fatal("expired writer-wait attach attempt committed")
	}
	if _, err := attempts.AttachAttempt(context.Background(), request.Identity.JTIHash); err == nil {
		t.Fatal("expired writer-wait attach attempt persisted")
	}
}

func TestAttachAttemptReadRechecksExpiryAfterWriterWait(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	attempts := openStore(t, path)
	request := store.AttachAttemptRequest{
		Identity:    store.AttachAttemptIdentity{JTIHash: [32]byte{12}, AttachID: "att_expiry_read_wait", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target", Provider: "claude-code"},
		Fingerprint: store.AttachAttemptFingerprint{Domain: "agentwharf.attach-request.v1", Version: 1, Digest: [32]byte{11}, KeyVersion: 1},
		ExpiresAt:   time.Now().Add(150 * time.Millisecond), Outcome: store.AttachAttemptAccepted,
	}
	generation := int64(1)
	request.IssuedCredentialGeneration = &generation
	if _, err := attempts.CommitAttachAttempt(context.Background(), request); err != nil {
		t.Fatalf("CommitAttachAttempt() error = %v", err)
	}
	db := openRawSQLite(t, path)
	lock, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open writer lock: %v", err)
	}
	t.Cleanup(func() { _ = lock.Close() })
	if _, err := lock.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("begin writer lock: %v", err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := attempts.AttachAttempt(context.Background(), request.Identity.JTIHash)
		result <- err
	}()
	time.Sleep(175 * time.Millisecond)
	if _, err := lock.ExecContext(context.Background(), `COMMIT`); err != nil {
		t.Fatalf("release writer lock: %v", err)
	}
	if err := <-result; err == nil {
		t.Fatal("expired writer-wait attach attempt was returned")
	}
}

func TestAttachAttemptReadCleansExpiredRow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	attempts := openStore(t, path)
	request := store.AttachAttemptRequest{
		Identity:    store.AttachAttemptIdentity{JTIHash: [32]byte{11}, AttachID: "att_expiry_cleanup", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target", Provider: "claude-code"},
		Fingerprint: store.AttachAttemptFingerprint{Domain: "agentwharf.attach-request.v1", Version: 1, Digest: [32]byte{10}, KeyVersion: 1},
		ExpiresAt:   time.Now().Add(50 * time.Millisecond), Outcome: store.AttachAttemptAccepted,
	}
	generation := int64(1)
	request.IssuedCredentialGeneration = &generation
	if _, err := attempts.CommitAttachAttempt(context.Background(), request); err != nil {
		t.Fatalf("CommitAttachAttempt() error = %v", err)
	}
	time.Sleep(75 * time.Millisecond)
	if _, err := attempts.AttachAttempt(context.Background(), request.Identity.JTIHash); err == nil {
		t.Fatal("AttachAttempt() returned expired row")
	}
	db := openRawSQLite(t, path)
	var rows int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM session_attach_attempts WHERE attempt_jti_hash = ?`, request.Identity.JTIHash[:]).Scan(&rows); err != nil {
		t.Fatalf("count expired row: %v", err)
	}
	if rows != 0 {
		t.Fatalf("expired row count = %d, want 0", rows)
	}
}

func TestAttachAttemptCleanupIsBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	attempts := openStore(t, path)
	db := openRawSQLite(t, path)
	expiresAt := time.Now().Add(-time.Minute)
	createdAt := expiresAt.Add(-time.Minute).UnixMilli()
	for index := 0; index <= 128; index++ {
		jti := make([]byte, 32)
		jti[0], jti[1] = byte(index), byte(index>>8)
		if _, err := db.ExecContext(context.Background(), `
INSERT INTO session_attach_attempts
(attempt_jti_hash, attach_id, bootstrap_session_id, target_session_id, provider,
 fingerprint_domain, fingerprint_version, fingerprint_digest, fingerprint_key_version,
 expires_at_ns, admission_outcome, issued_credential_generation, created_at_ms)
VALUES (?, ?, ?, ?, 'claude-code', 'agentwharf.attach-request.v1', 1, zeroblob(32), 1, ?, 'accepted', 1, ?)
`, jti, fmt.Sprintf("att_cleanup_%d", index), fmt.Sprintf("ses_bootstrap_%d", index), fmt.Sprintf("ses_target_%d", index), expiresAt.UnixNano(), createdAt); err != nil {
			t.Fatalf("seed expired attempt %d: %v", index, err)
		}
	}
	missing := [32]byte{255}
	if _, err := attempts.AttachAttempt(context.Background(), missing); err == nil {
		t.Fatal("missing attach attempt unexpectedly exists")
	}
	var rows int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM session_attach_attempts`).Scan(&rows); err != nil {
		t.Fatalf("count bounded cleanup rows: %v", err)
	}
	if rows != 1 {
		t.Fatalf("first cleanup rows = %d, want 1", rows)
	}
	if _, err := attempts.AttachAttempt(context.Background(), missing); err == nil {
		t.Fatal("missing attach attempt unexpectedly exists after second cleanup")
	}
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM session_attach_attempts`).Scan(&rows); err != nil {
		t.Fatalf("count completed cleanup rows: %v", err)
	}
	if rows != 0 {
		t.Fatalf("second cleanup rows = %d, want 0", rows)
	}
}

func TestAttachAttemptRejectsExpiredTargetLeftAfterBoundedCleanup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	attempts := openStore(t, path)
	db := openRawSQLite(t, path)
	expiresAt := time.Now().Add(-time.Minute)
	createdAt := expiresAt.Add(-time.Minute).UnixMilli()
	for index := 0; index <= 128; index++ {
		jti := make([]byte, 32)
		jti[0], jti[1] = byte(index), byte(index>>8)
		if _, err := db.ExecContext(context.Background(), `
	INSERT INTO session_attach_attempts
	(attempt_jti_hash, attach_id, bootstrap_session_id, target_session_id, provider,
	 fingerprint_domain, fingerprint_version, fingerprint_digest, fingerprint_key_version,
	 expires_at_ns, admission_outcome, issued_credential_generation, created_at_ms)
	VALUES (?, ?, ?, ?, 'claude-code', 'agentwharf.attach-request.v1', 1, zeroblob(32), 1, ?, 'accepted', 1, ?)
	`, jti, fmt.Sprintf("att_expired_%d", index), fmt.Sprintf("ses_bootstrap_%d", index), fmt.Sprintf("ses_target_%d", index), expiresAt.UnixNano(), createdAt); err != nil {
			t.Fatalf("seed expired attempt %d: %v", index, err)
		}
	}
	target := [32]byte{255}
	digest := make([]byte, 32)
	digest[0] = 1
	if _, err := db.ExecContext(context.Background(), `
	INSERT INTO session_attach_attempts
	(attempt_jti_hash, attach_id, bootstrap_session_id, target_session_id, provider,
	 fingerprint_domain, fingerprint_version, fingerprint_digest, fingerprint_key_version,
	 expires_at_ns, admission_outcome, issued_credential_generation, created_at_ms)
	VALUES (?, 'att_expired_target', 'ses_bootstrap_target', 'ses_target_target', 'claude-code',
	 'agentwharf.attach-request.v1', 1, ?, 1, ?, 'accepted', 1, ?)
	`, target[:], digest, expiresAt.UnixNano(), createdAt); err != nil {
		t.Fatalf("seed expired target: %v", err)
	}
	if _, err := attempts.AttachAttempt(context.Background(), target); err == nil {
		t.Fatal("AttachAttempt() returned expired target left after bounded cleanup")
	}
}

func TestConnectionPreHelloLifecycleAndRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	connections := &sqliteConnectionHarness{Store: openStore(t, path), path: path}
	init := store.AdapterConnectionInitialize{SessionID: "ses_prehello", ActiveCredentialGeneration: 3, ActiveCredentialExpiresAt: time.Now().Add(50 * time.Millisecond)}
	initial, err := connections.InitializeAdapterConnection(context.Background(), init)
	if err != nil {
		t.Fatalf("InitializeAdapterConnection() error = %v", err)
	}
	refresh := store.AdapterCredentialPreHelloRefresh{ExpectedActiveCredentialGeneration: 3, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}
	if _, err := connections.RefreshAdapterCredentialBeforeHello(context.Background(), init.SessionID, refresh); err == nil {
		t.Fatal("live pre-hello credential refresh unexpectedly succeeded")
	}
	time.Sleep(75 * time.Millisecond)
	rollback := errors.New("rollback pre-hello refresh")
	if err := connections.WithAdapterConnectionTransaction(context.Background(), func(tx store.AdapterConnectionStore) error {
		if _, err := tx.RefreshAdapterCredentialBeforeHello(context.Background(), init.SessionID, refresh); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatalf("refresh rollback error = %v, want %v", err, rollback)
	}
	if current, err := connections.AdapterConnection(context.Background(), init.SessionID); err != nil || !current.ActiveCredentialExpiresAt.Equal(initial.ActiveCredentialExpiresAt) {
		t.Fatalf("refresh rollback connection = %+v, %v", current, err)
	}
	refreshed, err := connections.RefreshAdapterCredentialBeforeHello(context.Background(), init.SessionID, refresh)
	if err != nil || !refreshed.ActiveCredentialExpiresAt.Equal(time.UnixMilli(refresh.ActiveCredentialExpiresAt.UnixMilli())) {
		t.Fatalf("RefreshAdapterCredentialBeforeHello() = %+v, %v", refreshed, err)
	}
	if exact, err := connections.RefreshAdapterCredentialBeforeHello(context.Background(), init.SessionID, refresh); err != nil || !reflect.DeepEqual(exact, refreshed) {
		t.Fatalf("exact pre-hello refresh = %+v, %v", exact, err)
	}
	if _, err := connections.AcceptAdapterHello(context.Background(), init.SessionID, store.AdapterHello{CredentialGeneration: 3}); err != nil {
		t.Fatalf("AcceptAdapterHello() error = %v", err)
	}
	if _, err := connections.RefreshAdapterCredentialBeforeHello(context.Background(), init.SessionID, refresh); err == nil {
		t.Fatal("post-hello credential refresh unexpectedly succeeded")
	}
	if _, err := connections.TerminateAdapterConnectionBeforeHello(context.Background(), init.SessionID, store.AdapterConnectionPreHelloTermination{ExpectedActiveCredentialGeneration: 3}); err == nil {
		t.Fatal("post-hello termination unexpectedly succeeded")
	}

	terminate := store.AdapterConnectionInitialize{SessionID: "ses_prehello_terminate", ActiveCredentialGeneration: 4, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}
	if _, err := connections.InitializeAdapterConnection(context.Background(), terminate); err != nil {
		t.Fatalf("initialize termination lineage: %v", err)
	}
	request := store.AdapterConnectionPreHelloTermination{ExpectedActiveCredentialGeneration: 4}
	terminated, err := connections.TerminateAdapterConnectionBeforeHello(context.Background(), terminate.SessionID, request)
	if err != nil || terminated.RevokedAt == nil || terminated.TerminalAt == nil || !terminated.RevokedAt.Equal(*terminated.TerminalAt) {
		t.Fatalf("pre-hello termination = %+v, %v", terminated, err)
	}
	if exact, err := connections.TerminateAdapterConnectionBeforeHello(context.Background(), terminate.SessionID, request); err != nil || !reflect.DeepEqual(exact, terminated) {
		t.Fatalf("exact pre-hello termination = %+v, %v", exact, err)
	}
	if _, err := connections.AcceptAdapterHello(context.Background(), terminate.SessionID, store.AdapterHello{CredentialGeneration: 4}); err == nil {
		t.Fatal("terminated pre-hello lineage accepted hello")
	}
}

func TestConnectionFenceAllocatorPersistsAcrossSessionsAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	connections := &sqliteConnectionHarness{Store: openStore(t, path), path: path}
	for _, sessionID := range []string{"ses_fence_a", "ses_fence_b"} {
		if _, err := connections.InitializeAdapterConnection(context.Background(), store.AdapterConnectionInitialize{
			SessionID: sessionID, ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute),
		}); err != nil {
			t.Fatalf("initialize %s: %v", sessionID, err)
		}
	}
	a1, err := connections.AcceptAdapterHello(context.Background(), "ses_fence_a", store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	b1, err := connections.AcceptAdapterHello(context.Background(), "ses_fence_b", store.AdapterHello{CredentialGeneration: 1})
	if err != nil || b1.AcceptedFence <= a1.AcceptedFence {
		t.Fatalf("cross-session fences a=%d b=%d, err=%v", a1.AcceptedFence, b1.AcceptedFence, err)
	}
	if err := connections.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	connections.Store = openStore(t, path)
	a2, err := connections.AcceptAdapterHello(context.Background(), "ses_fence_a", store.AdapterHello{CredentialGeneration: 1})
	if err != nil || a2.AcceptedFence <= b1.AcceptedFence || a2.ConnectionEpoch != a1.ConnectionEpoch+1 {
		t.Fatalf("reopened fence = %+v after %+v, %v", a2, b1, err)
	}
}

func TestConnectionGrantFenceSharesAllocatorAndRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	connections := &sqliteConnectionHarness{Store: openStore(t, path), path: path}
	init := store.AdapterConnectionInitialize{
		SessionID: "ses_grant_fence", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute),
	}
	if _, err := connections.InitializeAdapterConnection(context.Background(), init); err != nil {
		t.Fatalf("InitializeAdapterConnection() error = %v", err)
	}
	record, err := connections.AcceptAdapterHello(context.Background(), init.SessionID, store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatalf("AcceptAdapterHello() error = %v", err)
	}
	fences, ok := any(connections.Store).(store.AdapterGrantFenceStore)
	if !ok {
		t.Fatal("SQLite connection store does not own adapter grant fences")
	}
	grant, err := fences.AllocateAdapterGrantFence(context.Background())
	if err != nil || grant <= record.AcceptedFence {
		t.Fatalf("AllocateAdapterGrantFence() = %d, %v after accepted fence %d", grant, err, record.AcceptedFence)
	}
	admission := store.AdapterConnectionAdmission{
		CredentialGeneration: 1, ConnectionEpoch: record.ConnectionEpoch, AcceptedFence: record.AcceptedFence, GrantFence: grant,
	}
	if _, err := connections.ValidateAdapterAdmission(context.Background(), record.SessionID, admission); err != nil {
		t.Fatalf("validate allocated grant: %v", err)
	}
	forged := admission
	forged.GrantFence = math.MaxInt64
	if _, err := connections.ValidateAdapterAdmission(context.Background(), record.SessionID, forged); err == nil {
		t.Fatal("admission accepted an unallocated high grant fence")
	}

	rollback := errors.New("rollback grant fence")
	var rolledBack int64
	if err := connections.WithAdapterConnectionTransaction(context.Background(), func(tx store.AdapterConnectionStore) error {
		txFences, ok := tx.(store.AdapterGrantFenceStore)
		if !ok {
			return errors.New("transaction does not own adapter grant fences")
		}
		var err error
		rolledBack, err = txFences.AllocateAdapterGrantFence(context.Background())
		if err != nil {
			return err
		}
		assertDurableFenceAllocated(t, path, rolledBack)
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatalf("grant rollback error = %v, want %v", err, rollback)
	}
	reused, err := fences.AllocateAdapterGrantFence(context.Background())
	if err != nil || reused <= rolledBack {
		t.Fatalf("post-rollback grant = %d, %v, want > returned rollback fence %d", reused, err, rolledBack)
	}
	var panicked int64
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("grant transaction panic was swallowed")
			}
		}()
		_ = connections.WithAdapterConnectionTransaction(context.Background(), func(tx store.AdapterConnectionStore) error {
			var err error
			panicked, err = tx.(store.AdapterGrantFenceStore).AllocateAdapterGrantFence(context.Background())
			if err != nil {
				return err
			}
			assertDurableFenceAllocated(t, path, panicked)
			panic("rollback grant fence panic")
		})
	}()
	afterPanic, err := fences.AllocateAdapterGrantFence(context.Background())
	if err != nil || afterPanic <= panicked {
		t.Fatalf("post-panic grant = %d, %v, want > returned panic fence %d", afterPanic, err, panicked)
	}
	reused = afterPanic
	grants := make(chan int64, 8)
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fence, err := fences.AllocateAdapterGrantFence(context.Background())
			grants <- fence
			errs <- err
		}()
	}
	wg.Wait()
	close(grants)
	close(errs)
	maxGrant := reused
	seen := map[int64]bool{reused: true}
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent grant allocation: %v", err)
		}
	}
	for fence := range grants {
		if fence <= rolledBack || seen[fence] {
			t.Fatalf("duplicate or stale concurrent grant fence %d after %d", fence, rolledBack)
		}
		seen[fence] = true
		if fence > maxGrant {
			maxGrant = fence
		}
	}
	newer, err := connections.AcceptAdapterHello(context.Background(), record.SessionID, store.AdapterHello{CredentialGeneration: 1})
	if err != nil || newer.AcceptedFence <= maxGrant {
		t.Fatalf("hello fence = %d, %v, want > committed grant %d", newer.AcceptedFence, err, maxGrant)
	}
	if _, err := connections.ValidateAdapterAdmission(context.Background(), record.SessionID, admission); err == nil {
		t.Fatal("new hello did not fence the older grant")
	}
	if err := connections.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	connections.Store = openStore(t, path)
	fences, ok = any(connections.Store).(store.AdapterGrantFenceStore)
	if !ok {
		t.Fatal("reopened SQLite connection store does not own adapter grant fences")
	}
	afterReopen, err := fences.AllocateAdapterGrantFence(context.Background())
	if err != nil || afterReopen <= newer.AcceptedFence {
		t.Fatalf("reopened grant = %d, %v, want > accepted fence %d", afterReopen, err, newer.AcceptedFence)
	}
}

func TestConnectionAcceptedFencesSurviveTransactionFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	connections := &sqliteConnectionHarness{Store: openStore(t, path), path: path}
	ctx := context.Background()
	for _, sessionID := range []string{"ses_hello_rollback", "ses_hello_cancel"} {
		if _, err := connections.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{
			SessionID: sessionID, ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute),
		}); err != nil {
			t.Fatalf("initialize %s: %v", sessionID, err)
		}
	}
	rollback := errors.New("rollback accepted hello")
	var rolledBack store.AdapterConnection
	if err := connections.WithAdapterConnectionTransaction(ctx, func(tx store.AdapterConnectionStore) error {
		var err error
		rolledBack, err = tx.AcceptAdapterHello(ctx, "ses_hello_rollback", store.AdapterHello{CredentialGeneration: 1})
		if err != nil {
			return err
		}
		assertDurableFenceAllocated(t, path, rolledBack.AcceptedFence)
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatalf("hello rollback error = %v", err)
	}
	afterRollback, err := connections.AcceptAdapterHello(ctx, "ses_hello_rollback", store.AdapterHello{CredentialGeneration: 1})
	if err != nil || afterRollback.ConnectionEpoch != 1 || afterRollback.AcceptedFence <= rolledBack.AcceptedFence {
		t.Fatalf("post-rollback hello = %+v, %v, returned rollback = %+v", afterRollback, err, rolledBack)
	}

	canceledCtx, cancel := context.WithCancel(ctx)
	var canceled store.AdapterConnection
	err = connections.WithAdapterConnectionTransaction(canceledCtx, func(tx store.AdapterConnectionStore) error {
		var err error
		canceled, err = tx.AcceptAdapterHello(canceledCtx, "ses_hello_cancel", store.AdapterHello{CredentialGeneration: 1})
		cancel()
		return err
	})
	if err == nil {
		t.Fatal("canceled hello transaction unexpectedly committed")
	}
	afterCancel, err := connections.AcceptAdapterHello(ctx, "ses_hello_cancel", store.AdapterHello{CredentialGeneration: 1})
	if err != nil || afterCancel.ConnectionEpoch != 1 || afterCancel.AcceptedFence <= canceled.AcceptedFence {
		t.Fatalf("post-cancel hello = %+v, %v, returned canceled = %+v", afterCancel, err, canceled)
	}

	init := store.AdapterConnectionInitialize{SessionID: "ses_activation_panic", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}
	active := initializeConnectionForRotation(t, connections, init)
	rotation := store.AdapterCredentialRotation{ExpectedActiveCredentialGeneration: 1, ExpectedEpoch: active.ConnectionEpoch, PendingGeneration: 2, ExpiresAt: time.Now().Add(time.Minute), RotationID: "rot_activation_panic"}
	if _, err := connections.PrepareAdapterCredentialRotation(ctx, init.SessionID, rotation); err != nil {
		t.Fatalf("prepare activation panic: %v", err)
	}
	var panicked store.AdapterConnection
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("activation transaction panic was swallowed")
			}
		}()
		_ = connections.WithAdapterConnectionTransaction(ctx, func(tx store.AdapterConnectionStore) error {
			var err error
			panicked, err = tx.ActivateAdapterCredential(ctx, init.SessionID, store.AdapterCredentialActivation{ExpectedActiveCredentialGeneration: 1, ExpectedEpoch: active.ConnectionEpoch, PendingGeneration: 2, RotationID: rotation.RotationID})
			if err != nil {
				return err
			}
			assertDurableFenceAllocated(t, path, panicked.AcceptedFence)
			panic("rollback activation fence")
		})
	}()
	activated, err := connections.ActivateAdapterCredential(ctx, init.SessionID, store.AdapterCredentialActivation{ExpectedActiveCredentialGeneration: 1, ExpectedEpoch: active.ConnectionEpoch, PendingGeneration: 2, RotationID: rotation.RotationID})
	if err != nil || activated.AcceptedFence <= panicked.AcceptedFence {
		t.Fatalf("post-panic activation = %+v, %v, returned panic = %+v", activated, err, panicked)
	}
}

func TestConnectionFenceAllocatorRejectsOverflow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	connections := &sqliteConnectionHarness{Store: openStore(t, path), path: path}
	db := openRawSQLite(t, path+".fences")
	if _, err := db.ExecContext(context.Background(), `DROP TRIGGER adapter_fence_allocator_advance;
UPDATE adapter_fence_allocator SET next_fence = ? WHERE singleton = 1;
CREATE TRIGGER adapter_fence_allocator_advance BEFORE UPDATE OF next_fence ON adapter_fence_allocator WHEN NEW.next_fence <> OLD.next_fence + 1 BEGIN SELECT RAISE(ABORT, 'adapter fence allocator must advance by one'); END;`, int64(math.MaxInt64)); err != nil {
		t.Fatalf("seed maximum fence: %v", err)
	}
	if _, err := connections.Store.AllocateAdapterGrantFence(context.Background()); err == nil {
		t.Fatal("maximum fence allocation unexpectedly succeeded")
	}
	var kind string
	var next int64
	if err := db.QueryRowContext(context.Background(), `SELECT typeof(next_fence), next_fence FROM adapter_fence_allocator WHERE singleton = 1`).Scan(&kind, &next); err != nil || kind != "integer" || next != math.MaxInt64 {
		t.Fatalf("overflow allocator = kind %q next %d, %v", kind, next, err)
	}
}

func TestPrepareAdapterCredentialRotationSupersedesExpiredPending(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	connections := &sqliteConnectionHarness{Store: openStore(t, path), path: path}
	ctx := context.Background()
	init := store.AdapterConnectionInitialize{SessionID: "ses_expired_pending_recovery", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}
	active := initializeConnectionForRotation(t, connections, init)
	if _, err := connections.Store.PrepareAdapterCredentialRotation(ctx, init.SessionID, store.AdapterCredentialRotation{
		ExpectedActiveCredentialGeneration: 1, ExpectedEpoch: active.ConnectionEpoch, PendingGeneration: 2,
		ExpiresAt: time.Now().Add(time.Minute), RotationID: "rot_lost_delivery",
	}); err != nil {
		t.Fatalf("prepare first rotation: %v", err)
	}
	db := openRawSQLite(t, path)
	if _, err := db.ExecContext(ctx, `UPDATE session_adapter_connections SET pending_credential_expires_at_ms = created_at_ms + 1 WHERE session_id = ?`, init.SessionID); err != nil {
		t.Fatalf("expire pending rotation: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	recovered, err := connections.Store.PrepareAdapterCredentialRotation(ctx, init.SessionID, store.AdapterCredentialRotation{
		ExpectedActiveCredentialGeneration: 1, ExpectedEpoch: active.ConnectionEpoch, PendingGeneration: 3,
		ExpiresAt: time.Now().Add(time.Minute), RotationID: "rot_recovered",
	})
	if err != nil || recovered.PendingCredentialGeneration == nil || *recovered.PendingCredentialGeneration != 3 || recovered.RotationID == nil || *recovered.RotationID != "rot_recovered" || recovered.CredentialGenerationHighWatermark != 3 {
		t.Fatalf("expired pending recovery = %+v, %v", recovered, err)
	}
}

func TestConnectionFenceSidecarIdentityFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	connections, err := sqlite.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	rollback := errors.New("rollback sidecar identity")
	if err := connections.WithAdapterConnectionTransaction(context.Background(), func(tx store.AdapterConnectionStore) error {
		if _, err := tx.(store.AdapterGrantFenceStore).AllocateAdapterGrantFence(context.Background()); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatalf("allocate rollback fence: %v", err)
	}
	if err := connections.Close(); err != nil {
		t.Fatal(err)
	}
	sidecar, err := os.ReadFile(path + ".fences")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path + ".fences"); err != nil {
		t.Fatal(err)
	}
	if reopened, err := sqlite.Open(context.Background(), path); err == nil {
		_ = reopened.Close()
		t.Fatal("missing fence sidecar was silently recreated")
	}
	if err := os.WriteFile(path+".fences", sidecar, 0o600); err != nil {
		t.Fatal(err)
	}
	replacement := openRawSQLite(t, path+".fences")
	if _, err := replacement.ExecContext(context.Background(), `UPDATE adapter_fence_identity SET store_id = 'mismatched' WHERE singleton = 1`); err != nil {
		t.Fatalf("seed mismatched fence sidecar: %v", err)
	}
	if reopened, err := sqlite.Open(context.Background(), path); err == nil {
		_ = reopened.Close()
		t.Fatal("mismatched fence sidecar was accepted")
	}
}

func TestConnectionFencePartialStateFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name      string
		database  string
		statement string
	}{
		{name: "main identity table", statement: `DROP TABLE session_adapter_fence_identity`},
		{name: "main identity row", statement: `DELETE FROM session_adapter_fence_identity`},
		{name: "main allocator table", statement: `DROP TABLE session_adapter_fence_allocator`},
		{name: "main allocator row", statement: `DELETE FROM session_adapter_fence_allocator`},
		{name: "side identity table", database: ".fences", statement: `DROP TABLE adapter_fence_identity`},
		{name: "side identity row", database: ".fences", statement: `DELETE FROM adapter_fence_identity`},
		{name: "side allocator table", database: ".fences", statement: `DROP TABLE adapter_fence_allocator`},
		{name: "side allocator row", database: ".fences", statement: `DELETE FROM adapter_fence_allocator`},
		{name: "main identity extra row", statement: `PRAGMA ignore_check_constraints = ON; INSERT INTO session_adapter_fence_identity VALUES (2, 'extra')`},
		{name: "main allocator extra row", statement: `PRAGMA ignore_check_constraints = ON; INSERT INTO session_adapter_fence_allocator VALUES (2, 1)`},
		{name: "side identity extra row", database: ".fences", statement: `PRAGMA ignore_check_constraints = ON; INSERT INTO adapter_fence_identity VALUES (2, 'extra')`},
		{name: "side allocator extra row", database: ".fences", statement: `PRAGMA ignore_check_constraints = ON; INSERT INTO adapter_fence_allocator VALUES (2, 1)`},
		{name: "main fence trigger", statement: `DROP TRIGGER session_adapter_connections_advance_fence`},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "events.db")
			st, err := sqlite.Open(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			db := openRawSQLite(t, path+test.database)
			if _, err := db.ExecContext(context.Background(), test.statement); err != nil {
				t.Fatalf("corrupt %s: %v", test.name, err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if reopened, err := sqlite.Open(context.Background(), path); err == nil {
				_ = reopened.Close()
				t.Fatalf("missing %s was silently repaired", test.name)
			}
		})
	}
}

func TestConnectionFenceClosedBackupPreservesReturnedFence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "events.db")
	st, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	rollback := errors.New("rollback before backup")
	var returned int64
	if err := st.WithAdapterConnectionTransaction(ctx, func(tx store.AdapterConnectionStore) error {
		var err error
		returned, err = tx.(store.AdapterGrantFenceStore).AllocateAdapterGrantFence(ctx)
		if err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatalf("allocate returned rollback fence: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	fenceDB := openRawSQLite(t, path+".fences")
	var mode string
	if err := fenceDB.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "delete" {
		t.Fatalf("fence journal mode = %q, %v, want delete", mode, err)
	}
	if err := fenceDB.Close(); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(t.TempDir(), "restored.db")
	for _, suffix := range []string{"", ".fences"} {
		contents, err := os.ReadFile(path + suffix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(restored+suffix, contents, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	reopened, err := sqlite.Open(ctx, restored)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	next, err := reopened.AllocateAdapterGrantFence(ctx)
	if err != nil || next <= returned {
		t.Fatalf("restored fence = %d, %v, want > returned rollback fence %d", next, err, returned)
	}
}

func TestConnectionFenceStaleSameIdentitySidecarFailsClosed(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "events.db")
	st, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	stale, err := os.ReadFile(path + ".fences")
	if err != nil {
		t.Fatal(err)
	}
	st, err = sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	rollback := errors.New("rollback before stale restore")
	var returned int64
	if err := st.WithAdapterConnectionTransaction(ctx, func(tx store.AdapterConnectionStore) error {
		var err error
		returned, err = tx.(store.AdapterGrantFenceStore).AllocateAdapterGrantFence(ctx)
		if err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatalf("allocate returned rollback fence: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".fences", stale, 0o600); err != nil {
		t.Fatal(err)
	}
	if reopened, err := sqlite.Open(ctx, path); err == nil {
		next, allocationErr := reopened.AllocateAdapterGrantFence(ctx)
		_ = reopened.Close()
		t.Fatalf("stale same-identity sidecar was accepted: next=%d allocation_err=%v returned=%d", next, allocationErr, returned)
	}
}

func TestConnectionFenceRuntimeDivergenceFailsClosed(t *testing.T) {
	t.Run("sidecar rewind", func(t *testing.T) {
		ctx := context.Background()
		path := filepath.Join(t.TempDir(), "events.db")
		st := openStore(t, path)
		grant, err := st.AllocateAdapterGrantFence(ctx)
		if err != nil {
			t.Fatal(err)
		}
		side := openRawSQLite(t, path+".fences")
		if _, err := side.ExecContext(ctx, `UPDATE adapter_fence_allocator SET next_fence = ? WHERE singleton = 1`, grant); err == nil {
			if reused, err := st.AllocateAdapterGrantFence(ctx); err == nil {
				t.Fatalf("runtime sidecar rewind reused fence %d after %d", reused, grant)
			}
		}
	})
	t.Run("main shadow inflation", func(t *testing.T) {
		ctx := context.Background()
		path := filepath.Join(t.TempDir(), "events.db")
		st, err := sqlite.Open(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		init := store.AdapterConnectionInitialize{SessionID: "ses_runtime_divergence", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}
		if _, err := st.InitializeAdapterConnection(ctx, init); err != nil {
			t.Fatal(err)
		}
		connection, err := st.AcceptAdapterHello(ctx, init.SessionID, store.AdapterHello{CredentialGeneration: 1})
		if err != nil {
			t.Fatal(err)
		}
		grant, err := st.AllocateAdapterGrantFence(ctx)
		if err != nil {
			t.Fatal(err)
		}
		main := openRawSQLite(t, path)
		forged := grant + 100
		if _, err := main.ExecContext(ctx, `UPDATE session_adapter_fence_allocator SET next_fence = ? WHERE singleton = 1`, forged+1); err != nil {
			t.Fatal(err)
		}
		if _, err := st.ValidateAdapterAdmission(ctx, init.SessionID, store.AdapterConnectionAdmission{CredentialGeneration: 1, ConnectionEpoch: connection.ConnectionEpoch, AcceptedFence: connection.AcceptedFence, GrantFence: forged}); err == nil {
			t.Fatal("runtime main-shadow inflation admitted an unallocated grant")
		}
		if err := st.Close(); err == nil {
			t.Fatal("Close() accepted runtime main-shadow inflation")
		}
	})
}

func TestConnectionTwoStoreOrderingAndExpiry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	first, second := openStore(t, path), openStore(t, path)
	ctx := context.Background()
	init := store.AdapterConnectionInitialize{SessionID: "ses_two_store", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute)}
	if _, err := first.InitializeAdapterConnection(ctx, init); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan store.AdapterConnection, 8)
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for index := range 8 {
		current := first
		if index%2 == 1 {
			current = second
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			connection, err := current.AcceptAdapterHello(ctx, init.SessionID, store.AdapterHello{CredentialGeneration: 1})
			results <- connection
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("two-Store hello: %v", err)
		}
	}
	seen := make(map[int64]bool)
	for connection := range results {
		if seen[connection.AcceptedFence] {
			t.Fatalf("duplicate two-Store fence %d", connection.AcceptedFence)
		}
		seen[connection.AcceptedFence] = true
	}

	for _, tc := range []struct {
		sessionID string
		mutation  func(store.AdapterConnectionStore) error
	}{
		{sessionID: "ses_two_store_expired_hello", mutation: func(connections store.AdapterConnectionStore) error {
			_, err := connections.AcceptAdapterHello(ctx, "ses_two_store_expired_hello", store.AdapterHello{CredentialGeneration: 1})
			return err
		}},
		{sessionID: "ses_two_store_expired_rotation", mutation: func(connections store.AdapterConnectionStore) error {
			_, err := connections.PrepareAdapterCredentialRotation(ctx, "ses_two_store_expired_rotation", store.AdapterCredentialRotation{ExpectedActiveCredentialGeneration: 1, ExpectedEpoch: 1, PendingGeneration: 2, ExpiresAt: time.Now().Add(60 * time.Millisecond), RotationID: "rot_two_store_expired"})
			return err
		}},
	} {
		expiresAt := time.Now().Add(60 * time.Millisecond)
		if _, err := first.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{SessionID: tc.sessionID, ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: expiresAt}); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(tc.sessionID, "rotation") {
			if _, err := first.AcceptAdapterHello(ctx, tc.sessionID, store.AdapterHello{CredentialGeneration: 1}); err != nil {
				t.Fatal(err)
			}
		}
		lockDB := openRawSQLite(t, path)
		lockTx, err := lockDB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := lockTx.ExecContext(ctx, `UPDATE session_adapter_connections SET updated_at_ms = updated_at_ms WHERE session_id = ?`, tc.sessionID); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- tc.mutation(second) }()
		time.Sleep(100 * time.Millisecond)
		if err := lockTx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err == nil {
			t.Fatalf("queued expired mutation for %s succeeded", tc.sessionID)
		}
	}
}

func assertDurableFenceAllocated(t *testing.T, path string, fence int64) {
	t.Helper()
	db := openRawSQLite(t, path+".fences")
	var next int64
	if err := db.QueryRowContext(context.Background(), `SELECT next_fence FROM adapter_fence_allocator WHERE singleton = 1`).Scan(&next); err != nil || next <= fence {
		t.Fatalf("durable fence next = %d, %v, want > returned %d", next, err, fence)
	}
}

func initializeConnectionForRotation(t *testing.T, connections *sqliteConnectionHarness, init store.AdapterConnectionInitialize) store.AdapterConnection {
	t.Helper()
	if _, err := connections.InitializeAdapterConnection(context.Background(), init); err != nil {
		t.Fatalf("initialize rotation connection: %v", err)
	}
	connection, err := connections.AcceptAdapterHello(context.Background(), init.SessionID, store.AdapterHello{CredentialGeneration: init.ActiveCredentialGeneration})
	if err != nil {
		t.Fatalf("hello rotation connection: %v", err)
	}
	return connection
}

func TestConnectionCredentialLineageCorruptionFailsClosed(t *testing.T) {
	schemaPath := filepath.Join(t.TempDir(), "schema.db")
	openStore(t, schemaPath)
	schemaDB := openRawSQLite(t, schemaPath)
	var schema string
	if err := schemaDB.QueryRowContext(context.Background(), `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'session_adapter_connections'`).Scan(&schema); err != nil {
		t.Fatalf("read adapter connection schema: %v", err)
	}
	for _, invariant := range []string{
		"pending_credential_generation <> active_credential_generation",
		"prior_recovery_credential_generation <> active_credential_generation",
		"pending_credential_generation <> prior_recovery_credential_generation",
	} {
		if !strings.Contains(schema, invariant) {
			t.Fatalf("adapter connection schema lacks lineage invariant %q", invariant)
		}
	}
	cases := []struct {
		name                  string
		active, highWatermark int64
		pending, prior        any
		rotation              any
	}{
		{name: "pending_equals_active", active: 1, highWatermark: 2, pending: int64(1), rotation: "rot_corrupt"},
		{name: "prior_equals_active", active: 2, highWatermark: 2, prior: int64(2)},
		{name: "pending_equals_prior", active: 2, highWatermark: 3, pending: int64(1), prior: int64(1), rotation: "rot_corrupt"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "events.db")
			connections := &sqliteConnectionHarness{Store: openStore(t, path), path: path}
			db := openRawSQLite(t, path)
			if _, err := db.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = ON`); err != nil {
				t.Fatal(err)
			}
			now := time.Now().UnixMilli()
			if _, err := db.ExecContext(context.Background(), `
INSERT INTO session_adapter_connections (
    session_id, connection_epoch, accepted_fence, active_credential_generation,
    credential_generation_high_watermark, active_credential_expires_at_ms,
    pending_credential_generation, pending_credential_expires_at_ms,
    prior_recovery_credential_generation, rotation_id, created_at_ms, updated_at_ms
) VALUES (?, 1, 1, ?, ?, ?, ?, CASE WHEN ? IS NULL THEN NULL ELSE ? END, ?, ?, ?, ?);
UPDATE session_adapter_fence_allocator SET next_fence = 2 WHERE singleton = 1;
`, "ses_lineage_corrupt", tc.active, tc.highWatermark, now+60000, tc.pending, tc.pending, now+60000,
				tc.prior, tc.rotation, now, now); err != nil {
				t.Fatalf("seed corrupt lineage: %v", err)
			}
			seedFixtureFence(t, path, 2)
			before := connectionCorruptionSnapshot(t, db, "ses_lineage_corrupt")
			if _, err := connections.AdapterConnection(context.Background(), "ses_lineage_corrupt"); err == nil {
				t.Fatal("AdapterConnection() accepted corrupt credential lineage")
			}
			mutations := []func() error{
				func() error {
					_, err := connections.AcceptAdapterHello(context.Background(), "ses_lineage_corrupt", store.AdapterHello{CredentialGeneration: tc.active})
					return err
				},
				func() error {
					_, err := connections.PrepareAdapterCredentialRotation(context.Background(), "ses_lineage_corrupt", store.AdapterCredentialRotation{ExpectedActiveCredentialGeneration: tc.active, ExpectedEpoch: 1, PendingGeneration: tc.highWatermark + 1, ExpiresAt: time.Now().Add(time.Minute), RotationID: "rot_new"})
					return err
				},
				func() error {
					_, err := connections.ActivateAdapterCredential(context.Background(), "ses_lineage_corrupt", store.AdapterCredentialActivation{ExpectedActiveCredentialGeneration: tc.active, ExpectedEpoch: 1, PendingGeneration: 1, RotationID: "rot_corrupt"})
					return err
				},
			}
			for index, mutation := range mutations {
				if err := mutation(); err == nil {
					t.Fatalf("mutation %d accepted corrupt credential lineage", index)
				}
			}
			if after := connectionCorruptionSnapshot(t, db, "ses_lineage_corrupt"); after != before {
				t.Fatalf("corrupt lineage mutation changed state: before=%q after=%q", before, after)
			}
		})
	}
}

func connectionCorruptionSnapshot(t *testing.T, db *sql.DB, sessionID string) string {
	t.Helper()
	var snapshot string
	if err := db.QueryRowContext(context.Background(), `
SELECT printf('%d|%d|%d|%d|%s|%s|%s|%d', connection_epoch, accepted_fence,
    active_credential_generation, credential_generation_high_watermark,
    COALESCE(pending_credential_generation, 'null'), COALESCE(prior_recovery_credential_generation, 'null'),
    COALESCE(rotation_id, 'null'), (SELECT next_fence FROM session_adapter_fence_allocator WHERE singleton = 1))
FROM session_adapter_connections WHERE session_id = ?
`, sessionID).Scan(&snapshot); err != nil {
		t.Fatalf("snapshot adapter connection: %v", err)
	}
	return snapshot
}

func TestConnectionCorruptionAndForbiddenColumnsFailClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	connections := &sqliteConnectionHarness{Store: openStore(t, path), path: path}
	if _, err := connections.InitializeAdapterConnection(context.Background(), store.AdapterConnectionInitialize{
		SessionID: "ses_connection_corrupt", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Minute),
	}); err != nil {
		t.Fatalf("InitializeAdapterConnection() error = %v", err)
	}
	db := openRawSQLite(t, path)
	rows, err := db.QueryContext(context.Background(), `PRAGMA table_info(session_adapter_connections)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{"bearer", "token", "secret", "provider", "platform", "payload", "content"} {
			if strings.Contains(strings.ToLower(name), forbidden) {
				t.Fatalf("connection column %q contains forbidden concept %q", name, forbidden)
			}
		}
	}
	if _, err := db.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE session_adapter_fence_allocator SET next_fence = 0 WHERE singleton = 1`); err != nil {
		t.Fatalf("corrupt connection fence allocator: %v", err)
	}
	if _, err := connections.AdapterConnection(context.Background(), "ses_connection_corrupt"); err == nil {
		t.Fatal("AdapterConnection() accepted corrupt allocator fence")
	}
}

func TestProposalLedgerStoresReferencesOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	proposals := &sqliteProposalHarness{Store: openStore(t, path), path: path}
	seedProposalAuthorities(t, path)
	marker := "proposal-secret-marker"
	if _, err := proposals.CommitProposedEvent(context.Background(), "ses_proposal_corrupt",
		store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}, store.ProposedEventRequest{
			ProposalID: "proposal_reference_only",
			Event:      store.PendingEvent{Type: "session.state", Time: testTime(1), Payload: []byte(fmt.Sprintf(`{"marker":%q}`, marker))},
		}); err != nil {
		t.Fatalf("CommitProposedEvent() error = %v", err)
	}
	db := openRawSQLite(t, path)
	rows, err := db.QueryContext(context.Background(), `PRAGMA table_info(session_event_proposals)`)
	if err != nil {
		t.Fatalf("read proposal columns: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatalf("scan proposal column: %v", err)
		}
		for _, forbidden := range []string{"payload", "content", "secret", "token", "credential", "provider"} {
			if strings.Contains(strings.ToLower(name), forbidden) {
				t.Fatalf("proposal column %q contains forbidden concept %q", name, forbidden)
			}
		}
	}
	var values string
	if err := db.QueryRowContext(context.Background(), `
SELECT session_id || '|' || proposal_id || '|' || event_seq FROM session_event_proposals WHERE proposal_id = ?
`, "proposal_reference_only").Scan(&values); err != nil {
		t.Fatalf("read proposal reference: %v", err)
	}
	if strings.Contains(values, marker) {
		t.Fatalf("proposal reference copied event content: %q", values)
	}
}

func TestProposalCorruptionFailsClosedWithoutMaterializingOversizedEvent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	proposals := &sqliteProposalHarness{Store: openStore(t, path), path: path}
	seedProposalAuthorities(t, path)
	authority := store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
	request := store.ProposedEventRequest{ProposalID: "proposal_corrupt", Event: store.PendingEvent{
		Type: "session.state", Time: testTime(1), Payload: []byte(`{"state":"ready"}`),
	}}
	if _, err := proposals.CommitProposedEvent(context.Background(), "ses_proposal_corrupt", authority, request); err != nil {
		t.Fatalf("CommitProposedEvent() error = %v", err)
	}
	db := openRawSQLite(t, path)
	if _, err := db.ExecContext(context.Background(), `
UPDATE session_events SET payload = zeroblob(8 * 1024 * 1024) WHERE session_id = ? AND seq = 1
`, "ses_proposal_corrupt"); err != nil {
		t.Fatalf("corrupt proposal event payload: %v", err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if _, err := proposals.CommitProposedEvent(context.Background(), "ses_proposal_corrupt", authority, request); err == nil {
		t.Fatal("duplicate CommitProposedEvent() accepted oversized referenced payload")
	}
	runtime.ReadMemStats(&after)
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 2*1024*1024 {
		t.Fatalf("oversized corrupt proposal allocated %d bytes, want <= 2 MiB", allocated)
	}
}

func TestProposalQueuedBehindAuthorityChangeWritesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	proposals := &sqliteProposalHarness{Store: openStore(t, path), path: path}
	seedProposalAuthorities(t, path)
	db := openRawSQLite(t, path)
	if _, err := db.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("begin proposal authority change: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
UPDATE session_adapter_connections SET connection_epoch = 2, updated_at_ms = ? WHERE session_id = ?
`, time.Now().UnixMilli(), "ses_proposal_queued"); err != nil {
		t.Fatalf("stage proposal authority change: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := proposals.CommitProposedEvent(ctx, "ses_proposal_queued",
			store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}, store.ProposedEventRequest{
				ProposalID: "proposal_queued", Event: store.PendingEvent{Type: "session.state", Time: testTime(1), Payload: []byte(`{"state":"ready"}`)},
			})
		result <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if _, err := db.ExecContext(context.Background(), `COMMIT`); err != nil {
		t.Fatalf("commit proposal authority change: %v", err)
	}
	if err := <-result; err == nil {
		t.Fatal("queued stale CommitProposedEvent() unexpectedly succeeded")
	}
	if latest, err := proposals.LatestSeq(context.Background(), "ses_proposal_queued"); err != nil || latest != 0 {
		t.Fatalf("queued stale proposal latest seq = %d, %v; want 0, nil", latest, err)
	}
	var receipts int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM session_event_proposals WHERE session_id = ?`, "ses_proposal_queued").Scan(&receipts); err != nil {
		t.Fatalf("count queued proposal receipts: %v", err)
	}
	if receipts != 0 {
		t.Fatalf("queued stale proposal receipts = %d, want 0", receipts)
	}
}

func TestProposalDuplicateQueuedBehindAuthorityChangeReturnsNoReceipt(t *testing.T) {
	for _, test := range []struct {
		name      string
		statement string
		args      func(now int64, sessionID string) []any
	}{
		{name: "epoch", statement: `UPDATE session_adapter_connections SET connection_epoch = 2, updated_at_ms = ? WHERE session_id = ?`, args: func(now int64, sessionID string) []any { return []any{now, sessionID} }},
		{name: "generation", statement: `UPDATE session_adapter_connections SET active_credential_generation = 2, credential_generation_high_watermark = 2, updated_at_ms = ? WHERE session_id = ?`, args: func(now int64, sessionID string) []any { return []any{now, sessionID} }},
		{name: "revoked", statement: `UPDATE session_adapter_connections SET revoked_at_ms = ?, updated_at_ms = ? WHERE session_id = ?`, args: func(now int64, sessionID string) []any { return []any{now, now, sessionID} }},
		{name: "expired", statement: `UPDATE session_adapter_connections SET created_at_ms = ?, active_credential_expires_at_ms = ?, updated_at_ms = ? WHERE session_id = ?`, args: func(now int64, sessionID string) []any {
			return []any{now - int64((2*time.Minute)/time.Millisecond), now - 1, now, sessionID}
		}},
		{name: "terminal", statement: `UPDATE session_adapter_connections SET terminal_at_ms = ?, updated_at_ms = ? WHERE session_id = ?`, args: func(now int64, sessionID string) []any { return []any{now, now, sessionID} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "events.db")
			proposals := &sqliteProposalHarness{Store: openStore(t, path), path: path}
			seedProposalAuthorities(t, path)
			sessionID := "ses_proposal_duplicate_" + test.name
			authority := store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
			request := store.ProposedEventRequest{ProposalID: "proposal_duplicate_authority", Event: store.PendingEvent{
				Type: "session.state", Time: testTime(1), Payload: []byte(`{"state":"ready"}`),
			}}
			if _, err := proposals.CommitProposedEvent(context.Background(), sessionID, authority, request); err != nil {
				t.Fatalf("initial CommitProposedEvent() error = %v", err)
			}
			db := openRawSQLite(t, path)
			if _, err := db.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
				t.Fatalf("begin duplicate authority change: %v", err)
			}
			now := time.Now().UnixMilli()
			if _, err := db.ExecContext(context.Background(), test.statement, test.args(now, sessionID)...); err != nil {
				t.Fatalf("stage duplicate authority change: %v", err)
			}
			result := make(chan error, 1)
			go func() {
				_, err := proposals.CommitProposedEvent(context.Background(), sessionID, authority, request)
				result <- err
			}()
			var retryErr error
			received := false
			select {
			case retryErr = <-result:
				received = true
				if retryErr == nil {
					t.Fatal("duplicate proposal returned accepted receipt while authority change was pending")
				}
			case <-time.After(50 * time.Millisecond):
			}
			if _, err := db.ExecContext(context.Background(), `COMMIT`); err != nil {
				t.Fatalf("commit duplicate authority change: %v", err)
			}
			if !received {
				retryErr = <-result
			}
			if retryErr == nil {
				t.Fatal("duplicate proposal returned accepted receipt after authority change")
			}
			events := replayProposalEvents(t, proposals, sessionID)
			if len(events) != 1 || events[0].Seq != 1 {
				t.Fatalf("duplicate authority race changed durable events: %+v", events)
			}
		})
	}
}

func TestProposalReceiptFailureRollsBackEvent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	proposals := &sqliteProposalHarness{Store: openStore(t, path), path: path}
	seedProposalAuthorities(t, path)
	db := openRawSQLite(t, path)
	if _, err := db.ExecContext(context.Background(), `
CREATE TRIGGER fail_proposal_receipt BEFORE INSERT ON session_event_proposals
BEGIN SELECT RAISE(ABORT, 'proposal receipt failpoint'); END
`); err != nil {
		t.Fatalf("create proposal receipt failpoint: %v", err)
	}
	if _, err := proposals.CommitProposedEvent(context.Background(), "ses_proposal_rollback",
		store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}, store.ProposedEventRequest{
			ProposalID: "proposal_rollback", Event: store.PendingEvent{Type: "session.state", Time: testTime(1), Payload: []byte(`{"state":"ready"}`)},
		}); err == nil {
		t.Fatal("CommitProposedEvent() unexpectedly survived receipt failpoint")
	}
	if latest, err := proposals.LatestSeq(context.Background(), "ses_proposal_rollback"); err != nil || latest != 0 {
		t.Fatalf("proposal receipt rollback latest seq = %d, %v; want 0, nil", latest, err)
	}
}

func replayProposalEvents(t *testing.T, proposals store.ProposedEventStore, sessionID string) []store.Event {
	t.Helper()
	var events []store.Event
	if err := proposals.Replay(context.Background(), sessionID, 0, func(event store.Event) error {
		events = append(events, event)
		return nil
	}); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	return events
}

func TestAttachmentStoreContract(t *testing.T) {
	storetest.AttachmentContract(t, storetest.AttachmentHarness{
		Open: func(t *testing.T) store.AttachmentStore {
			t.Helper()
			path := filepath.Join(t.TempDir(), "events.db")
			return &sqliteAttachmentHarness{Store: openStore(t, path), path: path}
		},
		Reopen: func(t *testing.T, current store.AttachmentStore) store.AttachmentStore {
			t.Helper()
			harness := current.(*sqliteAttachmentHarness)
			if err := harness.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			harness.Store = openStore(t, harness.path)
			return harness
		},
	})
}

func TestAttachmentCorruptionFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name      string
		statement string
		args      func() []any
	}{
		{
			name:      "future creation window",
			statement: `UPDATE session_attachments SET created_at_ms = ?, updated_at_ms = ?, expires_at_ns = ? WHERE attach_id = ?`,
			args: func() []any {
				created := time.Now().Add(time.Minute).UnixMilli()
				return []any{created, created, (created + 10000) * int64(time.Millisecond), "attach_corrupt"}
			},
		},
		{
			name:      "oversized queue reason",
			statement: `UPDATE session_attachments SET status = 'queued', queue_reason = ?, blocking_session_id = ? WHERE attach_id = ?`,
			args: func() []any {
				return []any{strings.Repeat("x", 129), "ses_blocker", "attach_corrupt"}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "events.db")
			ledger := &sqliteAttachmentHarness{Store: openStore(t, path), path: path}
			request := store.AttachmentCreate{Identity: store.AttachmentIdentity{
				AttachID: "attach_corrupt", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target",
				TargetCredentialLineageRef: "lineage_attach_corrupt",
			}, ExpiresAt: time.Now().Add(20 * time.Second)}
			if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
				t.Fatalf("CreateAttachment() error = %v", err)
			}
			db := openRawSQLite(t, path)
			if _, err := db.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = ON`); err != nil {
				t.Fatalf("enable corruption fixture: %v", err)
			}
			if _, err := db.ExecContext(context.Background(), test.statement, test.args()...); err != nil {
				t.Fatalf("corrupt attachment row: %v", err)
			}
			if _, err := ledger.Attachment(context.Background(), request.Identity.AttachID); err == nil {
				t.Fatal("Attachment() accepted corrupt row")
			}
		})
	}
}

func TestAttachmentConflictAndCancellationLeaveNoPartialState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	ledger := &sqliteAttachmentHarness{Store: openStore(t, path), path: path}
	first := store.AttachmentCreate{Identity: store.AttachmentIdentity{
		AttachID: "attach_first", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target",
		TargetCredentialLineageRef: "lineage_first",
	}, ExpiresAt: time.Now().Add(20 * time.Second)}
	if _, err := ledger.CreateAttachment(context.Background(), first); err != nil {
		t.Fatalf("CreateAttachment() error = %v", err)
	}
	conflict := first
	conflict.Identity.AttachID = "attach_conflict"
	conflict.Identity.BootstrapSessionID = "ses_other_bootstrap"
	conflict.Identity.TargetCredentialLineageRef = "lineage_conflict"
	if _, err := ledger.CreateAttachment(context.Background(), conflict); err == nil {
		t.Fatal("target-conflicting CreateAttachment() unexpectedly succeeded")
	}
	if _, err := ledger.Attachment(context.Background(), conflict.Identity.AttachID); err == nil {
		t.Fatal("target-conflicting attachment left partial state")
	}
	cancel := store.AttachmentUpdate{Status: store.AttachmentCanceled, DeliveryState: store.AttachmentDeliveryPending,
		Blocker: &store.AttachmentBlocker{Kind: store.AttachmentBlockerNewRunRequired}}
	if _, err := ledger.UpdateAttachment(context.Background(), first.Identity.AttachID, 0, cancel); err != nil {
		t.Fatalf("cancel UpdateAttachment() error = %v", err)
	}
	if _, err := ledger.UpdateAttachment(context.Background(), first.Identity.AttachID, 1,
		store.AttachmentUpdate{Status: store.AttachmentStartReceived, DeliveryState: store.AttachmentDeliveryReceived}); err == nil {
		t.Fatal("canceled attachment recorded start receipt")
	}
}

func TestAttachmentStartReceiptRechecksExpiryAtWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	ledger := &sqliteAttachmentHarness{Store: openStore(t, path), path: path}
	request := store.AttachmentCreate{Identity: store.AttachmentIdentity{
		AttachID: "attach_expiry_race", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target",
		TargetCredentialLineageRef: "lineage_expiry_race",
	}, ExpiresAt: time.Now().Add(time.Second)}
	if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
		t.Fatalf("CreateAttachment() error = %v", err)
	}

	db := openRawSQLite(t, path)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open lock connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.ExecContext(context.Background(), `PRAGMA busy_timeout = 5000`); err != nil {
		t.Fatalf("set lock timeout: %v", err)
	}
	if _, err := conn.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("hold attachment write lock: %v", err)
	}

	updated := make(chan error, 1)
	go func() {
		_, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0,
			store.AttachmentUpdate{Status: store.AttachmentStartReceived, DeliveryState: store.AttachmentDeliveryReceived})
		updated <- err
	}()
	time.Sleep(100 * time.Millisecond)
	if delay := time.Until(request.ExpiresAt) + 50*time.Millisecond; delay > 0 {
		time.Sleep(delay)
	}
	if _, err := conn.ExecContext(context.Background(), `ROLLBACK`); err != nil {
		t.Fatalf("release attachment write lock: %v", err)
	}
	if err := <-updated; err == nil || !strings.Contains(err.Error(), "attachment version conflict") {
		t.Fatalf("expired write-time start receipt error = %v, want attachment version conflict", err)
	}

	attachment, err := ledger.Attachment(context.Background(), request.Identity.AttachID)
	if err != nil {
		t.Fatalf("Attachment() error = %v", err)
	}
	if attachment.Status != store.AttachmentJoinPending || attachment.DeliveryVersion != 0 ||
		attachment.ExpiresAt == nil || !attachment.ExpiresAt.Equal(request.ExpiresAt) {
		t.Fatalf("expired start receipt changed durable truth: %+v", attachment)
	}
}

func TestAttachmentUpdateBusyRetryIsBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	ledger := &sqliteAttachmentHarness{Store: openStore(t, path), path: path}
	request := store.AttachmentCreate{Identity: store.AttachmentIdentity{
		AttachID: "attach_busy_deadline", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target",
		TargetCredentialLineageRef: "lineage_busy_deadline",
	}, ExpiresAt: time.Now().Add(10 * time.Second)}
	if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
		t.Fatalf("CreateAttachment() error = %v", err)
	}

	db := openRawSQLite(t, path)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open lock connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("hold attachment write lock: %v", err)
	}
	started := time.Now()
	_, updateErr := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0,
		store.AttachmentUpdate{Status: store.AttachmentStartReceived, DeliveryState: store.AttachmentDeliveryReceived})
	elapsed := time.Since(started)
	if _, err := conn.ExecContext(context.Background(), `ROLLBACK`); err != nil {
		t.Fatalf("release attachment write lock: %v", err)
	}
	if !errors.Is(updateErr, context.DeadlineExceeded) {
		t.Fatalf("busy attachment update error = %v, want deadline exceeded", updateErr)
	}
	if elapsed < 1800*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("busy attachment update elapsed = %v, want bounded near two seconds", elapsed)
	}

	attachment, err := ledger.Attachment(context.Background(), request.Identity.AttachID)
	if err != nil {
		t.Fatalf("Attachment() error = %v", err)
	}
	if attachment.Status != store.AttachmentJoinPending || attachment.DeliveryVersion != 0 {
		t.Fatalf("timed-out attachment update changed durable truth: %+v", attachment)
	}
}

func TestAttachmentSummaryDoesNotAliasCallerOrAttachment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	ledger := &sqliteAttachmentHarness{Store: openStore(t, path), path: path}
	request := store.AttachmentCreate{Identity: store.AttachmentIdentity{
		AttachID: "attach_alias", BootstrapSessionID: "ses_bootstrap", TargetSessionID: "ses_target",
		TargetCredentialLineageRef: "lineage_alias",
	}, ExpiresAt: time.Now().Add(20 * time.Second)}
	if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
		t.Fatalf("CreateAttachment() error = %v", err)
	}
	reason := "capacity"
	blockingSessionID := "ses_blocker"
	expiresAt := request.ExpiresAt
	update := store.AttachmentUpdate{Status: store.AttachmentQueued, DeliveryState: store.AttachmentDeliveryPending,
		QueueReason: &reason, ExpiresAt: &expiresAt, BlockingSessionID: &blockingSessionID,
		Blocker: &store.AttachmentBlocker{Kind: store.AttachmentBlockerQueued, Reason: &reason,
			ExpiresAt: &expiresAt, BlockingSessionID: &blockingSessionID}}
	mutation, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, update)
	if err != nil {
		t.Fatalf("UpdateAttachment() error = %v", err)
	}
	wantReason, wantBlocker, wantExpiry := reason, blockingSessionID, expiresAt
	reason = "rewritten"
	blockingSessionID = "ses_rewritten"
	expiresAt = expiresAt.Add(-time.Second)
	*mutation.Attachment.ExpiresAt = mutation.Attachment.ExpiresAt.Add(-time.Second)
	if mutation.Summary.Blocker == nil || mutation.Summary.Blocker.Reason == nil || *mutation.Summary.Blocker.Reason != wantReason ||
		mutation.Summary.Blocker.BlockingSessionID == nil || *mutation.Summary.Blocker.BlockingSessionID != wantBlocker ||
		mutation.Summary.ExpiresAt == nil || !mutation.Summary.ExpiresAt.Equal(wantExpiry) {
		t.Fatalf("summary changed through pointer alias: %+v", mutation.Summary)
	}
	stored, err := ledger.Attachment(context.Background(), request.Identity.AttachID)
	if err != nil || stored.QueueReason == nil || *stored.QueueReason != wantReason || stored.BlockingSessionID == nil ||
		*stored.BlockingSessionID != wantBlocker || stored.ExpiresAt == nil || !stored.ExpiresAt.Equal(wantExpiry) {
		t.Fatalf("committed attachment changed through pointer alias: %+v, %v", stored, err)
	}
}

func TestPendingCommandLedgerStoresReferencesOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	ledger := &sqliteCommandHarness{Store: openStore(t, path), path: path}
	seedCommandAuthorities(t, path)
	marker := "ledger-secret-marker"
	request := store.PendingCommandRequest{CommandID: "cmd_reference_only", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
	if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}, store.PendingEvent{
		Type: "session.message", Time: testTime(1), Payload: []byte(fmt.Sprintf(`{"role":"user","content":[{"text":%q}]}`, marker)),
	}, request); err != nil {
		t.Fatalf("CommitPendingCommand() error = %v", err)
	}

	db := openRawSQLite(t, path)
	rows, err := db.QueryContext(context.Background(), `PRAGMA table_info(session_pending_commands)`)
	if err != nil {
		t.Fatalf("read pending-command columns: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatalf("scan pending-command column: %v", err)
		}
		lower := strings.ToLower(name)
		for _, forbidden := range []string{"payload", "content", "secret", "token", "credential", "provider"} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("pending-command column %q contains forbidden concept %q", name, forbidden)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate pending-command columns: %v", err)
	}

	var values string
	if err := db.QueryRowContext(context.Background(), `
SELECT session_id || '|' || cmd_id || '|' || type || '|' || event_seq || '|' || status || '|' || expires_at_ns
FROM session_pending_commands WHERE session_id = ? AND cmd_id = ?
`, "ses_command_1", request.CommandID).Scan(&values); err != nil {
		t.Fatalf("read pending-command values: %v", err)
	}
	if strings.Contains(values, marker) {
		t.Fatalf("pending-command row copied event content: %q", values)
	}
}

func TestPendingCommandCorruptStatusFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	ledger := &sqliteCommandHarness{Store: openStore(t, path), path: path}
	seedCommandAuthorities(t, path)
	authority := store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
	request := store.PendingCommandRequest{CommandID: "cmd_corrupt", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
	if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, store.PendingEvent{
		Type: "session.message", Time: testTime(1), Payload: []byte(`{"role":"user"}`),
	}, request); err != nil {
		t.Fatalf("CommitPendingCommand() error = %v", err)
	}
	db := openRawSQLite(t, path)
	if _, err := db.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("enable corruption fixture: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE session_pending_commands SET status = 'corrupt' WHERE cmd_id = ?`, request.CommandID); err != nil {
		t.Fatalf("corrupt pending-command status: %v", err)
	}
	if _, err := ledger.ClaimPendingCommand(context.Background(), "ses_command_1", authority, request.CommandID); err == nil {
		t.Fatal("ClaimPendingCommand() accepted corrupt status")
	}
	if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, store.PendingEvent{
		Type: "session.message", Time: testTime(2), Payload: []byte(`{"role":"user"}`),
	}, request); err == nil {
		t.Fatal("duplicate CommitPendingCommand() returned corrupt ledger truth")
	}
}

func TestPendingCommandCorruptReferenceFailsClosed(t *testing.T) {
	futureCreatedAt := time.Now().Add(time.Minute).UnixMilli()
	for _, test := range []struct {
		name      string
		statement string
		args      []any
	}{
		{
			name:      "missing event",
			statement: `DELETE FROM session_events WHERE session_id = ? AND seq = 1`,
			args:      []any{"ses_command_1"},
		},
		{
			name:      "wrong event type",
			statement: `UPDATE session_events SET type = 'session.state' WHERE session_id = ? AND seq = 1`,
			args:      []any{"ses_command_1"},
		},
		{
			name:      "non-user event",
			statement: `UPDATE session_events SET payload = ? WHERE session_id = ? AND seq = 1`,
			args:      []any{[]byte(`{"role":"agent"}`), "ses_command_1"},
		},
		{
			name:      "unbounded expiry",
			statement: `UPDATE session_pending_commands SET expires_at_ns = (created_at_ms + 31000) * 1000000 WHERE session_id = ? AND cmd_id = ?`,
			args:      []any{"ses_command_1", "cmd_corrupt_reference"},
		},
		{
			name:      "future expiry window",
			statement: `UPDATE session_pending_commands SET created_at_ms = ?, expires_at_ns = ? WHERE session_id = ? AND cmd_id = ?`,
			args:      []any{futureCreatedAt, (futureCreatedAt + 10000) * int64(time.Millisecond), "ses_command_1", "cmd_corrupt_reference"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "events.db")
			ledger := &sqliteCommandHarness{Store: openStore(t, path), path: path}
			seedCommandAuthorities(t, path)
			authority := store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
			request := store.PendingCommandRequest{CommandID: "cmd_corrupt_reference", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
			event := store.PendingEvent{Type: "session.message", Time: testTime(1), Payload: []byte(`{"role":"user"}`)}
			if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, event, request); err != nil {
				t.Fatalf("CommitPendingCommand() error = %v", err)
			}
			db := openRawSQLite(t, path)
			if _, err := db.ExecContext(context.Background(), `PRAGMA foreign_keys = OFF`); err != nil {
				t.Fatalf("disable foreign keys for corruption fixture: %v", err)
			}
			if _, err := db.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = ON`); err != nil {
				t.Fatalf("enable corruption fixture: %v", err)
			}
			if _, err := db.ExecContext(context.Background(), test.statement, test.args...); err != nil {
				t.Fatalf("corrupt pending-command reference: %v", err)
			}
			if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, event, request); err == nil {
				t.Fatal("duplicate CommitPendingCommand() returned corrupt reference truth")
			}
			if _, err := ledger.ClaimPendingCommand(context.Background(), "ses_command_1", authority, request.CommandID); err == nil {
				t.Fatal("ClaimPendingCommand() accepted corrupt reference truth")
			}
		})
	}
}

func TestPendingCommandPayloadBound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	ledger := &sqliteCommandHarness{Store: openStore(t, path), path: path}
	seedCommandAuthorities(t, path)
	authority := store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
	request := store.PendingCommandRequest{CommandID: "cmd_payload_exact", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
	if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, store.PendingEvent{
		Type: "session.message", Time: testTime(1), Payload: userPayloadBytes(t, 64*1024),
	}, request); err != nil {
		t.Fatalf("exact-bound CommitPendingCommand() error = %v", err)
	}
	request.CommandID = "cmd_payload_oversized"
	if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, store.PendingEvent{
		Type: "session.message", Time: testTime(2), Payload: userPayloadBytes(t, 64*1024+1),
	}, request); err == nil {
		t.Fatal("oversized CommitPendingCommand() unexpectedly succeeded")
	}

	db := openRawSQLite(t, path)
	if _, err := db.ExecContext(context.Background(), `UPDATE session_events SET payload = ? WHERE session_id = ? AND seq = 1`, userPayloadBytes(t, 64*1024+1), "ses_command_1"); err != nil {
		t.Fatalf("corrupt event payload size: %v", err)
	}
	request.CommandID = "cmd_payload_exact"
	if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, store.PendingEvent{
		Type: "session.message", Time: testTime(3), Payload: []byte(`{"role":"user"}`),
	}, request); err == nil {
		t.Fatal("duplicate CommitPendingCommand() accepted oversized referenced payload")
	}
}

func TestPendingCommandOversizedCorruptReferenceIsNotMaterialized(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	ledger := &sqliteCommandHarness{Store: openStore(t, path), path: path}
	seedCommandAuthorities(t, path)
	authority := store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}
	request := store.PendingCommandRequest{CommandID: "cmd_payload_materialization", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
	event := store.PendingEvent{Type: "session.message", Time: testTime(1), Payload: []byte(`{"role":"user"}`)}
	if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, event, request); err != nil {
		t.Fatalf("CommitPendingCommand() error = %v", err)
	}
	db := openRawSQLite(t, path)
	if _, err := db.ExecContext(context.Background(), `UPDATE session_events SET payload = zeroblob(8 * 1024 * 1024) WHERE session_id = ? AND seq = 1`, "ses_command_1"); err != nil {
		t.Fatalf("corrupt event payload size: %v", err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, event, request); err == nil {
		t.Fatal("duplicate CommitPendingCommand() accepted oversized referenced payload")
	}
	runtime.ReadMemStats(&after)
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 2*1024*1024 {
		t.Fatalf("oversized corrupt reference allocated %d bytes, want <= 2 MiB", allocated)
	}
}

func TestPendingCommandQueuedBehindAuthorityChangeWritesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	ledger := &sqliteCommandHarness{Store: openStore(t, path), path: path}
	seedCommandAuthorities(t, path)
	db := openRawSQLite(t, path)
	if _, err := db.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("begin authority change: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
UPDATE session_adapter_connections SET connection_epoch = 2, updated_at_ms = ? WHERE session_id = ?
`, time.Now().UnixMilli(), "ses_command_1"); err != nil {
		t.Fatalf("stage authority change: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := ledger.CommitPendingCommand(ctx, "ses_command_1", store.CommandAuthority{ConnectionEpoch: 1, CredentialGeneration: 1}, store.PendingEvent{
			Type: "session.message", Time: testTime(1), Payload: []byte(`{"role":"user"}`),
		}, store.PendingCommandRequest{CommandID: "cmd_queued_stale", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)})
		result <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if _, err := db.ExecContext(context.Background(), `COMMIT`); err != nil {
		t.Fatalf("commit authority change: %v", err)
	}
	if err := <-result; err == nil {
		t.Fatal("queued stale CommitPendingCommand() unexpectedly succeeded")
	}
	latest, err := ledger.LatestSeq(context.Background(), "ses_command_1")
	if err != nil || latest != 0 {
		t.Fatalf("queued stale latest seq = %d, %v; want 0, nil", latest, err)
	}
	var commands int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM session_pending_commands WHERE session_id = ?`, "ses_command_1").Scan(&commands); err != nil {
		t.Fatalf("count queued stale commands: %v", err)
	}
	if commands != 0 {
		t.Fatalf("queued stale pending commands = %d, want 0", commands)
	}
}

func TestClosedStoreRejectsOperations(t *testing.T) {
	st, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := st.Append(context.Background(), "ses_closed", []store.PendingEvent{{Type: "session.message", Time: testTime(1), Payload: []byte(`{}`)}}); err == nil {
		t.Fatal("Append() on closed store unexpectedly succeeded")
	}
	if err := st.Replay(context.Background(), "ses_closed", 0, func(store.Event) error { return nil }); err == nil {
		t.Fatal("Replay() on closed store unexpectedly succeeded")
	}
	if _, err := st.LatestSeq(context.Background(), "ses_closed"); err == nil {
		t.Fatal("LatestSeq() on closed store unexpectedly succeeded")
	}
	if _, err := st.History(context.Background(), "ses_closed", nil, 1); err == nil {
		t.Fatal("History() on closed store unexpectedly succeeded")
	}
}

func TestReplayRejectsNilCallback(t *testing.T) {
	st := openStore(t, filepath.Join(t.TempDir(), "events.db"))
	if err := st.Replay(context.Background(), "ses_nil_callback", 0, nil); err == nil {
		t.Fatal("Replay(nil) unexpectedly succeeded")
	}
}

func TestHistoryReportsInternalRetentionGap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	st := openStore(t, path)
	if _, err := st.Append(context.Background(), "ses_history_internal_gap", []store.PendingEvent{
		{Type: "session.message", Time: testTime(1), Payload: []byte(`{"n":1}`)},
		{Type: "session.message", Time: testTime(2), Payload: []byte(`{"n":2}`)},
		{Type: "session.message", Time: testTime(3), Payload: []byte(`{"n":3}`)},
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open history database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(context.Background(), `
		DELETE FROM session_events WHERE session_id = ? AND seq = ?
	`, "ses_history_internal_gap", 2); err != nil {
		t.Fatalf("delete internal history event: %v", err)
	}

	page, err := st.History(context.Background(), "ses_history_internal_gap", nil, 100)
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	if page.RetentionState != store.RetentionGap || page.LatestSeq != 3 || len(page.Events) != 2 || page.Events[0].Seq != 1 || page.Events[1].Seq != 3 {
		t.Fatalf("History() = %+v, want internal retention gap", page)
	}
}

func testTime(sequence int64) time.Time {
	return time.UnixMilli(1764937200000 + sequence)
}

func openStore(t *testing.T, path string) *sqlite.Store {
	t.Helper()
	st, err := sqlite.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return st
}

type sqliteCommandHarness struct {
	*sqlite.Store
	path string
}

type sqliteProposalHarness struct {
	*sqlite.Store
	path string
}

type sqliteConnectionHarness struct {
	*sqlite.Store
	path string
}

func (h *sqliteConnectionHarness) InitializeAdapterConnection(ctx context.Context, request store.AdapterConnectionInitialize) (store.AdapterConnection, error) {
	request.ActiveCredentialExpiresAt = stableContractExpiry(request.ActiveCredentialExpiresAt)
	return h.Store.InitializeAdapterConnection(ctx, request)
}

func (h *sqliteConnectionHarness) PrepareAdapterCredentialRotation(ctx context.Context, sessionID string, rotation store.AdapterCredentialRotation) (store.AdapterConnection, error) {
	rotation.ExpiresAt = stableContractExpiry(rotation.ExpiresAt)
	return h.Store.PrepareAdapterCredentialRotation(ctx, sessionID, rotation)
}

func stableContractExpiry(expiry time.Time) time.Time {
	minimum := time.Now().Add(5 * time.Millisecond)
	if expiry.Before(minimum) {
		return minimum
	}
	return expiry
}

func (h *sqliteConnectionHarness) AcceptAdapterHello(ctx context.Context, sessionID string, hello store.AdapterHello) (store.AdapterConnection, error) {
	connection, err := h.Store.AcceptAdapterHello(ctx, sessionID, hello)
	if err != nil {
		return store.AdapterConnection{}, err
	}
	if fence, err := h.Store.AllocateAdapterGrantFence(ctx); err != nil || fence <= connection.AcceptedFence {
		return store.AdapterConnection{}, fmt.Errorf("preallocate post-hello grant fence %d after %d: %v", fence, connection.AcceptedFence, err)
	}
	return connection, nil
}

func (h *sqliteConnectionHarness) ActivateAdapterCredential(ctx context.Context, sessionID string, activation store.AdapterCredentialActivation) (store.AdapterConnection, error) {
	connection, err := h.Store.ActivateAdapterCredential(ctx, sessionID, activation)
	if err != nil {
		return store.AdapterConnection{}, err
	}
	if fence, err := h.Store.AllocateAdapterGrantFence(ctx); err != nil || fence <= connection.AcceptedFence {
		return store.AdapterConnection{}, fmt.Errorf("preallocate post-activation grant fence %d after %d: %v", fence, connection.AcceptedFence, err)
	}
	return connection, nil
}

type sqliteAttachmentHarness struct {
	*sqlite.Store
	path string
}

func invalidateAdapterConnection(t *testing.T, current store.AdapterConnectionStore, terminal bool) {
	t.Helper()
	harness := current.(*sqliteConnectionHarness)
	db := openRawSQLite(t, harness.path)
	var now int64
	if err := db.QueryRowContext(context.Background(), `SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)`).Scan(&now); err != nil {
		t.Fatalf("read invalidation Store clock: %v", err)
	}
	column := "revoked_at_ms"
	if terminal {
		column = "terminal_at_ms"
	}
	if _, err := db.ExecContext(context.Background(), `
UPDATE session_adapter_connections SET `+column+` = ?, updated_at_ms = ? WHERE session_id = 'ses_connection'
`, now, now); err != nil {
		t.Fatalf("invalidate adapter connection terminal=%v: %v", terminal, err)
	}
}

func seedProposalAuthorities(t *testing.T, path string) {
	t.Helper()
	db := openRawSQLite(t, path)
	now := time.Now().UnixMilli()
	for _, sessionID := range []string{
		"ses_proposal_1", "ses_proposal_conflict", "ses_proposal_snapshot", "ses_proposal_stale", "ses_proposal_reopen",
		"ses_proposal_corrupt", "ses_proposal_queued", "ses_proposal_rollback",
		"ses_proposal_duplicate_epoch", "ses_proposal_duplicate_generation", "ses_proposal_duplicate_revoked",
		"ses_proposal_duplicate_expired", "ses_proposal_duplicate_terminal",
	} {
		if _, err := db.ExecContext(context.Background(), `
INSERT INTO session_adapter_connections (
    session_id, connection_epoch, accepted_fence, active_credential_generation,
    credential_generation_high_watermark, active_credential_expires_at_ms, created_at_ms, updated_at_ms
) VALUES (?, 1, 1, 1, 1, ?, ?, ?)
`, sessionID, now+int64(time.Hour/time.Millisecond), now, now); err != nil {
			t.Fatalf("seed proposal authority for %s: %v", sessionID, err)
		}
	}
	seedFixtureFence(t, path, 2)
}

func invalidateProposalAuthority(t *testing.T, current store.ProposedEventStore, kind storetest.CommandAuthorityFailure) {
	t.Helper()
	harness := current.(*sqliteProposalHarness)
	db := openRawSQLite(t, harness.path)
	now := time.Now().UnixMilli()
	var statement string
	var args []any
	switch kind {
	case storetest.CommandAuthoritySuperseded:
		statement = `UPDATE session_adapter_connections SET connection_epoch = 2, updated_at_ms = ? WHERE session_id = 'ses_proposal_stale'`
		args = []any{now}
	case storetest.CommandAuthorityRevoked:
		statement = `UPDATE session_adapter_connections SET revoked_at_ms = ?, updated_at_ms = ? WHERE session_id = 'ses_proposal_stale'`
		args = []any{now, now}
	case storetest.CommandAuthorityExpired:
		statement = `UPDATE session_adapter_connections SET created_at_ms = ?, active_credential_expires_at_ms = ?, updated_at_ms = ? WHERE session_id = 'ses_proposal_stale'`
		args = []any{now - int64((2*time.Minute)/time.Millisecond), now - 1, now}
	case storetest.CommandAuthorityTerminal:
		statement = `UPDATE session_adapter_connections SET terminal_at_ms = ?, updated_at_ms = ? WHERE session_id = 'ses_proposal_stale'`
		args = []any{now, now}
	default:
		t.Fatalf("unknown proposal authority failure %q", kind)
	}
	if _, err := db.ExecContext(context.Background(), statement, args...); err != nil {
		t.Fatalf("invalidate proposal authority %s: %v", kind, err)
	}
}

func seedCommandAuthorities(t *testing.T, path string) {
	t.Helper()
	db := openRawSQLite(t, path)
	now := time.Now().UnixMilli()
	for _, sessionID := range []string{
		"ses_command_1", "ses_command_claim", "ses_command_stale",
		"ses_command_expired", "ses_command_reopen", "ses_command_invalid",
		"ses_command_unknown",
	} {
		if _, err := db.ExecContext(context.Background(), `
INSERT INTO session_adapter_connections (
    session_id, connection_epoch, accepted_fence, active_credential_generation,
    credential_generation_high_watermark, active_credential_expires_at_ms, created_at_ms, updated_at_ms
) VALUES (?, 1, 1, 1, 1, ?, ?, ?)
`, sessionID, now+int64(time.Hour/time.Millisecond), now, now); err != nil {
			t.Fatalf("seed command authority for %s: %v", sessionID, err)
		}
	}
	seedFixtureFence(t, path, 2)
}

func seedFixtureFence(t *testing.T, path string, next int64) {
	t.Helper()
	for _, fixture := range []struct{ path, table string }{{path, "session_adapter_fence_allocator"}, {path + ".fences", "adapter_fence_allocator"}} {
		db := openRawSQLite(t, fixture.path)
		if _, err := db.ExecContext(context.Background(), `UPDATE `+fixture.table+` SET next_fence = ? WHERE singleton = 1`, next); err != nil {
			t.Fatalf("seed fixture fence %s: %v", fixture.table, err)
		}
	}
}

func invalidateCommandAuthority(t *testing.T, current store.CommandLedgerStore, kind storetest.CommandAuthorityFailure) {
	t.Helper()
	harness := current.(*sqliteCommandHarness)
	db := openRawSQLite(t, harness.path)
	now := time.Now().UnixMilli()
	var statement string
	var args []any
	switch kind {
	case storetest.CommandAuthoritySuperseded:
		statement = `UPDATE session_adapter_connections SET connection_epoch = 2, updated_at_ms = ?`
		args = []any{now}
	case storetest.CommandAuthorityRevoked:
		statement = `UPDATE session_adapter_connections SET revoked_at_ms = ?, updated_at_ms = ?`
		args = []any{now, now}
	case storetest.CommandAuthorityExpired:
		statement = `UPDATE session_adapter_connections SET created_at_ms = ?, active_credential_expires_at_ms = ?, updated_at_ms = ?`
		args = []any{now - int64((2*time.Minute)/time.Millisecond), now - 1, now}
	case storetest.CommandAuthorityTerminal:
		statement = `UPDATE session_adapter_connections SET terminal_at_ms = ?, updated_at_ms = ?`
		args = []any{now, now}
	default:
		t.Fatalf("unknown command authority failure %q", kind)
	}
	if _, err := db.ExecContext(context.Background(), statement, args...); err != nil {
		t.Fatalf("invalidate command authority %s: %v", kind, err)
	}
}

func openRawSQLite(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw sqlite database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func userPayloadBytes(t *testing.T, size int) []byte {
	t.Helper()
	prefix := []byte(`{"role":"user","padding":"`)
	suffix := []byte(`"}`)
	if size < len(prefix)+len(suffix) {
		t.Fatalf("payload size %d is too small", size)
	}
	payload := make([]byte, size)
	copy(payload, prefix)
	for index := len(prefix); index < size-len(suffix); index++ {
		payload[index] = 'x'
	}
	copy(payload[size-len(suffix):], suffix)
	return payload
}
