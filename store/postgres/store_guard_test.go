package postgres_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/postgres"
)

// TestStoreRejectsUnusableSessionIdentifierBeforeDatabaseAccess pins the
// fail-closed input guard on every mutating ledger entry point.
//
// The store is backed by a pool that has already been closed, so any entry
// point that reached the database would surface a pgxpool "closed pool" error
// rather than its own validation error. Asserting the exact validation message
// is therefore what proves the guard short-circuits before a transaction is
// opened, keeping unvalidated session identifiers out of SQL entirely.
func TestStoreRejectsUnusableSessionIdentifierBeforeDatabaseAccess(t *testing.T) {
	ctx := context.Background()
	ledger := postgres.New(closedPool(t))

	for _, testCase := range []struct {
		name string
		want string
		call func() error
	}{
		{"PublishSettingsCapability", "invalid settings capability", func() error {
			_, err := ledger.PublishSettingsCapability(ctx, "", store.SettingsCapabilityUpdate{})
			return err
		}},
		{"SettingsCommandReserve", "invalid settings command reservation", func() error {
			_, err := ledger.SettingsCommandReserve(ctx, "", store.SettingsCommandRequest{})
			return err
		}},
		{"AcknowledgeSettingsCommandDelivery", "invalid settings delivery acknowledgement", func() error {
			_, err := ledger.AcknowledgeSettingsCommandDelivery(ctx, "", "cmd_guard", 1, store.SettingsWriter{})
			return err
		}},
		{"RecoverSettingsCommand", "invalid settings recovery", func() error {
			_, err := ledger.RecoverSettingsCommand(ctx, "", "cmd_guard", store.SettingsWriter{})
			return err
		}},
		{"FinalizeSettingsCommand", "invalid settings finalization", func() error {
			_, err := ledger.FinalizeSettingsCommand(ctx, "", "cmd_guard", store.SettingsCommandFinalize{})
			return err
		}},
		{"SettingsCommand", "invalid settings command lookup", func() error {
			_, err := ledger.SettingsCommand(ctx, "", "cmd_guard")
			return err
		}},
		{"PendingSettingsCommands", "invalid settings pending Session", func() error {
			_, err := ledger.PendingSettingsCommands(ctx, "")
			return err
		}},
		{"PublishRunControlCapability", "invalid run-control capability", func() error {
			_, err := ledger.PublishRunControlCapability(ctx, "", store.RunControlCapabilityUpdate{})
			return err
		}},
		{"RunControlReserve", "invalid run-control reservation", func() error {
			_, err := ledger.RunControlReserve(ctx, "", store.RunControlRequest{})
			return err
		}},
		{"RunControlFinalize", "invalid run-control finalization", func() error {
			_, err := ledger.RunControlFinalize(ctx, "", "cmd_guard", store.RunControlFinalize{})
			return err
		}},
		{"RecoverRunControl", "invalid run-control recovery", func() error {
			_, err := ledger.RecoverRunControl(ctx, "", "cmd_guard", "adapter_disconnected")
			return err
		}},
		{"PublishFileReferenceCapability", "invalid file-reference capability", func() error {
			_, err := ledger.PublishFileReferenceCapability(ctx, "", store.FileReferenceCapabilityUpdate{})
			return err
		}},
		{"CommitFileReferenceCommand", "invalid file-reference command", func() error {
			_, err := ledger.CommitFileReferenceCommand(ctx, "", store.PendingEvent{}, store.FileReferenceCommandRequest{})
			return err
		}},
		{"AcknowledgeFileReferenceDelivery", "invalid file-reference delivery", func() error {
			_, err := ledger.AcknowledgeFileReferenceDelivery(ctx, "", "cmd_guard", 1, store.FileReferenceWriter{})
			return err
		}},
		{"FinalizeFileReferenceCommand", "invalid file-reference finalization", func() error {
			_, err := ledger.FinalizeFileReferenceCommand(ctx, "", "cmd_guard", store.FileReferenceCommandFinalize{})
			return err
		}},
		{"InitializeAdapterConnection", "invalid adapter connection initialization", func() error {
			_, err := ledger.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{})
			return err
		}},
		{"ValidateWarmAttachTargetActivation", "invalid warm attach target activation", func() error {
			return ledger.ValidateWarmAttachTargetActivation(ctx, "", store.WarmAttachTargetActivation{})
		}},
		{"RefreshAdapterCredentialBeforeHello", "invalid pre-hello adapter credential refresh", func() error {
			_, err := ledger.RefreshAdapterCredentialBeforeHello(ctx, "", store.AdapterCredentialPreHelloRefresh{})
			return err
		}},
		{"TerminateAdapterConnectionBeforeHello", "invalid pre-hello adapter connection termination", func() error {
			_, err := ledger.TerminateAdapterConnectionBeforeHello(ctx, "", store.AdapterConnectionPreHelloTermination{})
			return err
		}},
		{"AcceptAdapterHello", "invalid adapter hello", func() error {
			_, err := ledger.AcceptAdapterHello(ctx, "", store.AdapterHello{})
			return err
		}},
		{"AdapterConnection", "invalid adapter connection session", func() error {
			_, err := ledger.AdapterConnection(ctx, "")
			return err
		}},
		{"ExpireWarmAttach", "invalid warm attach expiry", func() error {
			_, err := ledger.ExpireWarmAttach(ctx, "", 0)
			return err
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.call()
			if err == nil {
				t.Fatalf("%s accepted an unusable session identifier", testCase.name)
			}
			if err.Error() != testCase.want {
				t.Fatalf("%s = %q, want %q (a pool error here means the guard reached the database)", testCase.name, err, testCase.want)
			}
		})
	}
}

// TestStoreRejectsOverlongSessionIdentifier covers the upper bound of
// validConnectionID from the opposite side of the empty-string case: an
// identifier longer than the 255-character column width must be refused by the
// same guard rather than truncated into a different session's row.
func TestStoreRejectsOverlongSessionIdentifier(t *testing.T) {
	ctx := context.Background()
	ledger := postgres.New(closedPool(t))
	overlong := strings.Repeat("s", 256)

	if _, err := ledger.AdapterConnection(ctx, overlong); err == nil || err.Error() != "invalid adapter connection session" {
		t.Fatalf("AdapterConnection(overlong) = %v, want invalid adapter connection session", err)
	}
	if _, err := ledger.PendingSettingsCommands(ctx, overlong); err == nil || err.Error() != "invalid settings pending Session" {
		t.Fatalf("PendingSettingsCommands(overlong) = %v, want invalid settings pending Session", err)
	}
	if _, err := ledger.SettingsCommand(ctx, overlong, "cmd_guard"); err == nil || err.Error() != "invalid settings command lookup" {
		t.Fatalf("SettingsCommand(overlong) = %v, want invalid settings command lookup", err)
	}
}

// TestUnconfiguredStoreFailsClosedInsteadOfPanicking asserts that every entry
// point on a Store built without a pool returns an error rather than
// dereferencing the nil pool.
//
// This is the regression fence for the nil-pool guard that each entry point
// carries: without it a misconfigured store crashes the process on first use
// instead of surfacing a diagnosable error, and a newly added method that
// forgets the guard would panic here rather than in production.
func TestUnconfiguredStoreFailsClosedInsteadOfPanicking(t *testing.T) {
	ctx := context.Background()
	ledger := postgres.New(nil)
	events := []store.PendingEvent{{Type: "session.message", Payload: []byte(`{}`)}}
	key := store.WorkspaceLeaseKey{1}

	for _, testCase := range []struct {
		name string
		call func() error
	}{
		{"AttentionSnapshot", func() error {
			_, err := ledger.AttentionSnapshot(ctx, []string{"ses_unconfigured"})
			return err
		}},
		{"AttentionSummaryPage", func() error {
			_, err := ledger.AttentionSummaryPage(ctx, store.AttentionSummaryPageRequest{Limit: 1})
			return err
		}},
		{"Append", func() error {
			_, err := ledger.Append(ctx, "ses_unconfigured", events)
			return err
		}},
		{"Replay", func() error {
			return ledger.Replay(ctx, "ses_unconfigured", 0, func(store.Event) error { return nil })
		}},
		{"History", func() error {
			_, err := ledger.History(ctx, "ses_unconfigured", nil, 1)
			return err
		}},
		{"LatestSeq", func() error {
			_, err := ledger.LatestSeq(ctx, "ses_unconfigured")
			return err
		}},
		{"CommitPendingCommand", func() error {
			_, err := ledger.CommitPendingCommand(ctx, "ses_unconfigured", store.CommandAuthority{}, events[0], store.PendingCommandRequest{})
			return err
		}},
		{"ListPendingCommands", func() error {
			_, err := ledger.ListPendingCommands(ctx, "ses_unconfigured", store.CommandAuthority{})
			return err
		}},
		{"ResolvePendingCommandUnknown", func() error {
			_, err := ledger.ResolvePendingCommandUnknown(ctx, "ses_unconfigured", "cmd_unconfigured")
			return err
		}},
		{"CommitProposedEvent", func() error {
			_, err := ledger.CommitProposedEvent(ctx, "ses_unconfigured", store.CommandAuthority{}, store.ProposedEventRequest{})
			return err
		}},
		{"WithAdapterConnectionTransaction", func() error {
			return ledger.WithAdapterConnectionTransaction(ctx, func(store.AdapterConnectionStore) error { return nil })
		}},
		{"AcceptAdapterHello", func() error {
			_, err := ledger.AcceptAdapterHello(ctx, "ses_unconfigured", store.AdapterHello{CredentialGeneration: 1})
			return err
		}},
		{"AdapterConnection", func() error {
			_, err := ledger.AdapterConnection(ctx, "ses_unconfigured")
			return err
		}},
		{"AllocateAdapterGrantFence", func() error {
			_, err := ledger.AllocateAdapterGrantFence(ctx)
			return err
		}},
		{"AppendAdapterEvents", func() error {
			_, err := ledger.AppendAdapterEvents(ctx, "ses_unconfigured", store.AdapterConnectionAdmission{}, events)
			return err
		}},
		{"ValidateAdapterEffectAdmission", func() error {
			_, err := ledger.ValidateAdapterEffectAdmission(ctx, "ses_unconfigured", store.AdapterConnectionAdmission{})
			return err
		}},
		{"CommitAttachAttempt", func() error {
			_, err := ledger.CommitAttachAttempt(ctx, store.AttachAttemptRequest{})
			return err
		}},
		{"AttachAttempt", func() error {
			_, err := ledger.AttachAttempt(ctx, [32]byte{1})
			return err
		}},
		{"CommitWarmAttach", func() error {
			_, err := ledger.CommitWarmAttach(ctx, store.WarmAttachRequest{})
			return err
		}},
		{"ExpireWarmAttach", func() error {
			_, err := ledger.ExpireWarmAttach(ctx, "att_unconfigured", 0)
			return err
		}},
		{"ReserveWorkspaceLease", func() error {
			_, err := ledger.ReserveWorkspaceLease(ctx, store.WorkspaceLeaseReserve{})
			return err
		}},
		{"WorkspaceLease", func() error {
			_, err := ledger.WorkspaceLease(ctx, key)
			return err
		}},
		{"RecordWorkspaceStartReceived", func() error {
			_, err := ledger.RecordWorkspaceStartReceived(ctx, key, 1, store.WorkspaceLeaseOwner{})
			return err
		}},
		{"QuarantineWorkspaceLease", func() error {
			_, err := ledger.QuarantineWorkspaceLease(ctx, key, 1)
			return err
		}},
		{"ReleaseWorkspaceLeaseAfterQuiescence", func() error {
			_, err := ledger.ReleaseWorkspaceLeaseAfterQuiescence(ctx, key, 1, store.WorkspaceLeaseOwner{})
			return err
		}},
		{"CreateAttachment", func() error {
			_, err := ledger.CreateAttachment(ctx, store.AttachmentCreate{})
			return err
		}},
		{"Attachment", func() error {
			_, err := ledger.Attachment(ctx, "att_unconfigured")
			return err
		}},
		{"AttachmentForTarget", func() error {
			_, err := ledger.AttachmentForTarget(ctx, "ses_unconfigured")
			return err
		}},
		{"UpdateAttachment", func() error {
			_, err := ledger.UpdateAttachment(ctx, "att_unconfigured", 1, store.AttachmentUpdate{})
			return err
		}},
		{"BackfillAttentionBatch", func() error {
			_, err := ledger.BackfillAttentionBatch(ctx, postgres.AttentionBackfillCheckpoint{}, 1)
			return err
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("%s panicked on an unconfigured store instead of failing closed: %v", testCase.name, recovered)
				}
			}()
			if err := testCase.call(); err == nil {
				t.Fatalf("%s accepted an unconfigured store", testCase.name)
			}
		})
	}
}

// TestStoreRejectsMalformedReadRequests covers the request-shape guards on the
// read paths, which are reached only after the nil-pool check and so cannot be
// exercised by the unconfigured-store sweep. The closed pool again means a
// guard that failed to fire would surface a pool error instead.
func TestStoreRejectsMalformedReadRequests(t *testing.T) {
	ctx := context.Background()
	ledger := postgres.New(closedPool(t))

	t.Run("attention snapshot rejects an empty session set", func(t *testing.T) {
		if _, err := ledger.AttentionSnapshot(ctx, nil); err == nil {
			t.Fatal("AttentionSnapshot accepted an empty session set")
		}
	})
	t.Run("attention snapshot rejects an unusable member", func(t *testing.T) {
		if _, err := ledger.AttentionSnapshot(ctx, []string{""}); err == nil {
			t.Fatal("AttentionSnapshot accepted an unusable session ID")
		}
	})
	t.Run("attention snapshot rejects duplicates", func(t *testing.T) {
		// Duplicates would make the returned summary count disagree with the
		// requested count, so they are refused rather than silently deduped.
		if _, err := ledger.AttentionSnapshot(ctx, []string{"ses_dup", "ses_dup"}); err == nil {
			t.Fatal("AttentionSnapshot accepted a duplicated session ID")
		}
	})
	t.Run("attention summary page rejects an out-of-range limit", func(t *testing.T) {
		if _, err := ledger.AttentionSummaryPage(ctx, store.AttentionSummaryPageRequest{Limit: 0}); err == nil {
			t.Fatal("AttentionSummaryPage accepted a zero limit")
		}
		if _, err := ledger.AttentionSummaryPage(ctx, store.AttentionSummaryPageRequest{Limit: store.MaxAttentionSummaryPageSize + 1}); err == nil {
			t.Fatal("AttentionSummaryPage accepted an oversized limit")
		}
	})
	t.Run("attention summary page rejects an unusable cursor", func(t *testing.T) {
		if _, err := ledger.AttentionSummaryPage(ctx, store.AttentionSummaryPageRequest{Limit: 1, AfterSessionID: strings.Repeat("s", 256)}); err == nil {
			t.Fatal("AttentionSummaryPage accepted an unusable cursor")
		}
	})
	t.Run("replay rejects a nil visitor", func(t *testing.T) {
		if err := ledger.Replay(ctx, "ses_guard", 0, nil); err == nil {
			t.Fatal("Replay accepted a nil visitor")
		}
	})
	t.Run("adapter event append rejects an empty batch", func(t *testing.T) {
		if _, err := ledger.AppendAdapterEvents(ctx, "ses_guard", store.AdapterConnectionAdmission{}, nil); err == nil {
			t.Fatal("AppendAdapterEvents accepted an empty batch")
		}
	})
	t.Run("attention backfill rejects an out-of-range batch size", func(t *testing.T) {
		if _, err := ledger.BackfillAttentionBatch(ctx, postgres.AttentionBackfillCheckpoint{}, 0); err == nil {
			t.Fatal("BackfillAttentionBatch accepted a zero batch size")
		}
		if _, err := ledger.BackfillAttentionBatch(ctx, postgres.AttentionBackfillCheckpoint{}, 1<<20); err == nil {
			t.Fatal("BackfillAttentionBatch accepted an oversized batch size")
		}
	})
	t.Run("attention backfill rejects a missing checkpoint store", func(t *testing.T) {
		if _, err := ledger.RunAttentionBackfill(ctx, nil, 1); err == nil {
			t.Fatal("RunAttentionBackfill accepted a nil checkpoint store")
		}
	})
}

// closedPool builds a pool against an unroutable address and closes it, so any
// database access through it fails loudly. The DSN is never dialed: pgxpool
// connects lazily, and the pool is closed before the store sees it.
func closedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	pool, err := pgxpool.New(context.Background(), "postgres://unused:unused@127.0.0.1:1/unused?sslmode=disable")
	if err != nil {
		t.Fatalf("build guard-only pool: %v", err)
	}
	pool.Close()
	return pool
}
