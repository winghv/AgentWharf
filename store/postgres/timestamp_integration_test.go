package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/postgres"
)

func TestPostgresTimestampRoundTripIdempotency(t *testing.T) {
	t.Run("attach attempt", func(t *testing.T) {
		dsn := testDSN(t)
		schemaName := testSchemaName("agentwharf_timestamp_attempt")
		setupSchema(t, dsn, schemaName)
		t.Cleanup(func() { dropSchema(t, dsn, schemaName) })
		pool := openPool(t, dsn, schemaName, nil)
		t.Cleanup(pool.Close)
		resetSchema(t, pool)
		if _, err := pool.Exec(context.Background(), `INSERT INTO agent_sessions (id) VALUES ('ses_timestamp_bootstrap'), ('ses_timestamp_target')`); err != nil {
			t.Fatalf("seed attach attempt sessions: %v", err)
		}

		expiresAt := offsetNanosecondTimestamp(2 * time.Minute)
		generation := int64(1)
		request := store.AttachAttemptRequest{
			Identity: store.AttachAttemptIdentity{
				JTIHash: [32]byte{1}, AttachID: "att_timestamp", BootstrapSessionID: "ses_timestamp_bootstrap",
				TargetSessionID: "ses_timestamp_target", Provider: "claude-code",
			},
			Fingerprint: store.AttachAttemptFingerprint{
				Domain: "agentwharf.attach-request.v1", Version: 1, Digest: [32]byte{2}, KeyVersion: 1,
			},
			ExpiresAt: expiresAt, Outcome: store.AttachAttemptAccepted, IssuedCredentialGeneration: &generation,
		}
		attempts := postgres.New(pool)
		first, err := attempts.CommitAttachAttempt(context.Background(), request)
		if err != nil || first.Duplicate {
			t.Fatalf("first CommitAttachAttempt() = %+v, %v", first, err)
		}
		retry, err := attempts.CommitAttachAttempt(context.Background(), request)
		if err != nil || !retry.Duplicate {
			t.Fatalf("retry CommitAttachAttempt() = %+v, %v; want duplicate", retry, err)
		}
		assertDurableTimestamp(t, "attach attempt", first.Attempt.ExpiresAt, expiresAt)
		assertDurableTimestamp(t, "attach attempt retry", retry.Attempt.ExpiresAt, expiresAt)
		if request.ExpiresAt != expiresAt {
			t.Fatal("CommitAttachAttempt() mutated the caller timestamp")
		}
	})

	t.Run("warm attach", func(t *testing.T) {
		_, warm := newWarmAttachStore(t)
		request := warmAttachRequestForPostgres()
		attemptExpiry := offsetNanosecondTimestamp(2 * time.Minute)
		deliveryExpiry := offsetNanosecondTimestamp(20 * time.Second)
		request.Attempt.ExpiresAt = attemptExpiry
		request.Attachment.ExpiresAt = deliveryExpiry
		request.TargetActivation.ExpiresAt = deliveryExpiry
		request.FirstDelivery.ExpiresAt = deliveryExpiry

		first, err := warm.CommitWarmAttach(context.Background(), request)
		if err != nil || first.Duplicate {
			t.Fatalf("first CommitWarmAttach() = %+v, %v", first, err)
		}
		retry, err := warm.CommitWarmAttach(context.Background(), request)
		if err != nil || !retry.Duplicate {
			t.Fatalf("retry CommitWarmAttach() = %+v, %v; want duplicate", retry, err)
		}
		assertDurableTimestamp(t, "warm attempt", first.Attempt.ExpiresAt, attemptExpiry)
		if first.Attachment.ExpiresAt == nil {
			t.Fatal("warm attachment expiry is missing")
		}
		assertDurableTimestamp(t, "warm attachment", *first.Attachment.ExpiresAt, deliveryExpiry)
		assertDurableTimestamp(t, "warm activation", first.TargetActivation.ExpiresAt, deliveryExpiry)
		assertDurableTimestamp(t, "warm outbox", first.Outbox.ExpiresAt, deliveryExpiry)
		if request.Attempt.ExpiresAt != attemptExpiry || request.Attachment.ExpiresAt != deliveryExpiry ||
			request.TargetActivation.ExpiresAt != deliveryExpiry || request.FirstDelivery.ExpiresAt != deliveryExpiry {
			t.Fatal("CommitWarmAttach() mutated caller timestamps")
		}
	})

	t.Run("workspace lease child scope", func(t *testing.T) {
		harness := newPostgresWorkspaceLeaseHarness(t)
		leaseExpiry := offsetNanosecondTimestamp(20 * time.Second)
		childExpiry := offsetNanosecondTimestamp(15 * time.Second)
		child := &store.WorkspaceLeaseChildScope{
			ParentKey: store.WorkspaceLeaseKey{41}, CapabilityDigest: [32]byte{42}, ExpiresAt: childExpiry,
		}
		reserve := store.WorkspaceLeaseReserve{
			Key: store.WorkspaceLeaseKey{43}, ChildScope: child,
			Owner: store.WorkspaceLeaseOwner{
				WorkerID: "worker_timestamp", SessionID: "ses_workspace", ConnectionEpoch: 1,
				CredentialGeneration: 1, LeaseID: "lease_timestamp",
			},
			ExpiresAt: leaseExpiry,
		}
		first, err := harness.ReserveWorkspaceLease(context.Background(), reserve)
		if err != nil {
			t.Fatalf("first ReserveWorkspaceLease() = %v", err)
		}
		retry, err := harness.ReserveWorkspaceLease(context.Background(), reserve)
		if err != nil || retry.Version != first.Version {
			t.Fatalf("retry ReserveWorkspaceLease() = %+v, %v; want version %d", retry, err, first.Version)
		}
		assertDurableTimestamp(t, "workspace lease", first.ExpiresAt, leaseExpiry)
		if first.ChildScope == nil {
			t.Fatal("workspace child scope is missing")
		}
		assertDurableTimestamp(t, "workspace child scope", first.ChildScope.ExpiresAt, childExpiry)
		if reserve.ChildScope != child || child.ExpiresAt != childExpiry || reserve.ExpiresAt != leaseExpiry {
			t.Fatal("ReserveWorkspaceLease() mutated caller-owned scope")
		}
	})

	t.Run("attachment create and update", func(t *testing.T) {
		harness := newPostgresAttachmentHarness(t)
		createExpiry := offsetNanosecondTimestamp(20 * time.Second)
		request := store.AttachmentCreate{
			Identity: store.AttachmentIdentity{
				AttachID: "attach_timestamp", BootstrapSessionID: "ses_bootstrap_mutation",
				TargetSessionID: "ses_target_mutation", TargetCredentialLineageRef: "lineage_timestamp",
			},
			ExpiresAt: createExpiry,
		}
		first, err := harness.CreateAttachment(context.Background(), request)
		if err != nil || first.Noop {
			t.Fatalf("first CreateAttachment() = %+v, %v", first, err)
		}
		retry, err := harness.CreateAttachment(context.Background(), request)
		if err != nil || !retry.Noop {
			t.Fatalf("retry CreateAttachment() = %+v, %v; want no-op", retry, err)
		}
		if first.Attachment.ExpiresAt == nil {
			t.Fatal("created attachment expiry is missing")
		}
		assertDurableTimestamp(t, "attachment create", *first.Attachment.ExpiresAt, createExpiry)
		if request.ExpiresAt != createExpiry {
			t.Fatal("CreateAttachment() mutated the caller timestamp")
		}

		updateExpiry := offsetNanosecondTimestamp(15 * time.Second)
		blockerExpiry := updateExpiry
		reason := "workspace_busy"
		blockingSessionID := "ses_blocker_mutation"
		updateExpiryPointer := &updateExpiry
		blockerExpiryPointer := &blockerExpiry
		blocker := &store.AttachmentBlocker{
			Kind: store.AttachmentBlockerQueued, Reason: &reason, ExpiresAt: blockerExpiryPointer,
			BlockingSessionID: &blockingSessionID,
		}
		update := store.AttachmentUpdate{
			Status: store.AttachmentQueued, DeliveryState: store.AttachmentDeliveryPending,
			QueueReason: &reason, ExpiresAt: updateExpiryPointer, BlockingSessionID: &blockingSessionID, Blocker: blocker,
		}
		mutation, err := harness.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, update)
		if err != nil {
			t.Fatalf("UpdateAttachment() = %v", err)
		}
		if mutation.Attachment.ExpiresAt == nil || mutation.Summary.Blocker == nil || mutation.Summary.Blocker.ExpiresAt == nil {
			t.Fatalf("attachment mutation expiries are missing: %+v", mutation)
		}
		assertDurableTimestamp(t, "attachment update", *mutation.Attachment.ExpiresAt, updateExpiry)
		assertDurableTimestamp(t, "attachment blocker", *mutation.Summary.Blocker.ExpiresAt, blockerExpiry)
		if update.ExpiresAt != updateExpiryPointer || *update.ExpiresAt != updateExpiry || update.Blocker != blocker ||
			update.Blocker.ExpiresAt != blockerExpiryPointer || *update.Blocker.ExpiresAt != blockerExpiry {
			t.Fatal("UpdateAttachment() mutated caller-owned pointers")
		}

		if _, err := harness.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, update); err == nil {
			t.Fatal("stale attachment update unexpectedly succeeded")
		}
		stored, err := harness.Attachment(context.Background(), request.Identity.AttachID)
		if err != nil || stored.DeliveryVersion != 1 || stored.ExpiresAt == nil {
			t.Fatalf("attachment after stale retry = %+v, %v", stored, err)
		}
		assertDurableTimestamp(t, "attachment after stale retry", *stored.ExpiresAt, updateExpiry)
	})
}

func testSchemaName(prefix string) string {
	return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixNano(), schemaSeq.Add(1))
}

func offsetNanosecondTimestamp(after time.Duration) time.Time {
	instant := time.Now().UTC().Add(after).Truncate(time.Microsecond).Add(789 * time.Nanosecond)
	return instant.In(time.FixedZone("test-offset", 5*60*60+30*60))
}

func assertDurableTimestamp(t *testing.T, name string, got, input time.Time) {
	t.Helper()
	want := input.UTC().Truncate(time.Microsecond)
	if !got.UTC().Truncate(time.Microsecond).Equal(want) || got.Nanosecond()%1000 != 0 {
		t.Fatalf("%s timestamp = %v; want %v at microsecond precision", name, got, want)
	}
}
