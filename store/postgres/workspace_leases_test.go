package postgres_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/postgres"
	"github.com/winghv/agentwharf/store/storetest"
)

func TestWorkspaceLeaseStoreContract(t *testing.T) {
	storetest.WorkspaceLeaseContract(t, storetest.WorkspaceLeaseHarness{
		Open: func(t *testing.T) store.WorkspaceLeaseStore {
			return newPostgresWorkspaceLeaseHarness(t)
		},
		Reopen: func(t *testing.T, current store.WorkspaceLeaseStore) store.WorkspaceLeaseStore {
			harness := current.(*postgresWorkspaceLeaseHarness)
			harness.pool.Close()
			harness.reopen(t)
			return harness
		},
		Invalidate: func(t *testing.T, current store.WorkspaceLeaseStore, _ store.WorkspaceLeaseKey, _ store.WorkspaceLeaseOwner, kind storetest.WorkspaceLeaseAuthorityFailure) {
			harness := current.(*postgresWorkspaceLeaseHarness)
			statement := map[storetest.WorkspaceLeaseAuthorityFailure]string{
				storetest.WorkspaceLeaseAuthoritySuperseded: "UPDATE session_adapter_connections SET connection_epoch = 2",
				storetest.WorkspaceLeaseAuthorityRevoked:    "UPDATE session_adapter_connections SET revoked_at = clock_timestamp()",
				storetest.WorkspaceLeaseAuthorityExpired:    "UPDATE session_adapter_connections SET created_at = clock_timestamp() - interval '2 minutes', active_credential_expires_at = clock_timestamp() - interval '1 second'",
				storetest.WorkspaceLeaseAuthorityTerminal:   "UPDATE session_adapter_connections SET terminal_at = clock_timestamp()",
				storetest.WorkspaceLeaseAttachmentExpired:   "UPDATE session_attachments SET expires_at = clock_timestamp()",
				storetest.WorkspaceLeaseAttachmentCanceled: `UPDATE session_attachments
SET status = 'canceled', queue_reason = NULL, expires_at = NULL,
    canceled_at = clock_timestamp(), blocking_session_id = NULL`,
			}[kind]
			if statement == "" {
				t.Fatalf("unknown workspace lease authority failure %q", kind)
			}
			if _, err := harness.pool.Exec(context.Background(), statement); err != nil {
				t.Fatalf("invalidate workspace authority %s: %v", kind, err)
			}
		},
	})
}

func TestWorkspaceLeaseChildScopeSurvivesReopen(t *testing.T) {
	harness := newPostgresWorkspaceLeaseHarness(t)
	reserve := store.WorkspaceLeaseReserve{
		Key: store.WorkspaceLeaseKey{2},
		ChildScope: &store.WorkspaceLeaseChildScope{
			ParentKey: store.WorkspaceLeaseKey{1}, CapabilityDigest: [32]byte{3}, ExpiresAt: time.Now().Add(time.Minute),
		},
		Owner: store.WorkspaceLeaseOwner{
			WorkerID: "worker_child", SessionID: "ses_workspace", ConnectionEpoch: 1,
			CredentialGeneration: 1, LeaseID: "lease_child",
		},
		ExpiresAt: time.Now().Add(time.Minute),
	}
	reserved, err := harness.ReserveWorkspaceLease(context.Background(), reserve)
	if err != nil {
		t.Fatalf("reserve child workspace lease: %v", err)
	}
	harness.pool.Close()
	harness.reopen(t)
	reopened, err := harness.WorkspaceLease(context.Background(), reserve.Key)
	if err != nil {
		t.Fatalf("read reopened child workspace lease: %v", err)
	}
	if !reflect.DeepEqual(reopened, reserved) {
		t.Fatalf("reopened child workspace lease = %+v, want %+v", reopened, reserved)
	}
}

func TestWorkspaceLeaseExpiredReservationCanQuarantineOrRelease(t *testing.T) {
	for name, transition := range map[string]func(context.Context, *postgresWorkspaceLeaseHarness, store.WorkspaceLeaseKey, int64, store.WorkspaceLeaseOwner) error{
		"quarantine": func(ctx context.Context, harness *postgresWorkspaceLeaseHarness, key store.WorkspaceLeaseKey, version int64, _ store.WorkspaceLeaseOwner) error {
			_, err := harness.QuarantineWorkspaceLease(ctx, key, version)
			return err
		},
		"release": func(ctx context.Context, harness *postgresWorkspaceLeaseHarness, key store.WorkspaceLeaseKey, version int64, owner store.WorkspaceLeaseOwner) error {
			_, err := harness.ReleaseWorkspaceLeaseAfterQuiescence(ctx, key, version, owner)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			harness := newPostgresWorkspaceLeaseHarness(t)
			reserve := store.WorkspaceLeaseReserve{
				Key: store.WorkspaceLeaseKey{9}, Owner: store.WorkspaceLeaseOwner{
					WorkerID: "worker_expired", SessionID: "ses_workspace", ConnectionEpoch: 1,
					CredentialGeneration: 1, LeaseID: "lease_expired",
				},
				ExpiresAt: time.Now().Add(20 * time.Millisecond),
			}
			lease, err := harness.ReserveWorkspaceLease(context.Background(), reserve)
			if err != nil {
				t.Fatalf("reserve expiring workspace lease: %v", err)
			}
			time.Sleep(50 * time.Millisecond)
			if err := transition(context.Background(), harness, reserve.Key, lease.Version, reserve.Owner); err != nil {
				t.Fatalf("%s expired workspace lease: %v", name, err)
			}
		})
	}
}

type postgresWorkspaceLeaseHarness struct {
	*postgres.Store
	pool       *pgxpool.Pool
	dsn        string
	schemaName string
}

func newPostgresWorkspaceLeaseHarness(t *testing.T) *postgresWorkspaceLeaseHarness {
	t.Helper()
	dsn := testDSN(t)
	schemaName := fmt.Sprintf("agentwharf_workspace_lease_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))
	setupSchema(t, dsn, schemaName)
	harness := &postgresWorkspaceLeaseHarness{dsn: dsn, schemaName: schemaName}
	harness.reopen(t)
	resetSchema(t, harness.pool)
	if _, err := harness.pool.Exec(context.Background(), `
INSERT INTO agent_sessions (id) VALUES ('ses_workspace'), ('ses_workspace_blocker');
INSERT INTO session_adapter_connections (
    session_id, connection_epoch, accepted_fence, active_credential_generation,
    credential_generation_high_watermark, active_credential_expires_at
) VALUES ('ses_workspace', 1, 1, 1, 1, clock_timestamp() + interval '1 hour');
INSERT INTO session_attachments (
    attach_id, bootstrap_session_id, target_session_id, status, delivery_state,
    queue_reason, expires_at, blocking_session_id, target_credential_lineage_ref
) VALUES (
    'att_workspace', 'ses_workspace_blocker', 'ses_workspace', 'queued', 'pending',
    'workspace_busy', clock_timestamp() + interval '1 hour', 'ses_workspace_blocker', 'lineage_workspace'
);`); err != nil {
		t.Fatalf("seed workspace lease authority: %v", err)
	}
	t.Cleanup(func() {
		harness.pool.Close()
		dropSchema(t, dsn, schemaName)
	})
	return harness
}

func (h *postgresWorkspaceLeaseHarness) reopen(t *testing.T) {
	t.Helper()
	pool := openPool(t, h.dsn, h.schemaName, nil)
	h.pool = pool
	h.Store = postgres.New(pool)
}
