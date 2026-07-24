package postgres_test

import (
	"context"
	"errors"
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

func TestProviderStartAdmissionContract(t *testing.T) {
	newAdmission := func(t *testing.T, scopes ...*store.WorkspaceLeaseChildScope) (*postgresWorkspaceLeaseHarness, store.ProviderStartAdmission, store.WorkspaceLeaseKey) {
		t.Helper()
		harness := newPostgresWorkspaceLeaseHarness(t)
		var key store.WorkspaceLeaseKey
		key[0] = 73
		owner := store.WorkspaceLeaseOwner{WorkerID: "worker_provider_start", SessionID: "ses_workspace", ConnectionEpoch: 1, CredentialGeneration: 1, LeaseID: "lease_provider_start"}
		var scope *store.WorkspaceLeaseChildScope
		if len(scopes) > 0 {
			scope = scopes[0]
		}
		if _, err := harness.ReserveWorkspaceLease(context.Background(), store.WorkspaceLeaseReserve{Key: key, ChildScope: scope, Owner: owner, ExpiresAt: time.Now().Add(time.Minute)}); err != nil {
			t.Fatalf("ReserveWorkspaceLease() = %v", err)
		}
		var fence int64
		for fence <= 1 {
			var err error
			fence, err = harness.AllocateAdapterGrantFence(context.Background())
			if err != nil {
				t.Fatalf("AllocateAdapterGrantFence() = %v", err)
			}
		}
		return harness, store.ProviderStartAdmission{SessionID: "ses_workspace", Admission: store.AdapterConnectionAdmission{CredentialGeneration: 1, ConnectionEpoch: 1, AcceptedFence: 1, GrantFence: fence}, Writer: store.SettingsWriter{ConnectionEpoch: 1, CredentialGeneration: 1, LeaseID: "lease_provider_start"}}, key
	}

	t.Run("commits exactly one start receipt", func(t *testing.T) {
		harness, admission, key := newAdmission(t)
		lease, err := harness.RecordProviderStartAdmission(context.Background(), admission)
		if err != nil || lease.Key != key || lease.Status != store.WorkspaceLeaseStartReceived {
			t.Fatalf("RecordProviderStartAdmission() = %+v, %v", lease, err)
		}
		if _, err := harness.RecordProviderStartAdmission(context.Background(), admission); err == nil {
			t.Fatal("duplicate provider start was admitted")
		}
	})

	t.Run("re-admits only an explicit restarted child", func(t *testing.T) {
		harness, admission, key := newAdmission(t)
		if _, err := harness.RecordProviderStartAdmission(context.Background(), admission); err != nil {
			t.Fatalf("RecordProviderStartAdmission() = %v", err)
		}
		if _, err := harness.WithProviderStartAdmission(context.Background(), admission, func(context.Context) error { return nil }); err == nil {
			t.Fatal("implicit provider restart was admitted")
		}
		admission.ReAdmission = true
		called := false
		lease, err := harness.WithProviderStartAdmission(context.Background(), admission, func(context.Context) error {
			called = true
			return nil
		})
		if err != nil || !called || lease.Key != key || lease.Status != store.WorkspaceLeaseStartReceived {
			t.Fatalf("explicit provider restart = %+v, called=%t, err=%v", lease, called, err)
		}
	})

	t.Run("rejects an expired child scope during re-admission", func(t *testing.T) {
		scope := &store.WorkspaceLeaseChildScope{ParentKey: store.WorkspaceLeaseKey{72}, CapabilityDigest: [32]byte{73}, ExpiresAt: time.Now().Add(time.Minute)}
		harness, admission, key := newAdmission(t, scope)
		if _, err := harness.RecordProviderStartAdmission(context.Background(), admission); err != nil {
			t.Fatalf("RecordProviderStartAdmission() = %v", err)
		}
		if _, err := harness.pool.Exec(context.Background(), "UPDATE session_workspace_leases SET child_scope_expires_at=clock_timestamp() - interval '1 second'"); err != nil {
			t.Fatalf("expire child scope: %v", err)
		}
		admission.ReAdmission = true
		called := false
		if _, err := harness.WithProviderStartAdmission(context.Background(), admission, func(context.Context) error {
			called = true
			return nil
		}); err == nil || called {
			t.Fatalf("expired child scope re-admission err=%v, callback=%t", err, called)
		}
		lease, err := harness.WorkspaceLease(context.Background(), key)
		if err != nil || lease.Status != store.WorkspaceLeaseStartReceived {
			t.Fatalf("expired child scope lease = %+v, %v", lease, err)
		}
	})

	t.Run("rejects an expired reservation during re-admission", func(t *testing.T) {
		harness, admission, key := newAdmission(t)
		if _, err := harness.RecordProviderStartAdmission(context.Background(), admission); err != nil {
			t.Fatalf("RecordProviderStartAdmission() = %v", err)
		}
		if _, err := harness.pool.Exec(context.Background(), "UPDATE session_workspace_leases SET reservation_expires_at=clock_timestamp() - interval '1 second'"); err != nil {
			t.Fatalf("expire reservation: %v", err)
		}
		admission.ReAdmission = true
		called := false
		if _, err := harness.WithProviderStartAdmission(context.Background(), admission, func(context.Context) error {
			called = true
			return nil
		}); err == nil || called {
			t.Fatalf("expired reservation re-admission err=%v, callback=%t", err, called)
		}
		lease, err := harness.WorkspaceLease(context.Background(), key)
		if err != nil || lease.Status != store.WorkspaceLeaseStartReceived {
			t.Fatalf("expired reservation lease = %+v, %v", lease, err)
		}
	})

	for _, expiry := range []struct {
		name      string
		scope     *store.WorkspaceLeaseChildScope
		statement string
	}{
		{"reservation", nil, "UPDATE session_workspace_leases SET reservation_expires_at=clock_timestamp() + interval '20 milliseconds'"},
		{"child scope", &store.WorkspaceLeaseChildScope{ParentKey: store.WorkspaceLeaseKey{72}, CapabilityDigest: [32]byte{74}, ExpiresAt: time.Now().Add(time.Minute)}, "UPDATE session_workspace_leases SET child_scope_expires_at=clock_timestamp() + interval '20 milliseconds'"},
	} {
		t.Run("rejects "+expiry.name+" that expires during re-admission callback", func(t *testing.T) {
			harness, admission, key := newAdmission(t, expiry.scope)
			if _, err := harness.RecordProviderStartAdmission(context.Background(), admission); err != nil {
				t.Fatalf("RecordProviderStartAdmission() = %v", err)
			}
			if _, err := harness.pool.Exec(context.Background(), expiry.statement); err != nil {
				t.Fatalf("set callback expiry: %v", err)
			}
			admission.ReAdmission = true
			called := false
			if _, err := harness.WithProviderStartAdmission(context.Background(), admission, func(context.Context) error {
				called = true
				time.Sleep(50 * time.Millisecond)
				return nil
			}); err == nil || !called {
				t.Fatalf("expired %s re-admission err=%v, callback=%t", expiry.name, err, called)
			}
			lease, err := harness.WorkspaceLease(context.Background(), key)
			if err != nil || lease.Status != store.WorkspaceLeaseStartReceived {
				t.Fatalf("expired %s lease = %+v, %v", expiry.name, lease, err)
			}
		})
	}

	t.Run("callback failure leaves the lease reserved", func(t *testing.T) {
		harness, admission, key := newAdmission(t)
		if _, err := harness.WithProviderStartAdmission(context.Background(), admission, func(context.Context) error {
			return errors.New("provider did not start")
		}); err == nil {
			t.Fatal("failed provider start callback was admitted")
		}
		lease, err := harness.WorkspaceLease(context.Background(), key)
		if err != nil || lease.Status != store.WorkspaceLeaseReserved {
			t.Fatalf("failed callback lease = %+v, %v", lease, err)
		}
	})

	for _, invalidation := range []struct {
		name      string
		statement string
	}{
		{"replacement", "UPDATE session_adapter_connections SET connection_epoch=2, accepted_fence=2"},
		{"revocation", "UPDATE session_adapter_connections SET revoked_at=clock_timestamp()"},
		{"terminal", "UPDATE session_adapter_connections SET terminal_at=clock_timestamp()"},
		{"quarantine", "UPDATE session_workspace_leases SET status='quarantined', version=version+1, expires_at=NULL, quarantine_reason='authority_superseded', recovery_state='pending'"},
	} {
		t.Run(invalidation.name, func(t *testing.T) {
			harness, admission, key := newAdmission(t)
			if _, err := harness.pool.Exec(context.Background(), invalidation.statement); err != nil {
				t.Fatalf("invalidate provider start %s: %v", invalidation.name, err)
			}
			if _, err := harness.RecordProviderStartAdmission(context.Background(), admission); err == nil {
				t.Fatal("invalidated provider start was admitted")
			}
			lease, err := harness.WorkspaceLease(context.Background(), key)
			if err != nil || lease.Status == store.WorkspaceLeaseStartReceived {
				t.Fatalf("invalidated lease = %+v, %v", lease, err)
			}
		})
	}
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

func TestWorkspaceLeaseExpiredReleaseAllowsReplacement(t *testing.T) {
	harness := newPostgresWorkspaceLeaseHarness(t)
	reserve := store.WorkspaceLeaseReserve{
		Key: store.WorkspaceLeaseKey{10}, Owner: store.WorkspaceLeaseOwner{
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
	if _, err := harness.ReleaseWorkspaceLeaseAfterQuiescence(context.Background(), reserve.Key, lease.Version, reserve.Owner); err != nil {
		t.Fatalf("release expired workspace lease: %v", err)
	}
	replacement := reserve
	replacement.Owner = store.WorkspaceLeaseOwner{
		WorkerID: "worker_replacement", SessionID: "ses_workspace", ConnectionEpoch: 2,
		CredentialGeneration: 2, LeaseID: "lease_replacement",
	}
	replacement.ExpiresAt = time.Now().Add(time.Minute)
	if _, err := harness.ReserveWorkspaceLease(context.Background(), replacement); err != nil {
		t.Fatalf("reserve released workspace replacement: %v", err)
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
