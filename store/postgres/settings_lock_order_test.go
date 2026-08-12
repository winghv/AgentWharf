package postgres_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/winghv/agentwharf/store"
)

const (
	settingsLockOrderFingerprint = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	settingsLockOrderSessionID   = "ses_settings_1"
)

type settingsLockOrderTracer struct {
	mu    sync.Mutex
	locks []string
}

func (tracer *settingsLockOrderTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	query := strings.Join(strings.Fields(data.SQL), " ")
	var lock string
	switch {
	case strings.Contains(query, "FROM session_settings_capabilities") && strings.Contains(query, "FOR UPDATE"):
		lock = "capability"
	case strings.Contains(query, "FROM session_settings_commands") && strings.Contains(query, "FOR UPDATE"):
		lock = "command"
	default:
		return ctx
	}
	tracer.mu.Lock()
	tracer.locks = append(tracer.locks, lock)
	tracer.mu.Unlock()
	return ctx
}

func (*settingsLockOrderTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (tracer *settingsLockOrderTracer) reset() {
	tracer.mu.Lock()
	tracer.locks = nil
	tracer.mu.Unlock()
}

func (tracer *settingsLockOrderTracer) assertCapabilityBeforeCommand(t *testing.T) {
	t.Helper()
	tracer.mu.Lock()
	locks := append([]string(nil), tracer.locks...)
	tracer.mu.Unlock()
	if len(locks) < 2 || locks[0] != "capability" || locks[1] != "command" {
		t.Fatalf("settings row-lock order = %v, want capability then command", locks)
	}
}

func TestRecoverSettingsCommandLocksCapabilityBeforeCommand(t *testing.T) {
	tracer := &settingsLockOrderTracer{}
	harness := newPostgresCommandHarness(t, "agentwharf_settings_recover_lock_order", tracer)
	seedDeliveryPendingSettingsCommand(t, harness)
	tracer.reset()

	writer := store.SettingsWriter{ConnectionEpoch: 1, CredentialGeneration: 1, LeaseID: "lease_lock_order"}
	if _, err := harness.RecoverSettingsCommand(context.Background(), settingsLockOrderSessionID, "cmd_lock_order", writer); err == nil || !strings.Contains(err.Error(), "delivery deadline has not elapsed") {
		t.Fatalf("RecoverSettingsCommand() error = %v, want unelapsed delivery deadline", err)
	}
	tracer.assertCapabilityBeforeCommand(t)
}

func TestUnboundFinalizeSettingsCommandLocksCapabilityBeforeCommand(t *testing.T) {
	tracer := &settingsLockOrderTracer{}
	harness := newPostgresCommandHarness(t, "agentwharf_settings_finalize_lock_order", tracer)
	seedPendingSettingsCommand(t, harness)
	tracer.reset()

	reasoning := "medium"
	reason := "operation_timeout"
	writer := store.SettingsWriter{ConnectionEpoch: 1, CredentialGeneration: 1, LeaseID: "lease_lock_order"}
	_, err := harness.FinalizeSettingsCommand(context.Background(), settingsLockOrderSessionID, "cmd_lock_order", store.SettingsCommandFinalize{
		ReservationVersion: 1,
		ExpectedStatus:     store.SettingsCommandPending,
		Outcome:            store.SettingsCommandTimeout,
		ReasonCode:         &reason,
		EffectiveCapability: store.SettingsCapability{
			SessionID:                  settingsLockOrderSessionID,
			EventSeq:                   1,
			Fingerprint:                settingsLockOrderFingerprint,
			EffectiveModelID:           "model_a",
			EffectiveReasoningEffortID: &reasoning,
			EffectivePermissionModeID:  "ask",
			Version:                    1,
			Writer:                     writer,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "elapsed operation deadline") {
		t.Fatalf("FinalizeSettingsCommand() error = %v, want unelapsed operation deadline", err)
	}
	tracer.assertCapabilityBeforeCommand(t)
}

func seedDeliveryPendingSettingsCommand(t *testing.T, harness *postgresCommandHarness) {
	t.Helper()
	if _, err := harness.pool.Exec(context.Background(), settingsLockOrderSeedSQL("delivery_pending", "NULL")); err != nil {
		t.Fatalf("seed delivery-pending settings command: %v", err)
	}
}

func seedPendingSettingsCommand(t *testing.T, harness *postgresCommandHarness) {
	t.Helper()
	if _, err := harness.pool.Exec(context.Background(), settingsLockOrderSeedSQL("pending", "statement_timestamp() + interval '30 seconds'")); err != nil {
		t.Fatalf("seed pending settings command: %v", err)
	}
}

func settingsLockOrderSeedSQL(status, operationDeadline string) string {
	return `
INSERT INTO session_events (session_id, seq, type, payload)
VALUES ('ses_settings_1', 1, 'session.settings.capabilities', '{}'::jsonb);
INSERT INTO session_settings_capabilities (
    session_id, capability_event_seq, fingerprint, effective_model_id,
    effective_reasoning_effort_id, effective_permission_mode_id,
    capability_version, writer_connection_epoch, writer_credential_generation,
    writer_lease_id
) VALUES (
    'ses_settings_1', 1, '` + settingsLockOrderFingerprint + `', 'model_a',
    'medium', 'ask', 1, 1, 1, 'lease_lock_order'
);
INSERT INTO session_settings_commands (
    session_id, cmd_id, request_fingerprint, requested_reasoning_effort_id,
    reservation_version, delivery_deadline, operation_deadline,
    writer_connection_epoch, writer_credential_generation, writer_lease_id,
    reserved_capability_event_seq, reserved_fingerprint,
    reserved_effective_model_id, reserved_effective_reasoning_effort_id,
    reserved_effective_permission_mode_id, status
) VALUES (
    'ses_settings_1', 'cmd_lock_order', '` + settingsLockOrderFingerprint + `', 'high',
    1, statement_timestamp() + interval '4 seconds', ` + operationDeadline + `,
    1, 1, 'lease_lock_order', 1, '` + settingsLockOrderFingerprint + `',
    'model_a', 'medium', 'ask', '` + status + `'
);`
}
