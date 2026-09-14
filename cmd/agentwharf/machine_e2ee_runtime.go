package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
)

// A single runtime owns the endpoint execution lane. Opening storage grants no
// session access: authenticated local membership initialization is separate.
type machineE2EERuntime struct {
	database *sql.DB
	registry *e2ee.DeviceRegistry
	vault    *e2ee.SessionKeyVault
	executor *e2ee.CommandExecutor
	public   e2ee.PairingIdentity
}

// deliverCommand releases plaintext only inside the durable executor callback.
// The provider callback must finish accepting the command before returning and
// must not retain the temporary payload buffer.
func (r *machineE2EERuntime) deliverCommand(ctx context.Context, session string, command *protocol.Command, provider func(context.Context, *protocol.Command) error) (e2ee.CommandAdmission, error) {
	if r == nil || r.executor == nil || command == nil || provider == nil || command.SessionID != session {
		return e2ee.CommandAdmission{}, errors.New("encrypted endpoint command unavailable")
	}
	return r.executor.ExecuteWire(ctx, session, command.CommandID, string(command.Type), command.Payload, func(ctx context.Context, payload json.RawMessage) error {
		decoded := *command
		decoded.Payload = payload
		return provider(ctx, &decoded)
	})
}

// initializeSession is the raw relay ingress. Routing is compared with the
// signed request before any local key/grant mutation or key distribution.
func (r *machineE2EERuntime) initializeSession(ctx context.Context, session string, data []byte) (e2ee.WrappedKey, error) {
	if r == nil || r.executor == nil || r.registry == nil {
		return e2ee.WrappedKey{}, errors.New("encrypted initialization unavailable")
	}
	request, err := e2ee.DecodeSessionInitialization(data)
	if err != nil || request.Session != session {
		return e2ee.WrappedKey{}, errors.New("invalid encrypted initialization")
	}
	wrapped, err := r.executor.InitializeSession(ctx, r.registry, request)
	if err != nil {
		return e2ee.WrappedKey{}, errors.New("encrypted initialization rejected")
	}
	return wrapped, nil
}

// initializeSessionTrusted is the v2 trusted ingress for a terminal that never
// consumed a local machine offer. The executor, not relay metadata, decides
// whether account-terminal trust enrolls the device.
func (r *machineE2EERuntime) initializeSessionTrusted(ctx context.Context, session string, data []byte, trusted, control bool) (e2ee.WrappedKey, error) {
	if r == nil || r.executor == nil || r.registry == nil {
		return e2ee.WrappedKey{}, errors.New("encrypted initialization unavailable")
	}
	request, err := e2ee.DecodeTrustedSessionKeyRequest(data)
	if err != nil || request.Session != session {
		return e2ee.WrappedKey{}, errors.New("invalid encrypted initialization")
	}
	wrapped, err := r.executor.InitializeSessionTrusted(ctx, r.registry, request, trusted, control)
	if err != nil {
		return e2ee.WrappedKey{}, errors.New("encrypted initialization rejected")
	}
	return wrapped, nil
}

// requireSession verifies local provisioning before a Provider process starts.
// This does not authorize commands; ExecuteWire still authenticates each one.
func (r *machineE2EERuntime) requireSession(ctx context.Context, session string) error {
	if r == nil || r.database == nil || r.vault == nil || session == "" {
		return errors.New("local encrypted session unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var keyID string
	if err := r.database.QueryRowContext(ctx, `SELECT key_id FROM e2ee_local_sessions WHERE session=? AND epoch>0`, session).Scan(&keyID); err != nil {
		return errors.New("local encrypted session unavailable")
	}
	key, err := r.vault.Load(ctx, session, keyID)
	clear(key)
	if err != nil {
		return errors.New("local encrypted session key unavailable")
	}
	return nil
}

// ensureSession verifies the session key and creates an endpoint-owned one when
// no terminal initialized the session. The machine is the endpoint authority, so
// a run it starts itself (a startup smoke or an auto claim) still has a key to
// seal events with; terminals can request it through the key relay afterwards.
func (r *machineE2EERuntime) ensureSession(ctx context.Context, session string) error {
	if err := r.requireSession(ctx, session); err == nil {
		return nil
	}
	journal, err := e2ee.NewCommandJournal(ctx, r.database)
	if err != nil {
		return err
	}
	if _, err := r.vault.InitializeLocalSession(ctx, journal, session); err != nil {
		return err
	}
	return r.requireSession(ctx, session)
}

// sealEvent selects the locally active epoch. Callers retain the resulting
// bytes across proposal retries; the Hub remains the sole seq allocator.
func (r *machineE2EERuntime) sealEvent(ctx context.Context, session, messageID, eventType string, payload json.RawMessage) (json.RawMessage, error) {
	if r == nil || r.vault == nil {
		return nil, errors.New("encrypted event runtime unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var keyID string
	if err := r.database.QueryRowContext(ctx, `SELECT key_id FROM e2ee_local_sessions WHERE session=?`, session).Scan(&keyID); err != nil {
		return nil, errors.New("local encrypted session unavailable")
	}
	event, err := r.vault.SealEventWire(ctx, e2ee.Context{Scope: "event", Session: session, KeyID: keyID, Sender: r.public.Device, MessageID: messageID, Type: eventType}, payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(event)
}

// Bind the entire database, including session-key and command-journal tables.
// Registry scoping alone cannot isolate those session-indexed tables.
func bindMachineE2EEDatabase(ctx context.Context, database *sql.DB, machine, account string) error {
	if machine == "" || account == "" {
		return errors.New("missing local endpoint binding")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS e2ee_endpoint_binding (singleton INTEGER PRIMARY KEY CHECK(singleton=1), machine TEXT NOT NULL, account TEXT NOT NULL)`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO e2ee_endpoint_binding(singleton,machine,account) VALUES(1,?,?) ON CONFLICT(singleton) DO NOTHING`, machine, account); err != nil {
		return err
	}
	var boundMachine, boundAccount string
	if err := tx.QueryRowContext(ctx, `SELECT machine,account FROM e2ee_endpoint_binding WHERE singleton=1`).Scan(&boundMachine, &boundAccount); err != nil {
		return err
	}
	if boundMachine != machine || boundAccount != account {
		return errors.New("local endpoint database binding mismatch")
	}
	return tx.Commit()
}

func openMachineE2EERuntime(ctx context.Context, directory, machine, account string) (*machineE2EERuntime, error) {
	identity, err := e2ee.LoadOrCreateIdentity(ctx, directory)
	if err != nil {
		return nil, err
	}
	defer clear(identity.SigningSeed)
	defer clear(identity.WrappingPrivate)
	database, err := e2ee.OpenLocalDatabase(ctx, directory)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			_ = database.Close()
		}
	}()
	if err := bindMachineE2EEDatabase(ctx, database, machine, account); err != nil {
		return nil, err
	}
	if _, err := database.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS e2ee_local_launches(session TEXT PRIMARY KEY CHECK(length(session) BETWEEN 1 AND 128),provider TEXT NOT NULL CHECK(length(provider) BETWEEN 1 AND 128),carrier TEXT NOT NULL CHECK(length(CAST(carrier AS BLOB)) BETWEEN 1 AND 32768))`); err != nil {
		return nil, errors.New("initialize local launch storage failed")
	}
	if _, err := database.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS e2ee_local_launch_recoveries(session TEXT PRIMARY KEY CHECK(length(session) BETWEEN 1 AND 128),provider TEXT NOT NULL CHECK(length(provider) BETWEEN 1 AND 128),settings TEXT NOT NULL CHECK(length(settings) BETWEEN 1 AND 8192))`); err != nil {
		return nil, errors.New("initialize local launch recovery storage failed")
	}
	registry, err := e2ee.NewDeviceRegistry(ctx, database, machine, account)
	if err != nil {
		return nil, err
	}
	journal, err := e2ee.NewCommandJournal(ctx, database)
	if err != nil {
		return nil, err
	}
	vault, err := e2ee.NewSessionKeyVault(ctx, database, identity)
	if err != nil {
		return nil, err
	}
	executor, err := e2ee.NewCommandExecutor(journal, vault)
	if err != nil {
		return nil, err
	}
	public, err := identity.Public()
	if err != nil {
		return nil, err
	}
	success = true
	return &machineE2EERuntime{database: database, registry: registry, vault: vault, executor: executor, public: public}, nil
}
