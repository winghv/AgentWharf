package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
)

// encryptedEventPayloadBudget leaves headroom for SealPacket's
// `{"public":{...},"payload":...}` wrapper inside e2ee.MaxContentBytes (32 KiB).
const encryptedEventPayloadBudget = e2ee.MaxContentBytes - 1024

func eventPayloadBudget(cfg wrapConfig) int {
	if cfg.ContentMode == protocol.ContentModeRequired {
		return encryptedEventPayloadBudget
	}
	return protocol.MaxEventPayloadBytes
}

// boundEncryptedEventPayload guarantees that post-masking expansion or rare
// large metadata frames cannot exceed the 32 KiB E2EE plaintext ceiling and
// fail SealEventWire after consuming a durable seal slot. It never mutates the
// caller's slice and preserves all public routing projections.
func boundEncryptedEventPayload(eventType string, payload json.RawMessage) json.RawMessage {
	if len(payload) <= encryptedEventPayloadBudget {
		return payload
	}
	var fields map[string]any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&fields); err != nil || fields == nil {
		return payload
	}
	encode := func(value map[string]any) (json.RawMessage, bool) {
		encoded, err := json.Marshal(value)
		if err != nil {
			return payload, false
		}
		return encoded, len(encoded) <= encryptedEventPayloadBudget
	}

	switch eventType {
	case "session.tool_call":
		delete(fields, "diffs")
		if encoded, ok := encode(fields); ok {
			return encoded
		}
		if result, ok := fields["result"].(map[string]any); ok {
			preview, _ := result["output_preview"].(string)
			for preview != "" {
				preview = preview[:transcriptUTF8PrefixEnd(preview, len(preview)/2)]
				result["output_preview"] = preview
				result["truncated"] = true
				if encoded, ok := encode(fields); ok {
					return encoded
				}
			}
		}
		if fields["input"] != nil {
			fields["input"] = map[string]any{"_truncated": true}
		}
		if title, ok := fields["title"].(string); ok && len(title) > 256 {
			fields["title"] = title[:transcriptUTF8PrefixEnd(title, 256)]
		}
		if encoded, err := json.Marshal(fields); err == nil {
			return encoded
		}
	case "session.message":
		if items, ok := fields["content"].([]any); ok {
			for index, item := range items {
				block, ok := item.(map[string]any)
				if !ok {
					continue
				}
				text, _ := block["text"].(string)
				for text != "" {
					text = text[:transcriptUTF8PrefixEnd(text, len(text)/2)]
					block["text"] = text
					items[index] = block
					if encoded, fits := encode(fields); fits {
						return encoded
					}
				}
			}
		}
	case "agent.activity":
		for _, textKey := range []string{"text", "markdown"} {
			text, _ := fields[textKey].(string)
			for text != "" {
				text = text[:transcriptUTF8PrefixEnd(text, len(text)/2)]
				fields[textKey] = text
				if textKey == "markdown" {
					fields["truncated"] = true
				}
				if encoded, fits := encode(fields); fits {
					return encoded
				}
			}
		}
		minimal := map[string]any{"truncated": true}
		for _, key := range []string{"kind", "provider_session_id", "phase", "plan_id", "warning_id", "category"} {
			if value, ok := fields[key]; ok {
				minimal[key] = value
			}
		}
		if encoded, err := json.Marshal(minimal); err == nil {
			return encoded
		}
	case "session.state":
		fields["metadata"] = map[string]any{"truncated": true}
		if encoded, err := json.Marshal(fields); err == nil {
			return encoded
		}
	case "permission.request":
		if summary, ok := fields["summary"].(string); ok && len(summary) > 1024 {
			fields["summary"] = summary[:transcriptUTF8PrefixEnd(summary, 1024)]
		}
		if encoded, ok := encode(fields); ok {
			return encoded
		}
		fields["detail"] = map[string]any{"truncated": true}
		if encoded, err := json.Marshal(fields); err == nil {
			return encoded
		}
	case "session.commands":
		if commands, ok := fields["commands"].([]any); ok {
			for len(commands) > 0 {
				commands = commands[:len(commands)-1]
				fields["commands"] = commands
				if encoded, fits := encode(fields); fits {
					return encoded
				}
			}
		}
	}
	return payload
}

// A single runtime owns the endpoint execution lane. Opening storage grants no
// session access: authenticated local membership initialization is separate.
type machineE2EERuntime struct {
	database *sql.DB
	registry *e2ee.DeviceRegistry
	vault    *e2ee.SessionKeyVault
	executor *e2ee.CommandExecutor
	public   e2ee.PairingIdentity
}

// encryptedEpochStale reports whether a durable command admission failed
// because the sender sealed under a stale key epoch. The wire ack then carries
// the content-free reason "epoch_stale" so an encrypted terminal can refresh
// its epoch and resend; every other failure keeps the existing behavior.
func encryptedEpochStale(err error) bool {
	return errors.Is(err, e2ee.ErrEpochStale)
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
// The durable per-key seal budget never resets within one epoch, so the sealer
// rekeys proactively at the rotation threshold and once more on a hard
// capacity or stale-epoch failure. Without this, a long-running session hits
// the bound and wedges its adapter in a permanent restart loop.
func (r *machineE2EERuntime) sealEvent(ctx context.Context, session, messageID, eventType string, payload json.RawMessage) (json.RawMessage, error) {
	if r == nil || r.vault == nil {
		return nil, errors.New("encrypted event runtime unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	journal, err := e2ee.NewCommandJournal(ctx, r.database)
	if err != nil {
		return nil, errors.New("local encrypted session unavailable")
	}
	currentKeyID := func() string {
		var keyID string
		if err := r.database.QueryRowContext(ctx, `SELECT key_id FROM e2ee_local_sessions WHERE session=?`, session).Scan(&keyID); err != nil {
			return ""
		}
		return keyID
	}
	keyID := currentKeyID()
	if keyID == "" {
		return nil, errors.New("local encrypted session unavailable")
	}
	if used, budgetErr := r.vault.SealsUsed(ctx, session); budgetErr == nil && used >= e2ee.SealRotationThreshold {
		// A conflict means a concurrent seal already rotated; either way the
		// next read picks up the winning epoch.
		_, _ = r.vault.RotateSessionKeysForBudget(ctx, journal, session)
		if keyID = currentKeyID(); keyID == "" {
			return nil, errors.New("local encrypted session unavailable")
		}
	}
	event, sealErr := r.vault.SealEventWire(ctx, e2ee.Context{Scope: "event", Session: session, KeyID: keyID, Sender: r.public.Device, MessageID: messageID, Type: eventType}, payload)
	if sealErr != nil && (errors.Is(sealErr, e2ee.ErrCapacity) || errors.Is(sealErr, e2ee.ErrUnauthorized)) {
		if _, rotErr := r.vault.RotateSessionKeysForBudget(ctx, journal, session); rotErr != nil && !errors.Is(rotErr, e2ee.ErrConflict) {
			return nil, sealErr
		}
		if keyID = currentKeyID(); keyID == "" {
			return nil, errors.New("local encrypted session unavailable")
		}
		event, sealErr = r.vault.SealEventWire(ctx, e2ee.Context{Scope: "event", Session: session, KeyID: keyID, Sender: r.public.Device, MessageID: messageID, Type: eventType}, payload)
	}
	if sealErr != nil {
		return nil, sealErr
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
