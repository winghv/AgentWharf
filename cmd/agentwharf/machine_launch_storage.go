package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/winghv/agentwharf/protocol"
)

// The retained signed launch is local recovery evidence, never a platform
// credential. Every use revalidates its signature and current control grant.
func (r *machineE2EERuntime) retainLaunch(ctx context.Context, session, provider, wire string) error {
	id, err := decodeEncryptedLaunchCarrier(wire)
	if err != nil {
		return errors.New("invalid retained launch")
	}
	if err := r.executor.VerifyWire(ctx, session, id, "session.send", []byte(wire)); err != nil {
		return err
	}
	// A same-provider replacement is the endpoint re-sealing its own launch after
	// a session-key rotation; a different provider still conflicts.
	_, err = r.database.ExecContext(ctx, `INSERT INTO e2ee_local_launches(session,provider,carrier) VALUES(?,?,?)
 ON CONFLICT(session) DO UPDATE SET provider=excluded.provider, carrier=excluded.carrier
 WHERE e2ee_local_launches.provider=excluded.provider`, session, provider, wire)
	if err != nil {
		return errors.New("retain encrypted launch failed")
	}
	retainedProvider, retained, err := r.loadLaunch(ctx, session)
	if err != nil || retainedProvider != provider || retained != wire {
		return errors.New("retained launch conflicts with original")
	}
	return nil
}

func (r *machineE2EERuntime) loadLaunch(ctx context.Context, session string) (string, string, error) {
	var provider, wire string
	if err := r.database.QueryRowContext(ctx, `SELECT provider,carrier FROM e2ee_local_launches WHERE session=?`, session).Scan(&provider, &wire); err != nil {
		return "", "", errors.New("local signed launch unavailable")
	}
	return provider, wire, nil
}

// retainLaunchRecovery stores the launch configuration the endpoint validated
// when it first applied the launch. Session content keys rotate when
// account-terminal trust is turned off, which can invalidate the original
// terminal-signed carrier; this endpoint-private copy lets the machine re-seal
// the same launch under its own current control grant instead of losing the
// Session. It is written only after the signed carrier validated, and it holds
// no platform credential and no transcript content.
func (r *machineE2EERuntime) retainLaunchRecovery(ctx context.Context, session, provider string, settings protocol.EncryptedLaunchSettings) error {
	data, err := json.Marshal(settings)
	if err != nil {
		return errors.New("encode retained launch recovery failed")
	}
	_, err = r.database.ExecContext(ctx, `INSERT INTO e2ee_local_launch_recoveries(session,provider,settings) VALUES(?,?,?)
 ON CONFLICT(session) DO UPDATE SET provider=excluded.provider, settings=excluded.settings`, session, provider, string(data))
	if err != nil {
		return errors.New("retain launch recovery failed")
	}
	return nil
}

func (r *machineE2EERuntime) loadLaunchRecovery(ctx context.Context, session string) (string, protocol.EncryptedLaunchSettings, error) {
	var provider, raw string
	err := r.database.QueryRowContext(ctx, `SELECT provider,settings FROM e2ee_local_launch_recoveries WHERE session=?`, session).Scan(&provider, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return "", protocol.EncryptedLaunchSettings{}, errors.New("retained launch recovery unavailable")
	}
	if err != nil {
		return "", protocol.EncryptedLaunchSettings{}, errors.New("retained launch recovery unavailable")
	}
	var settings protocol.EncryptedLaunchSettings
	if err := json.Unmarshal([]byte(raw), &settings); err != nil {
		return "", protocol.EncryptedLaunchSettings{}, errors.New("retained launch recovery corrupt")
	}
	return provider, settings, nil
}

// authorizeMachineLaunch re-seals a validated launch as the endpoint's own
// session.send command. The machine's current control grant authorizes the
// result, so a revoked terminal never keeps launch authority.
func (r *machineE2EERuntime) authorizeMachineLaunch(ctx context.Context, session, provider string, settings protocol.EncryptedLaunchSettings) (string, error) {
	if settings.Provider != provider {
		return "", errors.New("retained launch recovery provider mismatch")
	}
	payload, err := json.Marshal(struct {
		Launch protocol.EncryptedLaunchSettings `json:"launch"`
	}{Launch: settings})
	if err != nil {
		return "", errors.New("encode recovery launch failed")
	}
	wire, err := r.vault.SealCommandAsMachine(ctx, session, "recovery:"+session, payload)
	if err != nil {
		return "", errors.New("seal recovery launch failed")
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return "", errors.New("encode recovery launch carrier failed")
	}
	return string(encoded), nil
}
