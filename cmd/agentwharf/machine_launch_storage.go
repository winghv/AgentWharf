package main

import (
	"context"
	"errors"
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
	_, err = r.database.ExecContext(ctx, `INSERT INTO e2ee_local_launches(session,provider,carrier) VALUES(?,?,?) ON CONFLICT(session) DO NOTHING`, session, provider, wire)
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
