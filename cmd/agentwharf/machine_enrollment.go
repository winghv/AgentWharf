package main

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
)

// enrollMachineEndpoint requires a trusted local account binding and private
// directory. displayLocal is an explicit local scan surface, never a logger or
// relay writer. Account bearer authentication alone does not enroll any device.
func enrollMachineEndpoint(ctx context.Context, client *http.Client, credential machineCredential, directory, accountBinding string, displayLocal func(e2ee.MachineOffer) error) error {
	if displayLocal == nil || accountBinding == "" || credential.MachineID == "" {
		return errors.New("local enrollment configuration is incomplete")
	}
	identity, err := e2ee.LoadOrCreateIdentity(ctx, directory)
	if err != nil {
		return err
	}
	defer clear(identity.SigningSeed)
	defer clear(identity.WrappingPrivate)
	database, err := e2ee.OpenLocalDatabase(ctx, directory)
	if err != nil {
		return err
	}
	defer database.Close()
	if err := bindMachineE2EEDatabase(ctx, database, credential.MachineID, accountBinding); err != nil {
		return err
	}
	registry, err := e2ee.NewDeviceRegistry(ctx, database, credential.MachineID, accountBinding)
	if err != nil {
		return err
	}
	invitation, offer, err := e2ee.NewMachineOffer(credential.MachineID, identity, time.Now())
	if err != nil {
		return err
	}
	if err := displayLocal(offer); err != nil {
		return errors.New("local pairing display failed")
	}
	return pollEncryptedPairing(ctx, client, credential, offer.Offer.ID, time.UnixMilli(offer.Offer.ExpiresAt), invitation, registry)
}
