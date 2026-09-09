package main

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
)

// pollEncryptedPairing uses an invitation and registry constructed by the local
// endpoint, not by the relay. No offer secret or private identity leaves here.
func pollEncryptedPairing(ctx context.Context, client *http.Client, credential machineCredential, invitationID string, expires time.Time, invitation *e2ee.PairingInvitation, registry *e2ee.DeviceRegistry) error {
	if invitation == nil || registry == nil || !expires.After(time.Now()) {
		return errors.New("local pairing unavailable")
	}
	ctx, cancel := context.WithDeadline(ctx, expires)
	defer cancel()
	endpoint, err := cloudAPIEndpoint(credential.CloudAPIURL, "/machines/"+url.PathEscape(credential.MachineID)+"/e2ee-pairing/"+url.PathEscape(invitationID)+"/endpoint")
	if err != nil {
		return errors.New("pairing relay unavailable")
	}
	for {
		status, body, err := getCloudAPIJSON(ctx, client, endpoint, credential.MachineToken)
		if err != nil {
			return errors.New("pairing relay request failed")
		}
		if status == http.StatusNotFound {
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if status != http.StatusOK {
			return errors.New("pairing relay unavailable")
		}
		var response struct {
			Data struct {
				Request string `json:"request"`
			}
		}
		if decodeCloudAPIJSON(body, &response) != nil {
			return errors.New("invalid pairing relay response")
		}
		request, err := protocol.DecodeEncryptedPairingRequest([]byte(response.Data.Request))
		if err != nil {
			return err
		}
		receipt, err := registry.Enroll(ctx, invitation, e2ee.WrappedKey{Enc: request.Enc, Ciphertext: request.Ciphertext}, time.Now())
		if err != nil {
			return errors.New("endpoint pairing rejected")
		}
		payload := struct {
			Request      string `json:"request"`
			Confirmation string `json:"confirmation"`
		}{response.Data.Request, receipt.Confirmation}
		// Retry only the committed receipt, never re-fetch a potentially changed
		// enrollment request. The invitation deadline bounds all attempts.
		for {
			status, _, err = postCloudAPIJSON(ctx, client, endpoint, credential.MachineToken, payload)
			if err == nil && status == http.StatusNoContent {
				return nil
			}
			if err == nil && status < http.StatusInternalServerError {
				return errors.New("pairing receipt delivery rejected")
			}
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}
