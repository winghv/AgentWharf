package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"

	"github.com/winghv/agentwharf/experimental/e2ee"
)

// Enrollment and daemon startup must address the same deployment/account/machine.
func machineEndpointDirectory(credential machineCredential, account string) (string, error) {
	if account == "" || credential.CloudAPIURL == "" || credential.MachineID == "" {
		return "", errors.New("missing local endpoint binding")
	}
	credentialPath, err := machineCredentialFile()
	if err != nil {
		return "", err
	}
	binding, _ := json.Marshal([]string{credential.CloudAPIURL, account, credential.MachineID})
	digest := sha256.Sum256(binding)
	return filepath.Join(filepath.Dir(credentialPath), "endpoint-"+hex.EncodeToString(digest[:])), nil
}

// The explicit file is a local scan/paste artifact, not a log. Never overwrite
// an existing path. The artifact is removed when enrollment ends or is cancelled.
func runMachineEnrollment(ctx context.Context, args []string) (result error) {
	if len(args) != 2 || args[0] == "" || !filepath.IsAbs(args[1]) {
		return errors.New("usage: wharf pair --enroll <local-account-binding> <absolute-private-offer-file>")
	}
	credential, err := loadMachineCredential()
	if err != nil {
		return err
	}
	directory, err := machineEndpointDirectory(credential, args[0])
	if err != nil {
		return err
	}
	var published os.FileInfo
	defer func() {
		if published == nil {
			return
		}
		current, err := os.Lstat(args[1])
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		if err != nil || !os.SameFile(published, current) {
			result = errors.Join(result, errors.New("pairing offer cleanup refused changed file"))
			return
		}
		if err := os.Remove(args[1]); err != nil {
			result = errors.Join(result, errors.New("pairing offer cleanup failed"))
		}
	}()
	return enrollMachineEndpoint(ctx, &http.Client{}, credential, directory, args[0], func(offer e2ee.MachineOffer) error {
		file, err := os.OpenFile(args[1], os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		published, err = file.Stat()
		if err != nil {
			_ = file.Close()
			return err
		}
		data, err := json.Marshal(offer)
		if err != nil {
			_ = file.Close()
			return err
		}
		defer clear(data)
		if _, err = file.Write(data); err != nil {
			_ = file.Close()
			return err
		}
		if err = file.Sync(); err != nil {
			_ = file.Close()
			return err
		}
		return file.Close()
	})
}
