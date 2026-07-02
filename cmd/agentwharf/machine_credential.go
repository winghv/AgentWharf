package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var errMachineCredentialNotFound = errors.New("machine credential not found")

type machineCredential struct {
	MachineID    string `json:"machine_id"`
	MachineToken string `json:"machine_token"`
	CloudAPIURL  string `json:"cloud_api_url"`
	HubWSURL     string `json:"hub_ws_url,omitempty"`
	ExpiresAt    string `json:"expires_at,omitempty"`
	CreatedAt    string `json:"created_at"`
}

func loadMachineCredential() (machineCredential, error) {
	path, err := machineCredentialFile()
	if err != nil {
		return machineCredential{}, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return machineCredential{}, errMachineCredentialNotFound
		}
		return machineCredential{}, fmt.Errorf("read machine credential: %w", err)
	}
	var credential machineCredential
	if err := json.Unmarshal(data, &credential); err != nil {
		return machineCredential{}, fmt.Errorf("decode machine credential: %w", err)
	}
	credential.CloudAPIURL = strings.TrimSpace(credential.CloudAPIURL)
	if credential.MachineID == "" || credential.MachineToken == "" || credential.CloudAPIURL == "" {
		return machineCredential{}, errors.New("machine credential is missing required fields")
	}
	return credential, nil
}

func saveMachineCredential(credential machineCredential) error {
	credential.CloudAPIURL = strings.TrimSpace(credential.CloudAPIURL)
	if credential.MachineID == "" || credential.MachineToken == "" || credential.CloudAPIURL == "" {
		return errors.New("machine credential is missing required fields")
	}
	if credential.CreatedAt == "" {
		credential.CreatedAt = time.Now().UTC().Format(time.RFC3339)
	}
	path, err := machineCredentialFile()
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create machine credential directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("secure machine credential directory: %w", err)
	}
	data, err := json.MarshalIndent(credential, "", "  ")
	if err != nil {
		return fmt.Errorf("encode machine credential: %w", err)
	}
	data = append(data, '\n')
	tmp, err := os.CreateTemp(dir, ".machine.json.*")
	if err != nil {
		return fmt.Errorf("create temporary machine credential: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("secure temporary machine credential: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temporary machine credential: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary machine credential: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace machine credential: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("secure machine credential: %w", err)
	}
	return nil
}

func deleteMachineCredential() error {
	path, err := machineCredentialFile()
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("delete machine credential: %w", err)
	}
	return nil
}

func machineCredentialExists() (bool, error) {
	path, err := machineCredentialFile()
	if err != nil {
		return false, err
	}
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("stat machine credential: %w", err)
	}
	return true, nil
}

func machineCredentialFile() (string, error) {
	if path := strings.TrimSpace(os.Getenv("AGENTWHARF_MACHINE_CREDENTIAL_FILE")); path != "" {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "", errors.New("locate home directory for machine credential")
	}
	return filepath.Join(home, ".agentwharf", "machine.json"), nil
}
