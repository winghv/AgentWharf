package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	directoryIdentityKeySize    = 32
	directoryIdentityPrefix     = "dir_v1_"
	directoryIdentityDomain     = "agentwharf.session-directory-id.v1"
	maxDirectoryIdentityEntries = 256
)

type directoryIdentityEntry struct {
	Path     string `json:"path"`
	LastSeen int64  `json:"last_seen"`
}

var directoryIdentityRegistryMu sync.Mutex

func directoryIdentityKeyFile() (string, error) {
	credentialPath, err := machineCredentialFile()
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(credentialPath), "directory-identity.key"), nil
}

func directoryIdentityRegistryFile() (string, error) {
	keyPath, err := directoryIdentityKeyFile()
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(keyPath), "directory-identities.json"), nil
}

func rememberDirectoryIdentity(id, path string) error {
	if !validDirectoryIdentityID(id) || !filepath.IsAbs(path) {
		return errors.New("invalid directory identity mapping")
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat directory identity path: %w", err)
	}
	if !info.IsDir() {
		return errors.New("directory identity path is not a directory")
	}
	registryPath, err := directoryIdentityRegistryFile()
	if err != nil {
		return err
	}
	directoryIdentityRegistryMu.Lock()
	defer directoryIdentityRegistryMu.Unlock()
	entries := map[string]directoryIdentityEntry{}
	if data, readErr := os.ReadFile(registryPath); readErr == nil {
		if err := json.Unmarshal(data, &entries); err != nil {
			return errors.New("decode directory identity registry")
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return fmt.Errorf("read directory identity registry: %w", readErr)
	}
	for existingID, entry := range entries {
		if !validDirectoryIdentityID(existingID) || !filepath.IsAbs(entry.Path) {
			delete(entries, existingID)
		}
	}
	entries[id] = directoryIdentityEntry{Path: filepath.Clean(path), LastSeen: time.Now().UTC().UnixNano()}
	for len(entries) > maxDirectoryIdentityEntries {
		oldestID := ""
		oldestSeen := int64(0)
		for candidateID, entry := range entries {
			if oldestID == "" || entry.LastSeen < oldestSeen || (entry.LastSeen == oldestSeen && candidateID < oldestID) {
				oldestID, oldestSeen = candidateID, entry.LastSeen
			}
		}
		delete(entries, oldestID)
	}
	return writeDirectoryIdentityRegistry(registryPath, entries)
}

func resolveDirectoryIdentity(id string) (string, error) {
	if !validDirectoryIdentityID(id) {
		return "", errors.New("invalid directory identity")
	}
	registryPath, err := directoryIdentityRegistryFile()
	if err != nil {
		return "", err
	}
	directoryIdentityRegistryMu.Lock()
	defer directoryIdentityRegistryMu.Unlock()
	data, err := os.ReadFile(registryPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", errors.New("directory identity is unavailable")
		}
		return "", fmt.Errorf("read directory identity registry: %w", err)
	}
	entries := map[string]directoryIdentityEntry{}
	if err := json.Unmarshal(data, &entries); err != nil {
		return "", errors.New("decode directory identity registry")
	}
	entry, ok := entries[id]
	if !ok || !filepath.IsAbs(entry.Path) {
		return "", errors.New("directory identity is unavailable")
	}
	info, err := os.Stat(entry.Path)
	if err != nil {
		return "", errors.New("directory identity path is unavailable")
	}
	if !info.IsDir() {
		return "", errors.New("directory identity path is not a directory")
	}
	return filepath.Clean(entry.Path), nil
}

func writeDirectoryIdentityRegistry(path string, entries map[string]directoryIdentityEntry) error {
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return fmt.Errorf("encode directory identity registry: %w", err)
	}
	data = append(data, '\n')
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create directory identity registry directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("secure directory identity registry directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".directory-identities.json.*")
	if err != nil {
		return fmt.Errorf("create temporary directory identity registry: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("secure temporary directory identity registry: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temporary directory identity registry: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temporary directory identity registry: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary directory identity registry: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace directory identity registry: %w", err)
	}
	return os.Chmod(path, 0o600)
}

func loadOrCreateDirectoryIdentityKey(path string) ([]byte, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create directory identity config directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("secure directory identity config directory: %w", err)
	}

	for attempt := 0; attempt < 100; attempt++ {
		info, err := os.Lstat(path)
		if err == nil {
			if !info.Mode().IsRegular() {
				return nil, errors.New("directory identity key is not a regular file")
			}
			key, readErr := os.ReadFile(path)
			if readErr != nil {
				return nil, fmt.Errorf("read directory identity key: %w", readErr)
			}
			if len(key) == directoryIdentityKeySize {
				if err := os.Chmod(path, 0o600); err != nil {
					return nil, fmt.Errorf("secure directory identity key: %w", err)
				}
				return key, nil
			}
			if len(key) > directoryIdentityKeySize {
				return nil, errors.New("directory identity key has invalid size")
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("inspect directory identity key: %w", err)
		}

		key := make([]byte, directoryIdentityKeySize)
		if _, err := rand.Read(key); err != nil {
			return nil, fmt.Errorf("generate directory identity key: %w", err)
		}
		tmp, err := os.CreateTemp(dir, ".directory-identity.key.*")
		if err != nil {
			return nil, fmt.Errorf("create temporary directory identity key: %w", err)
		}
		tmpPath := tmp.Name()
		cleanup := func() {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
		if err := tmp.Chmod(0o600); err != nil {
			cleanup()
			return nil, fmt.Errorf("secure temporary directory identity key: %w", err)
		}
		if _, err := tmp.Write(key); err != nil {
			cleanup()
			return nil, fmt.Errorf("write directory identity key: %w", err)
		}
		if err := tmp.Sync(); err != nil {
			cleanup()
			return nil, fmt.Errorf("sync directory identity key: %w", err)
		}
		if err := tmp.Close(); err != nil {
			_ = os.Remove(tmpPath)
			return nil, fmt.Errorf("close directory identity key: %w", err)
		}
		// Linking the completed temporary inode creates the final name without
		// exposing a partially written key and never replaces a concurrent key.
		if err := os.Link(tmpPath, path); err != nil {
			_ = os.Remove(tmpPath)
			if errors.Is(err, os.ErrExist) {
				continue
			}
			return nil, fmt.Errorf("publish directory identity key: %w", err)
		}
		_ = os.Remove(tmpPath)
		return key, nil
	}
	return nil, errors.New("directory identity key creation timed out")
}

func directoryIdentityForPath(path string, key []byte) string {
	if len(key) != directoryIdentityKeySize || !filepath.IsAbs(path) {
		return ""
	}
	clean := filepath.Clean(path)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(directoryIdentityDomain))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(clean))
	return directoryIdentityPrefix + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)[:16])
}

func sessionDirectoryMetadata(path string) map[string]any {
	cwd, err := providerWorkingDirectory(path)
	if err != nil {
		return nil
	}
	basename := cwdEventBasename(cwd)
	if basename == "" {
		return nil
	}
	metadata := map[string]any{"cwd": basename}
	keyPath, err := directoryIdentityKeyFile()
	if err != nil {
		return metadata
	}
	key, err := loadOrCreateDirectoryIdentityKey(keyPath)
	if err != nil {
		return metadata
	}
	if id := directoryIdentityForPath(cwd, key); id != "" {
		// The opaque ID is useful to other browsers only when this Agent can
		// resolve it locally later. Never publish an ID without its local map.
		if err := rememberDirectoryIdentity(id, cwd); err == nil {
			metadata["cwd_id"] = id
		}
	}
	return metadata
}

func validDirectoryIdentityID(value string) bool {
	if !strings.HasPrefix(value, directoryIdentityPrefix) || len(value) != len(directoryIdentityPrefix)+22 {
		return false
	}
	for _, character := range strings.TrimPrefix(value, directoryIdentityPrefix) {
		if (character < 'A' || character > 'Z') &&
			(character < 'a' || character > 'z') &&
			(character < '0' || character > '9') && character != '_' && character != '-' {
			return false
		}
	}
	return true
}
