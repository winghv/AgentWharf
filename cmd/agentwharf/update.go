package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/winghv/agentwharf/internal/buildinfo"
)

const (
	defaultLatestReleaseURL = "https://api.github.com/repos/winghv/agentwharf/releases/latest"
	defaultInstallerURL     = "https://github.com/winghv/agentwharf/releases/latest/download/install.sh"
	updateCheckInterval     = 24 * time.Hour
	updateCheckTimeout      = 1200 * time.Millisecond
)

type releaseMetadata struct {
	TagName string `json:"tag_name"`
}

type updateCache struct {
	CheckedAt time.Time `json:"checked_at"`
	Latest    string    `json:"latest"`
}

func runUpgradeCommand(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	checkOnly := false
	for _, arg := range args {
		if arg == "--check" {
			checkOnly = true
			continue
		}
		return errors.New("usage: wharf upgrade [--check]")
	}

	latest, err := fetchLatestVersion(ctx, updateCheckTimeout)
	if err != nil {
		return fmt.Errorf("check for Wharf update: %w", err)
	}
	if !isNewerVersion(buildinfo.Version, latest) {
		_, _ = fmt.Fprintf(stdout, "Wharf %s is up to date.\n", buildinfo.Version)
		return nil
	}
	if checkOnly {
		_, _ = fmt.Fprintf(stdout, "Wharf %s is available (installed: %s). Run: wharf upgrade\n", latest, buildinfo.Version)
		return nil
	}
	if buildinfo.Version == "dev" {
		return errors.New("development builds cannot self-upgrade; install a released Wharf build first")
	}

	_, _ = fmt.Fprintf(stderr, "wharf: upgrading %s to %s\n", buildinfo.Version, latest)
	return runInstaller(ctx, stdin, stdout, stderr)
}

func maybePrintUpdateReminder(parent context.Context, output io.Writer) {
	if os.Getenv("WHARF_NO_UPDATE_CHECK") == "1" || !isReleaseVersion(buildinfo.Version) {
		return
	}
	cachePath, err := updateCachePath()
	if err != nil {
		return
	}
	if cached, ok := readFreshUpdateCache(cachePath, time.Now()); ok {
		printUpdateReminder(output, cached.Latest)
		return
	}
	latest, err := fetchLatestVersion(parent, updateCheckTimeout)
	if err != nil {
		return
	}
	_ = writeUpdateCache(cachePath, updateCache{CheckedAt: time.Now().UTC(), Latest: latest})
	printUpdateReminder(output, latest)
}

func printUpdateReminder(output io.Writer, latest string) {
	if isNewerVersion(buildinfo.Version, latest) {
		_, _ = fmt.Fprintf(output, "wharf: update available %s -> %s; run: wharf upgrade\n", buildinfo.Version, latest)
	}
}

func fetchLatestVersion(parent context.Context, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	endpoint := os.Getenv("WHARF_UPDATE_URL")
	if endpoint == "" {
		endpoint = defaultLatestReleaseURL
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "agentwharf/"+buildinfo.Version)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("release service returned %s", resp.Status)
	}
	var release releaseMetadata
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 64<<10))
	if err := decoder.Decode(&release); err != nil {
		return "", err
	}
	if !isReleaseVersion(release.TagName) {
		return "", errors.New("release service returned an invalid version")
	}
	return release.TagName, nil
}

func runInstaller(ctx context.Context, stdin io.Reader, stdout, stderr io.Writer) error {
	installerURL := os.Getenv("WHARF_INSTALLER_URL")
	if installerURL == "" {
		installerURL = defaultInstallerURL
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, installerURL, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("download Wharf installer: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download Wharf installer: %s", resp.Status)
	}
	tempFile, err := os.CreateTemp("", "wharf-install-*.sh")
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)
	if _, err := io.Copy(tempFile, io.LimitReader(resp.Body, 1<<20)); err != nil {
		tempFile.Close()
		return err
	}
	if err := tempFile.Close(); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "/bin/sh", tempPath)
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("upgrade Wharf: %w", err)
	}
	return nil
}

func updateCachePath() (string, error) {
	if path := os.Getenv("WHARF_UPDATE_CACHE"); path != "" {
		return path, nil
	}
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(cacheDir, "agentwharf", "update.json"), nil
}

func readFreshUpdateCache(path string, now time.Time) (updateCache, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return updateCache{}, false
	}
	var cached updateCache
	if json.Unmarshal(data, &cached) != nil || !isReleaseVersion(cached.Latest) || cached.CheckedAt.After(now) || now.Sub(cached.CheckedAt) >= updateCheckInterval {
		return updateCache{}, false
	}
	return cached, true
}

func writeUpdateCache(path string, cached updateCache) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(cached)
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), "update-*.json")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(tempPath, path)
}

func isNewerVersion(current, candidate string) bool {
	currentParts, ok := parseReleaseVersion(current)
	if !ok {
		return false
	}
	candidateParts, ok := parseReleaseVersion(candidate)
	if !ok {
		return false
	}
	for index := range currentParts {
		if candidateParts[index] != currentParts[index] {
			return candidateParts[index] > currentParts[index]
		}
	}
	return false
}

func isReleaseVersion(version string) bool {
	_, ok := parseReleaseVersion(version)
	return ok
}

func parseReleaseVersion(version string) ([3]int, bool) {
	var parsed [3]int
	version = strings.TrimPrefix(strings.TrimSpace(version), "v")
	parts := strings.Split(version, ".")
	if len(parts) != len(parsed) {
		return parsed, false
	}
	for index, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil || value < 0 {
			return parsed, false
		}
		parsed[index] = value
	}
	return parsed, true
}
