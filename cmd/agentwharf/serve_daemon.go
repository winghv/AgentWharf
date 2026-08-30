package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	daemonPIDFileName       = "wharf-serve.pid"
	daemonLogFileName       = "wharf-serve.log"
	foregroundFlag          = "--foreground"
	daemonReadyFileEnv      = "AGENTWHARF_DAEMON_READY_FILE"
	daemonStartupWait       = 10 * time.Second
	daemonStartupPollPeriod = 25 * time.Millisecond
)

var errServeDaemonLocked = errors.New("another wharf serve daemon is already running for this machine credential")

var errDaemonNotRunning = errors.New("wharf serve is not running")

// daemonStateDir is the directory holding the wharf serve pid and log files.
// It defaults to ~/.agentwharf and can be redirected for tests.
func daemonStateDir() (string, error) {
	if dir := strings.TrimSpace(os.Getenv("AGENTWHARF_DAEMON_DIR")); dir != "" {
		return dir, nil
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "", errors.New("locate home directory for wharf serve state")
	}
	return filepath.Join(home, ".agentwharf"), nil
}

func daemonPIDPath() (string, error) {
	dir, err := daemonStateDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, daemonPIDFileName), nil
}

func daemonLogPath() (string, error) {
	dir, err := daemonStateDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, daemonLogFileName), nil
}

func writeDaemonPID(pid int) error {
	path, err := daemonPIDPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(strconv.Itoa(pid)+"\n"), 0o600)
}

func daemonReadyPath() string {
	if path := strings.TrimSpace(os.Getenv(daemonReadyFileEnv)); path != "" {
		return path
	}
	dir, err := daemonStateDir()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, ".wharf-serve-ready")
}

func markDaemonReady(pid int) error {
	path := daemonReadyPath()
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(strconv.Itoa(pid)+"\n"), 0o600)
}

func removeDaemonReadyIfOwner(ownerPID int) {
	path := daemonReadyPath()
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil || strings.TrimSpace(string(data)) != strconv.Itoa(ownerPID) {
		return
	}
	_ = os.Remove(path)
}

func daemonReady(pid int) bool {
	path := daemonReadyPath()
	if path == "" {
		return false
	}
	data, err := os.ReadFile(path)
	return err == nil && strings.TrimSpace(string(data)) == strconv.Itoa(pid)
}

func readDaemonPID() (int, error) {
	path, err := daemonPIDPath()
	if err != nil {
		return 0, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, errDaemonNotRunning
		}
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return 0, errDaemonNotRunning
	}
	return pid, nil
}

func removeDaemonPIDIfOwner(ownerPID int) {
	pid, err := readDaemonPID()
	if err != nil || pid != ownerPID {
		return
	}
	if path, err := daemonPIDPath(); err == nil {
		_ = os.Remove(path)
	}
}

// ensurePaired gives the user a next step before the daemon's own credential
// load would surface a raw "machine credential not found" error.
func ensurePaired() error {
	exists, err := machineCredentialExists()
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("this machine is not paired yet; run wharf claude (or wharf codex) first, then wharf serve")
	}
	return nil
}

// runServeCommand is the user-facing wharf serve entry point: background by
// default, with explicit stop, status, and --foreground subcommands.
func runServeCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return startBackgroundServe(stdout, stderr)
	}
	switch args[0] {
	case "stop":
		if len(args) != 1 {
			return errors.New("usage: wharf serve stop")
		}
		return stopDaemon(stdout)
	case "status":
		if len(args) != 1 {
			return errors.New("usage: wharf serve status")
		}
		return statusDaemon(stdout)
	case foregroundFlag:
		cfg, err := parseMachineServeConfig(args[1:], stderr)
		if err != nil {
			return err
		}
		return runForegroundServe(ctx, cfg, stdout, stderr)
	case "-h", "--help":
		_, _ = fmt.Fprintln(stderr, "usage: wharf serve [--foreground] [--poll-interval SECONDS] [--max-concurrent N] [--startup-smoke] | wharf serve stop | wharf serve status")
		return errors.New("help requested")
	default:
		return fmt.Errorf("unknown wharf serve argument %q", args[0])
	}
}

// serveDaemonLockPath derives the daemon lock file from the resolved machine
// credential path, so one lock guards exactly one daemon per credential even
// when several credentials share the same state directory.
func serveDaemonLockPath() (string, error) {
	credPath, err := machineCredentialFile()
	if err != nil {
		return "", fmt.Errorf("resolve machine credential for serve lock: %w", err)
	}
	dir, err := daemonStateDir()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(credPath))
	return filepath.Join(dir, fmt.Sprintf("wharf-serve-%x.lock", sum[:4])), nil
}

// acquireServeDaemonLock takes the non-blocking exclusive daemon lock for the
// resolved machine credential. The lock is advisory but hard-enforced: the OS
// releases it when the holding process exits, and a second daemon for the same
// credential fails immediately instead of silently stacking. The lock file is
// never removed, so the file identity the lock guards stays stable.
func acquireServeDaemonLock() (*os.File, error) {
	path, err := serveDaemonLockPath()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("prepare wharf serve lock directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open wharf serve lock: %w", err)
	}
	if err := lockFileExclusive(file); err != nil {
		file.Close()
		return nil, errServeDaemonLocked
	}
	return file, nil
}

// runForegroundServe runs the machine daemon in the foreground, recording its
// pid so wharf serve stop/status still work while it is attached.
func runForegroundServe(ctx context.Context, cfg machineServeConfig, stdout, stderr io.Writer) error {
	if err := ensurePaired(); err != nil {
		return err
	}
	lock, err := acquireServeDaemonLock()
	if err != nil {
		return err
	}
	defer lock.Close()
	pid := os.Getpid()
	if err := writeDaemonPID(pid); err != nil {
		return fmt.Errorf("record wharf serve pid: %w", err)
	}
	defer removeDaemonPIDIfOwner(pid)
	defer removeDaemonReadyIfOwner(pid)
	return runMachineServe(ctx, cfg, stdout, stderr)
}

// startBackgroundServe re-executes the daemon detached from the terminal so it
// keeps serving after the user closes the shell. The child runs with
// --foreground and writes its own pid; the parent exits immediately.
// daemonAlreadyRunning reports whether a live wharf serve daemon has
// completed its authenticated startup handshake for the current state dir.
func daemonAlreadyRunning() bool {
	pid, err := readDaemonPID()
	if err != nil || !processAlive(pid) {
		return false
	}
	return daemonReady(pid)
}

// isTestBinary reports whether the running executable is a go test binary, in
// which case a detached daemon must never be spawned (it would leak past the
// test run and poll a dead mock server).
func isTestBinary() bool {
	exe, err := os.Executable()
	if err != nil {
		return false
	}
	return strings.Contains(filepath.Base(exe), ".test")
}

// ensureBackgroundDaemon starts the background daemon after a one-shot pairing
// or agent session so the machine stays online without a manual wharf serve.
// It returns startup failures so onboarding cannot report a false connection.
func ensureBackgroundDaemon(stderr io.Writer) error {
	if isTestBinary() {
		return nil
	}
	if daemonAlreadyRunning() {
		return nil
	}
	exists, err := machineCredentialExists()
	if err != nil || !exists {
		return nil
	}
	if err := startBackgroundServe(io.Discard, stderr); err != nil {
		_, _ = fmt.Fprintf(stderr, "wharf: could not start the background daemon (%v). Run wharf serve to keep this machine online.\n", err)
		return err
	}
	return nil
}

// acquirePairingReplacementLock prevents a new pairing from replacing the
// credential that an already-running daemon still holds in memory. Callers must
// hold the returned lock until the new credential has been persisted.
func acquirePairingReplacementLock() (*os.File, error) {
	lock, err := acquireServeDaemonLock()
	if err != nil {
		if errors.Is(err, errServeDaemonLocked) {
			return nil, errors.New("an existing wharf serve daemon is using this machine credential; stop it before pairing again")
		}
		return nil, err
	}
	return lock, nil
}

func startBackgroundServe(stdout, stderr io.Writer) error {
	if daemonAlreadyRunning() {
		pid, _ := readDaemonPID()
		_, _ = fmt.Fprintf(stdout, "wharf serve is already running (PID %d).\n", pid)
		return nil
	}
	if err := ensurePaired(); err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locate wharf executable: %w", err)
	}
	logPath, err := daemonLogPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		return err
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open wharf serve log: %w", err)
	}
	defer logFile.Close()

	cmd := exec.Command(exe, "serve", foregroundFlag)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Stdin = nil
	readyPath := filepath.Join(filepath.Dir(logPath), ".wharf-serve-ready")
	cmd.Env = append(os.Environ(), daemonReadyFileEnv+"="+readyPath)
	if err := os.Remove(readyPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("reset wharf serve readiness: %w", err)
	}
	if err := detachBackgroundCommand(cmd); err != nil {
		return fmt.Errorf("start wharf serve in background: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start wharf serve: %w", err)
	}
	pid := cmd.Process.Pid
	if err := waitForDaemonStartup(pid, readyPath); err != nil {
		_ = terminateProcess(pid)
		return fmt.Errorf("wharf serve did not start: %w (logs: %s)", err, logPath)
	}
	_, _ = fmt.Fprintf(stdout, "wharf serve started in the background (PID %d).\n", pid)
	_, _ = fmt.Fprintf(stdout, "Logs: %s\n", logPath)
	_, _ = fmt.Fprintln(stdout, "Stop it with: wharf serve stop")
	return nil
}

func waitForDaemonStartup(expectedPID int, readyPath string) error {
	deadline := time.NewTimer(daemonStartupWait)
	defer deadline.Stop()
	ticker := time.NewTicker(daemonStartupPollPeriod)
	defer ticker.Stop()
	for {
		pid, err := readDaemonPID()
		if err == nil && pid == expectedPID && daemonReady(expectedPID) {
			if processAlive(expectedPID) {
				return nil
			}
			return errors.New("daemon exited during startup")
		}
		select {
		case <-deadline.C:
			return errors.New("daemon did not publish a live PID")
		case <-ticker.C:
		}
	}
}

func stopDaemon(stdout io.Writer) error {
	pid, err := readDaemonPID()
	if err != nil {
		if errors.Is(err, errDaemonNotRunning) {
			_, _ = fmt.Fprintln(stdout, "wharf serve is not running.")
			return nil
		}
		return err
	}
	if !processAlive(pid) {
		removeDaemonPIDIfOwner(pid)
		_, _ = fmt.Fprintln(stdout, "wharf serve is not running (stale pid removed).")
		return nil
	}
	if err := terminateProcess(pid); err != nil {
		return fmt.Errorf("stop wharf serve: %w", err)
	}
	removeDaemonPIDIfOwner(pid)
	_, _ = fmt.Fprintln(stdout, "wharf serve stopped.")
	return nil
}

func statusDaemon(stdout io.Writer) error {
	pid, err := readDaemonPID()
	if err != nil {
		if errors.Is(err, errDaemonNotRunning) {
			_, _ = fmt.Fprintln(stdout, "wharf serve is not running.")
			return nil
		}
		return err
	}
	if !processAlive(pid) {
		removeDaemonPIDIfOwner(pid)
		_, _ = fmt.Fprintln(stdout, "wharf serve is not running (stale pid removed).")
		return nil
	}
	_, _ = fmt.Fprintf(stdout, "wharf serve is running (PID %d).\n", pid)
	return nil
}
