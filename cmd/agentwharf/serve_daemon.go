package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	daemonPIDFileName = "wharf-serve.pid"
	daemonLogFileName = "wharf-serve.log"
	foregroundFlag    = "--foreground"
)

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

// runForegroundServe runs the machine daemon in the foreground, recording its
// pid so wharf serve stop/status still work while it is attached.
func runForegroundServe(ctx context.Context, cfg machineServeConfig, stdout, stderr io.Writer) error {
	if err := ensurePaired(); err != nil {
		return err
	}
	pid := os.Getpid()
	if err := writeDaemonPID(pid); err != nil {
		return fmt.Errorf("record wharf serve pid: %w", err)
	}
	defer removeDaemonPIDIfOwner(pid)
	return runMachineServe(ctx, cfg, stdout, stderr)
}

// startBackgroundServe re-executes the daemon detached from the terminal so it
// keeps serving after the user closes the shell. The child runs with
// --foreground and writes its own pid; the parent exits immediately.
// daemonAlreadyRunning reports whether a live wharf serve daemon owns the pid file.
func daemonAlreadyRunning() bool {
	pid, err := readDaemonPID()
	if err != nil {
		return false
	}
	return processAlive(pid)
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
// It is best-effort and silent on success; failures get a recovery hint.
func ensureBackgroundDaemon(stderr io.Writer) {
	if isTestBinary() {
		return
	}
	if daemonAlreadyRunning() {
		return
	}
	exists, err := machineCredentialExists()
	if err != nil || !exists {
		return
	}
	if err := startBackgroundServe(io.Discard, stderr); err != nil {
		_, _ = fmt.Fprintf(stderr, "wharf: could not start the background daemon (%v). Run wharf serve to keep this machine online.\n", err)
	}
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
	if err := detachBackgroundCommand(cmd); err != nil {
		return fmt.Errorf("start wharf serve in background: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start wharf serve: %w", err)
	}
	_, _ = fmt.Fprintf(stdout, "wharf serve started in the background (PID %d).\n", cmd.Process.Pid)
	_, _ = fmt.Fprintf(stdout, "Logs: %s\n", logPath)
	_, _ = fmt.Fprintln(stdout, "Stop it with: wharf serve stop")
	return nil
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
