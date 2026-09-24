package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/winghv/agentwharf/internal/buildinfo"
)

const (
	providerSettingsProbeTimeout     = 45 * time.Second
	providerSettingsProbeStopTimeout = 2 * time.Second
)

func runProviderSettingsProbe(ctx context.Context, args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("probe-settings", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	provider := flags.String("provider", "", "ACP provider id")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected probe-settings arguments")
	}
	agent := map[string]string{
		"pi":               "pi",
		"deepseek-harness": "dsh",
		"codex":            "codex",
		"claude-code":      "claude",
	}[*provider]
	if agent == "" {
		return errors.New("unsupported provider settings probe")
	}
	workingDirectory, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get probe working directory: %w", err)
	}
	cfg := wrapConfig{
		Agent:            agent,
		Provider:         *provider,
		ProviderCommand:  defaultProviderCommand(agent),
		SecretDir:        strings.TrimSpace(os.Getenv("AGENTWHARF_SECRET_DIR")),
		WorkingDirectory: filepath.Clean(workingDirectory),
		ProtocolVersion:  1,
	}
	if cfg.SecretDir == "" {
		cfg.e2eeRuntime = &machineE2EERuntime{}
	}
	command, err := providerProcessCommand(cfg, nil, nil, io.Discard)
	if err != nil {
		return err
	}
	probeCtx, cancel := context.WithTimeout(ctx, providerSettingsProbeTimeout)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, command.Path, command.Args...)
	if command.ExactEnv {
		cmd.Env = command.Env
	} else {
		cmd.Env = mergeProbeEnvironment(os.Environ(), command.Env)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("open probe provider stdin: %w", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("open probe provider stdout: %w", err)
	}
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start ACP provider probe: %w", err)
	}
	waited := false
	stopProbe := func() {
		if waited {
			return
		}
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		waitDone := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(waitDone)
		}()
		select {
		case <-waitDone:
			waited = true
		case <-time.After(providerSettingsProbeStopTimeout):
			// A broken child pipe must not hold the machine probe poll open indefinitely.
		}
	}
	defer stopProbe()
	scanner := bufio.NewScanner(stdoutPipe)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	if err := writeACPRequest(stdin, 1, "initialize", map[string]any{
		"protocolVersion":    1,
		"clientInfo":         map[string]any{"name": "agentwharf-settings-probe", "version": buildinfo.Version},
		"clientCapabilities": map[string]any{"fs": map[string]any{"readTextFile": false, "writeTextFile": false}, "terminal": false},
	}); err != nil {
		return err
	}
	if _, err := readACPResponse(probeCtx, scanner, 1); err != nil {
		return fmt.Errorf("initialize ACP settings probe: %w", err)
	}
	if err := writeACPRequest(stdin, 2, "session/new", map[string]any{"cwd": cfg.WorkingDirectory, "mcpServers": []any{}}); err != nil {
		return err
	}
	session, err := readACPResponse(probeCtx, scanner, 2)
	if err != nil {
		return fmt.Errorf("create ACP settings probe session: %w", err)
	}
	sessionID := stringFieldFromAny(session["sessionId"])
	if sessionID == "" {
		return errors.New("ACP settings probe response omitted sessionId")
	}
	tracker := newACPSettingsTracker(session, acpSettingsPolicyForProvider(cfg.Provider))
	state, ok := tracker.Current()
	if !ok {
		return errors.New("ACP settings probe provider did not expose a valid capability")
	}
	capability := state.Capability
	if _, err := json.Marshal(capability); err != nil {
		return fmt.Errorf("encode ACP settings probe capability: %w", err)
	}
	if err := writeACPRequest(stdin, 3, "session/close", map[string]any{"sessionId": sessionID}); err != nil {
		return fmt.Errorf("close ACP settings probe session: %w", err)
	}
	_ = stdin.Close()
	result := make(chan error, 1)
	go func() { result <- cmd.Wait() }()
	select {
	case err := <-result:
		waited = true
		if err != nil && probeCtx.Err() == nil {
			return fmt.Errorf("stop ACP settings probe provider: %w", err)
		}
	case <-time.After(2 * time.Second):
		stopProbe()
		waited = true
	case <-probeCtx.Done():
		return fmt.Errorf("ACP settings probe timed out: %w", probeCtx.Err())
	}
	encoded, err := json.Marshal(capability)
	if err != nil {
		return fmt.Errorf("encode ACP settings probe capability: %w", err)
	}
	_, err = stdout.Write(encoded)
	if err == nil {
		_, err = io.WriteString(stdout, "\n")
	}
	return err
}

func mergeProbeEnvironment(parent, additions []string) []string {
	values := make(map[string]string, len(parent)+len(additions))
	order := make([]string, 0, len(parent)+len(additions))
	for _, entry := range append(append([]string(nil), parent...), additions...) {
		name, value, ok := strings.Cut(entry, "=")
		if !ok || name == "" {
			continue
		}
		if _, exists := values[name]; !exists {
			order = append(order, name)
		}
		values[name] = value
	}
	result := make([]string, 0, len(order))
	for _, name := range order {
		result = append(result, name+"="+values[name])
	}
	return result
}
