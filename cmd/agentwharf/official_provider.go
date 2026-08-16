package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/creack/pty"
	"github.com/winghv/agentwharf/adapter/core"
	"github.com/winghv/agentwharf/protocol"
	"golang.org/x/term"
)

// runOfficialProvider launches the native claude/codex CLI in the user's
// terminal (the official interactive TUI) and mirrors its session transcript to
// the Hub. It is used only for interactive wharf claude / wharf codex.
func runOfficialProvider(ctx context.Context, cfg wrapConfig, connection *hubConnection, masker *core.EventMasker, metrics *core.AdapterMetrics) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	writeFrame := func(frame protocol.Frame) error { return connection.write(ctx, frame) }
	publishState := func(state string) error {
		payload, err := json.Marshal(map[string]any{"state": state, "provider": cfg.Provider})
		if err != nil {
			return err
		}
		return writeFrame(&protocol.Event{Type: "session.state", SessionID: cfg.SessionID, Time: time.Now().UTC().UnixMilli(), Payload: payload})
	}

	if err := publishState("starting"); err != nil {
		return err
	}

	sessionID := newSessionUUID()
	command := officialAgentCommand(cfg.Agent)
	args := append([]string{}, cfg.ProviderCommand[1:]...)
	args = append(args, "--session-id", sessionID)

	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Dir = cfg.WorkingDirectory
	ptmx, err := pty.Start(cmd)
	if err != nil {
		return fmt.Errorf("start official agent %s: %w", command, err)
	}
	defer ptmx.Close()

	// Match the PTY to the user's terminal so the official TUI adapts its
	// layout, then keep it in sync on window resize.
	if width, height, sizeErr := term.GetSize(int(os.Stdin.Fd())); sizeErr == nil {
		_ = pty.Setsize(ptmx, &pty.Winsize{Rows: uint16(height), Cols: uint16(width)})
	}
	stopResize := watchTerminalResize(ptmx)
	defer stopResize()

	// Raw mode so the official TUI keeps single-key interaction through our PTY.
	oldState, rawErr := term.MakeRaw(int(os.Stdin.Fd()))
	if rawErr == nil {
		defer term.Restore(int(os.Stdin.Fd()), oldState)
	}

	if err := publishState("ready"); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return err
	}

	// Publish a read-only settings capability from the launch args so the Hub
	// can display the model, permission, and reasoning for this Session.
	if cfg.ProtocolVersion == protocol.ProtocolVersionV2 {
		_ = publishOfficialSettingsCapability(writeFrame, cfg.SessionID, cfg.ProviderCommand[1:])
	}

	// CLI output to the terminal, terminal input to the CLI.
	go func() { _, _ = io.Copy(os.Stdout, ptmx) }()
	go func() { _, _ = io.Copy(ptmx, os.Stdin) }()

	mirrorDone := make(chan error, 1)
	injected := &injectedPromptTracker{pending: make(map[string]int)}
	go func() {
		mirrorDone <- mirrorTranscript(runCtx, cfg, sessionID, writeFrame, injected)
	}()

	commandDone := make(chan error, 1)
	go func() {
		commandDone <- forwardHubCommandsToOfficialCLI(runCtx, connection, writeFrame, ptmx, injected)
	}()

	waitErr := cmd.Wait()
	// The official CLI has exited: stop the mirror/command loops so this returns
	// promptly instead of hanging on their blocking reads/polls.
	cancel()
	_ = publishState("ended")
	_ = ignoreContextError(waitErr)

	select {
	case <-mirrorDone:
	case <-ctx.Done():
	}
	select {
	case <-commandDone:
	case <-ctx.Done():
	}
	return nil
}

// forwardHubCommandsToOfficialCLI reads Hub frames and injects session.send
// instructions into the running official CLI's PTY, so the Hub can drive the
// same session the user is operating locally. It acks the command after the
// prompt is written and keeps the proposal/credential machinery healthy.
func forwardHubCommandsToOfficialCLI(ctx context.Context, connection *hubConnection, writeFrame func(protocol.Frame) error, ptmx *os.File, injected *injectedPromptTracker) error {
	accepted := newAcceptedCommandSet(2048)
	for {
		frame, err := connection.read(ctx)
		if err != nil {
			return ignoreContextError(err)
		}
		switch typed := frame.(type) {
		case *protocol.Ping:
			if err := writeFrame(&protocol.Pong{Nonce: typed.Nonce}); err != nil {
				return err
			}
		case *protocol.Command:
			if accepted.Contains(typed.CommandID) {
				if err := writeFrame(&protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckAccepted}); err != nil {
					return err
				}
				continue
			}
			if typed.Type != protocol.CommandSessionSend {
				if err := writeFrame(&protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckRejected, Reason: "unsupported"}); err != nil {
					return err
				}
				continue
			}
			prompt, err := acpPromptFromSessionSend(typed.Payload)
			if err != nil {
				if writeErr := writeFrame(&protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckRejected, Reason: err.Error()}); writeErr != nil {
					return writeErr
				}
				continue
			}
			var text strings.Builder
			for _, part := range prompt {
				if t, ok := part["text"].(string); ok {
					text.WriteString(t)
				}
			}
			if text.Len() == 0 {
				if writeErr := writeFrame(&protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckRejected, Reason: "empty prompt"}); writeErr != nil {
					return writeErr
				}
				continue
			}
			// Enter is 0x0D in raw mode; 0x0A is the newline key, so sending \n
			// only inserts a line break without submitting the prompt.
			if _, err := ptmx.Write([]byte(text.String() + "\r")); err != nil {
				if writeErr := writeFrame(&protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckRejected, Reason: err.Error()}); writeErr != nil {
					return writeErr
				}
				continue
			}
			if injected != nil {
				injected.add(text.String())
			}
			accepted.Add(typed.CommandID)
			if err := writeFrame(&protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckAccepted}); err != nil {
				return err
			}
		case *protocol.Error:
			return fmt.Errorf("hub error %s: %s", typed.Code, typed.Message)
		}
	}
}

func officialAgentCommand(agent string) string {
	switch agent {
	case "claude", "claude-code":
		return "claude"
	case "codex":
		return "codex"
	case "gemini":
		return "gemini"
	default:
		return agent
	}
}

func newSessionUUID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func claudeTranscriptPath(cwd, sessionID string) (string, error) {
	abs, err := filepath.Abs(cwd)
	if err != nil {
		return "", err
	}
	projectID := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' {
			return r
		}
		return '-'
	}, abs)
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	configDir := os.Getenv("CLAUDE_CONFIG_DIR")
	if configDir == "" {
		configDir = filepath.Join(home, ".claude")
	}
	return filepath.Join(configDir, "projects", projectID, sessionID+".jsonl"), nil
}

func mirrorTranscript(ctx context.Context, cfg wrapConfig, sessionID string, writeFrame func(protocol.Frame) error, injected *injectedPromptTracker) error {
	cwd := cfg.WorkingDirectory
	if cwd == "" {
		cwd = "."
	}
	path, err := claudeTranscriptPath(cwd, sessionID)
	if err != nil {
		return err
	}
	for {
		if _, err := os.Stat(path); err == nil {
			break
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}

	var offset int64
	for {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		if _, err := file.Seek(offset, 0); err != nil {
			file.Close()
			return err
		}
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			offset += int64(len(scanner.Bytes())) + 1
			// Skip user messages that were injected from the Hub; they are already
			// recorded by the Hub itself and would otherwise be mirrored twice.
			if text := transcriptUserText(scanner.Bytes()); text != "" && injected != nil && injected.consume(text) {
				continue
			}
			events, err := translateTranscriptLine(cfg.SessionID, scanner.Bytes())
			if err != nil {
				continue
			}
			for _, event := range events {
				if err := writeFrame(&event); err != nil {
					file.Close()
					return err
				}
			}
		}
		file.Close()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

func translateTranscriptLine(sessionID string, line []byte) ([]protocol.Event, error) {
	var raw map[string]any
	if err := json.Unmarshal(line, &raw); err != nil {
		return nil, err
	}
	entryType := stringFieldFromAny(raw["type"])
	if entryType != "user" && entryType != "assistant" {
		return nil, nil
	}
	entryID := stringFieldFromAny(raw["uuid"])
	message, _ := raw["message"].(map[string]any)
	if message == nil {
		return nil, nil
	}
	role := stringFieldFromAny(message["role"])
	content := message["content"]

	events := make([]protocol.Event, 0, 4)
	if text, ok := content.(string); ok {
		if text != "" {
			events = append(events, transcriptMessageEvent(sessionID, entryID, role, text))
		}
		return events, nil
	}
	blocks, _ := content.([]any)
	for _, item := range blocks {
		block, _ := item.(map[string]any)
		if block == nil {
			continue
		}
		switch stringFieldFromAny(block["type"]) {
		case "text":
			if text := stringFieldFromAny(block["text"]); text != "" {
				events = append(events, transcriptMessageEvent(sessionID, entryID, role, text))
			}
		case "tool_use":
			id := stringFieldFromAny(block["id"])
			name := stringFieldFromAny(block["name"])
			input, _ := json.Marshal(block["input"])
			events = append(events, transcriptToolCallEvent(sessionID, id, "start", name, input))
		}
	}
	return events, nil
}

func transcriptMessageEvent(sessionID, messageID, role, text string) protocol.Event {
	payload, _ := json.Marshal(map[string]any{
		"message_id": messageID,
		"role":       role,
		"content":    []map[string]string{{"kind": "text", "text": text}},
	})
	return protocol.Event{Type: "session.message", SessionID: sessionID, Time: time.Now().UTC().UnixMilli(), Payload: payload}
}

func transcriptToolCallEvent(sessionID, toolCallID, phase, name string, input []byte) protocol.Event {
	payload, _ := json.Marshal(map[string]any{
		"tool_call_id": toolCallID,
		"phase":        phase,
		"name":         name,
		"input":        json.RawMessage(input),
	})
	return protocol.Event{Type: "session.tool_call", SessionID: sessionID, Time: time.Now().UTC().UnixMilli(), Payload: payload}
}

// parseOfficialLaunchArgs extracts model/permission/reasoning from the
// passthrough agent arguments (e.g. --model sonnet --permission-mode acceptEdits).
func parseOfficialLaunchArgs(args []string) (model, permission, reasoning string) {
	for i := 0; i < len(args); i++ {
		name, value, hasValue := strings.Cut(args[i], "=")
		nextValue := func() string {
			if hasValue {
				return value
			}
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				i++
				return args[i]
			}
			return ""
		}
		switch name {
		case "--model", "-m":
			model = nextValue()
		case "--permission-mode":
			permission = nextValue()
		case "--reasoning-effort", "--effort":
			reasoning = nextValue()
		}
	}
	return model, permission, reasoning
}

// publishOfficialSettingsCapability publishes a read-only settings capability
// reflecting the launch args so the Hub can display the model, permission, and
// reasoning even though the official CLI has no mutable ACP controls.
func publishOfficialSettingsCapability(writeFrame func(protocol.Frame) error, sessionID string, args []string) error {
	model, permission, reasoning := parseOfficialLaunchArgs(args)
	if model == "" {
		model = "default"
	}
	if permission == "" {
		permission = "default"
	}
	capability := protocol.SettingsCapabilityPayload{
		SchemaVersion:                 protocol.SettingsCapabilitySchemaVersion,
		Models:                        []protocol.SettingsCapabilityChoice{{ID: model, Label: model}},
		PermissionModes:               []protocol.SettingsCapabilityChoice{{ID: permission, Label: permission}},
		EffectiveModelID:              model,
		EffectivePermissionModeID:     permission,
		ModelChange:                   "read_only",
		ReasoningEffortChange:         "unsupported",
		PermissionChange:              "read_only",
		ModelReadOnlyReason:           stringPointer("official_cli"),
		ReasoningEffortReadOnlyReason: stringPointer("provider_unsupported"),
		PermissionReadOnlyReason:      stringPointer("official_cli"),
	}
	if reasoning != "" {
		capability.ReasoningEfforts = []protocol.SettingsCapabilityChoice{{ID: reasoning, Label: reasoning}}
		capability.EffectiveReasoningEffortID = stringPointer(reasoning)
		capability.ReasoningEffortChange = "read_only"
		capability.ReasoningEffortReadOnlyReason = stringPointer("official_cli")
	}
	capability.Fingerprint = protocol.SettingsCapabilityFingerprint(capability)
	payload, err := json.Marshal(capability)
	if err != nil {
		return err
	}
	return writeFrame(&protocol.Event{Type: "session.settings.capabilities", SessionID: sessionID, Time: time.Now().UTC().UnixMilli(), Payload: payload})
}

// injectedPromptTracker remembers Hub-injected prompts so the transcript mirror
// can skip the matching user message (which the Hub already recorded) instead of
// publishing it a second time.
type injectedPromptTracker struct {
	mu      sync.Mutex
	pending map[string]int
}

func (t *injectedPromptTracker) add(text string) {
	if t == nil {
		return
	}
	key := strings.TrimSpace(text)
	if key == "" {
		return
	}
	t.mu.Lock()
	t.pending[key]++
	t.mu.Unlock()
}

func (t *injectedPromptTracker) consume(text string) bool {
	if t == nil {
		return false
	}
	key := strings.TrimSpace(text)
	if key == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pending[key] == 0 {
		return false
	}
	t.pending[key]--
	if t.pending[key] == 0 {
		delete(t.pending, key)
	}
	return true
}

// transcriptUserText returns the plain-text body of a user transcript entry, or
// an empty string when the line is not a plain-text user message.
func transcriptUserText(line []byte) string {
	var raw map[string]any
	if json.Unmarshal(line, &raw) != nil {
		return ""
	}
	if stringFieldFromAny(raw["type"]) != "user" {
		return ""
	}
	message, _ := raw["message"].(map[string]any)
	if message == nil {
		return ""
	}
	if stringFieldFromAny(message["role"]) != "user" {
		return ""
	}
	text, ok := message["content"].(string)
	if !ok {
		return ""
	}
	return text
}
