package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/creack/pty"
	"github.com/winghv/agentwharf/adapter/core"
	"github.com/winghv/agentwharf/protocol"
	"golang.org/x/term"
)

// runOfficialProvider launches the native agent CLI in the user's terminal (the
// official interactive TUI) and mirrors its session transcript to the Hub. It is
// used only for interactive wharf claude / wharf codex. The agent-specific half
// is provided by an agentProvider module (see agent_provider.go).
func runOfficialProvider(ctx context.Context, cfg wrapConfig, connection *hubConnection, masker *core.EventMasker, metrics *core.AdapterMetrics) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	provider := officialProviderForAgent(cfg.Agent)

	writeFrame := func(frame protocol.Frame) error { return connection.write(ctx, frame) }
	// Reduce the working directory to its basename (like the ACP ready payload)
	// so the Hub can group the Session by directory without durably storing the
	// host's full path.
	cwdBasename := ""
	if cwd, cwdErr := providerWorkingDirectory(cfg.WorkingDirectory); cwdErr == nil {
		cwdBasename = cwdEventBasename(cwd)
	}
	publishState := func(state string) (string, error) {
		body := map[string]any{"state": state, "provider": cfg.Provider}
		if cwdBasename != "" {
			body["metadata"] = map[string]any{"cwd": cwdBasename}
		}
		payload, err := json.Marshal(body)
		if err != nil {
			return "", err
		}
		event := &protocol.Event{Type: "session.state", SessionID: cfg.SessionID, Time: time.Now().UTC().UnixMilli(), Payload: payload}
		if err := writeFrame(event); err != nil {
			return "", err
		}
		return event.ProposalID, nil
	}

	if _, err := publishState("starting"); err != nil {
		return err
	}

	sessionID := newSessionUUID()
	args := append([]string{}, cfg.ProviderCommand[1:]...)
	args = append(args, provider.sessionArgs(sessionID)...)

	launchTime := time.Now()
	cmd := exec.CommandContext(ctx, provider.command(), args...)
	cmd.Dir = cfg.WorkingDirectory
	ptmx, err := pty.Start(cmd)
	if err != nil {
		return fmt.Errorf("start official agent %s: %w", provider.command(), err)
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

	if _, err := publishState("ready"); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return err
	}

	// Publish a read-only settings capability from the launch args so the Hub
	// can display the model, permission, and reasoning for this Session.
	if cfg.ProtocolVersion == protocol.ProtocolVersionV2 {
		_ = publishOfficialSettingsCapability(writeFrame, cfg.SessionID, provider.launchSettings(cfg.ProviderCommand[1:]))
	}

	// CLI output to the terminal, terminal input to the CLI.
	go func() { _, _ = io.Copy(os.Stdout, ptmx) }()
	go func() { _, _ = io.Copy(ptmx, os.Stdin) }()

	mirrorDone := make(chan error, 1)
	injected := &injectedPromptTracker{pending: make(map[string]int)}
	questions := newQuestionCache()
	go func() {
		mirrorDone <- mirrorTranscript(runCtx, cfg, provider, sessionID, launchTime, writeFrame, injected, questions)
	}()

	commandDone := make(chan error, 1)
	go func() {
		commandDone <- forwardHubCommandsToOfficialCLI(runCtx, connection, writeFrame, ptmx, injected, questions)
	}()

	waitErr := cmd.Wait()
	// The official CLI has exited: stop the mirror/command loops so this returns
	// promptly instead of hanging on their blocking reads/polls. Wait for the
	// command reader to release the Hub socket before reading the terminal-state
	// receipt on this goroutine.
	cancel()

	select {
	case <-mirrorDone:
	case <-ctx.Done():
	}
	select {
	case <-commandDone:
	case <-ctx.Done():
	}
	proposalID, err := publishState("ended")
	if err != nil {
		return fmt.Errorf("publish official agent terminal state: %w", err)
	}
	if cfg.ProtocolVersion == protocol.ProtocolVersionV2 {
		if proposalID == "" {
			return errors.New("official agent terminal state is missing a proposal ID")
		}
		receiptCtx, receiptCancel := context.WithTimeout(ctx, 10*time.Second)
		defer receiptCancel()
		if err := waitEventReceipt(receiptCtx, connection.read, writeFrame, proposalID, "official agent terminal state"); err != nil {
			return err
		}
	}
	_ = ignoreContextError(waitErr)
	return nil
}

// forwardHubCommandsToOfficialCLI reads Hub frames and injects session.send
// instructions into the running official CLI's PTY, so the Hub can drive the
// same session the user is operating locally. It acks the command after the
// prompt is written and keeps the proposal/credential machinery healthy.
func forwardHubCommandsToOfficialCLI(ctx context.Context, connection *hubConnection, writeFrame func(protocol.Frame) error, ptmx *os.File, injected *injectedPromptTracker, questions *questionCache) error {
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
			if typed.Type == protocol.CommandPermissionRespond {
				if err := forwardQuestionAnswer(ptmx, questions, typed.Payload); err != nil {
					if writeErr := writeFrame(&protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckRejected, Reason: err.Error()}); writeErr != nil {
						return writeErr
					}
					continue
				}
				accepted.Add(typed.CommandID)
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
			promptText := text.String()
			if injected != nil {
				// Register before writing: Claude can append its transcript entry
				// immediately, otherwise the mirror may publish the first prompt
				// before the tracker is updated.
				injected.add(promptText)
			}
			if _, err := ptmx.Write([]byte(promptText + "\r")); err != nil {
				if injected != nil {
					injected.remove(promptText)
				}
				if writeErr := writeFrame(&protocol.CommandAck{CommandID: typed.CommandID, Status: protocol.AckRejected, Reason: err.Error()}); writeErr != nil {
					return writeErr
				}
				continue
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

// forwardQuestionAnswer injects an AskUserQuestion answer into the claude PTY.
// It looks up the cached questions by the permission request_id, then writes the
// down-arrow/Enter keystrokes for the selected options.
//
// UNTESTED against the live claude TUI: the exact navigation may differ across
// claude versions.
func forwardQuestionAnswer(ptmx *os.File, questions *questionCache, payload json.RawMessage) error {
	var req struct {
		RequestID string            `json:"request_id"`
		Decision  string            `json:"decision"`
		Answers   map[string]string `json:"answers"`
	}
	if json.Unmarshal(payload, &req) != nil {
		return fmt.Errorf("invalid permission.respond payload")
	}
	// Deny/empty answers are a no-op: there is nothing to inject.
	if req.Decision != "approve" || len(req.Answers) == 0 {
		return nil
	}
	toolCallID := strings.TrimPrefix(req.RequestID, "question:")
	if toolCallID == "" {
		return fmt.Errorf("missing question request id")
	}
	qs := questions.Get(toolCallID)
	if len(qs) == 0 {
		return fmt.Errorf("question context not found")
	}
	keystrokes := askUserQuestionKeystrokes(qs, req.Answers)
	if len(keystrokes) == 0 {
		return nil
	}
	_, err := ptmx.Write(keystrokes)
	return err
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

// mirrorTranscript tails the agent's transcript file and publishes new messages
// to the Hub. The provider resolves the transcript path and translates lines;
// the tailing, prompt dedup, and publish loop are shared.
func mirrorTranscript(ctx context.Context, cfg wrapConfig, provider agentProvider, sessionID string, launchTime time.Time, writeFrame func(protocol.Frame) error, injected *injectedPromptTracker, questions *questionCache) error {
	path, err := provider.transcriptPath(ctx, cfg, sessionID, launchTime)
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
			if text := provider.transcriptUserText(scanner.Bytes()); text != "" && injected != nil && injected.consume(text) {
				continue
			}
			events, err := provider.translateLine(cfg.SessionID, scanner.Bytes())
			if err != nil {
				continue
			}
			for _, event := range events {
				cacheAskUserQuestionEvent(questions, &event)
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

func transcriptMessageEvent(sessionID, messageID, role, text string) protocol.Event {
	payload, _ := json.Marshal(map[string]any{
		"message_id": messageID,
		"role":       role,
		"content":    []map[string]string{{"kind": "text", "text": text}},
	})
	return protocol.Event{Type: "session.message", SessionID: sessionID, Time: time.Now().UTC().UnixMilli(), Payload: payload}
}

func transcriptMessageEvents(sessionID, messageID, role, text string) []protocol.Event {
	return splitTranscriptMessageEvents(text, protocol.MaxEventPayloadBytes-4*1024, func(part string) protocol.Event {
		return transcriptMessageEvent(sessionID, messageID, role, part)
	})
}

func splitTranscriptMessageEvents(text string, maxPayloadBytes int, event func(string) protocol.Event) []protocol.Event {
	if text == "" || maxPayloadBytes < 1 || event == nil {
		return nil
	}
	remaining := text
	result := make([]protocol.Event, 0, 1)
	for len(remaining) > 0 {
		end := len(remaining)
		if end > 8*1024 {
			end = 8 * 1024
		}
		end = transcriptUTF8PrefixEnd(remaining, end)
		if end == 0 {
			end = 1
		}
		for end > 1 && len(event(remaining[:end]).Payload) > maxPayloadBytes {
			end = transcriptUTF8PrefixEnd(remaining, end/2)
		}
		result = append(result, event(remaining[:end]))
		remaining = remaining[end:]
	}
	return result
}

func transcriptUTF8PrefixEnd(text string, end int) int {
	if end >= len(text) {
		return len(text)
	}
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return end
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

// publishOfficialSettingsCapability publishes a read-only settings capability
// reflecting the launch args so the Hub can display the model, permission, and
// reasoning even though the official CLI has no mutable ACP controls.
func publishOfficialSettingsCapability(writeFrame func(protocol.Frame) error, sessionID string, settings launchSettings) error {
	model, permission, reasoning := settings.model, settings.permission, settings.reasoning
	if model == "" {
		model = "default"
	}
	if permission == "" {
		permission = "default"
	}
	capability := protocol.SettingsCapabilityPayload{
		SchemaVersion:                 protocol.SettingsCapabilitySchemaVersion,
		Models:                        []protocol.SettingsCapabilityChoice{{ID: model, Label: model}},
		ReasoningEfforts:              []protocol.SettingsCapabilityChoice{},
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

func (t *injectedPromptTracker) remove(text string) {
	if t == nil {
		return
	}
	key := strings.TrimSpace(text)
	if key == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pending[key] <= 1 {
		delete(t.pending, key)
		return
	}
	t.pending[key]--
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
