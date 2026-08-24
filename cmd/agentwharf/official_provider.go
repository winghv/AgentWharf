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
	"sync/atomic"
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
		removeReconnectRunControl := connection.setReconnectProposalFactory("official-run-control", func() (*protocol.Event, error) {
			return newOfficialRunControlCapabilityEvent(cfg)
		})
		defer removeReconnectRunControl()
		if err := publishOfficialRunControlCapability(cfg, writeFrame); err != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			return fmt.Errorf("publish official agent run-control capability: %w", err)
		}
	}

	// CLI output to the terminal. Local keystrokes and Hub injects share ptyMu
	// so an idle recap or mouse-tracking burst cannot splice into a prompt.
	var ptyMu sync.Mutex
	go func() { _, _ = io.Copy(os.Stdout, ptmx) }()
	go func() { _ = copyLocked(ptmx, os.Stdin, &ptyMu) }()

	mirrorDone := make(chan error, 1)
	injected := &injectedPromptTracker{pending: make(map[string]int)}
	questions := newQuestionCache()
	go func() {
		mirrorDone <- mirrorTranscript(runCtx, cfg, provider, sessionID, launchTime, writeFrame, injected, questions)
	}()

	commandDone := make(chan error, 1)
	var stopInProgress atomic.Bool
	go func() {
		commandDone <- forwardHubCommandsToOfficialCLI(runCtx, cfg, connection, writeFrame, ptmx, &ptyMu, cmd.Process, &stopInProgress, injected, questions)
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
	var commandErr error
	select {
	case commandErr = <-commandDone:
	case <-ctx.Done():
	}
	if stopInProgress.Load() {
		if commandErr != nil {
			return fmt.Errorf("finalize official agent stop: %w", commandErr)
		}
		// The Hub atomically appends nonterminal -> ended before accepting the
		// completed stop outcome. Do not publish a second terminal state here.
		return nil
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
func forwardHubCommandsToOfficialCLI(ctx context.Context, cfg wrapConfig, connection *hubConnection, writeFrame func(protocol.Frame) error, ptmx *os.File, ptyMu *sync.Mutex, process *os.Process, stopInProgress *atomic.Bool, injected *injectedPromptTracker, questions *questionCache) error {
	accepted := newAcceptedCommandSet(2048)
	if ptyMu == nil {
		ptyMu = &sync.Mutex{}
	}
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
			if typed.Type == protocol.CommandSessionStop {
				return stopOfficialCLI(typed, connection.read, writeFrame, cfg, process, stopInProgress)
			}
			if typed.Type == protocol.CommandPermissionRespond {
				ptyMu.Lock()
				err := forwardQuestionAnswer(ptmx, questions, typed.Payload)
				ptyMu.Unlock()
				if err != nil {
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
			ptyMu.Lock()
			err = writeOfficialCLIPrompt(ptmx, promptText)
			ptyMu.Unlock()
			if err != nil {
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
			// After a long idle the TUI may be on a recap/pager, so the first
			// write lands but is not submitted. Confirm via transcript without
			// blocking Ping handling on this loop.
			go confirmOfficialCLIPrompt(ctx, ptmx, ptyMu, typed.SessionID, promptText, injected, writeFrame, defaultOfficialPromptInjection)
		case *protocol.Error:
			return fmt.Errorf("hub error %s: %s", typed.Code, typed.Message)
		}
	}
}

// publishOfficialRunControlCapability describes the operations the interactive
// Claude/Codex wrapper can actually complete. Unlike the ACP bridge it cannot
// safely interrupt an arbitrary in-flight turn, but an explicit stop can end
// the child CLI process for the archive workflow.
func publishOfficialRunControlCapability(cfg wrapConfig, writeFrame func(protocol.Frame) error) error {
	event, err := newOfficialRunControlCapabilityEvent(cfg)
	if err != nil || event == nil {
		return err
	}
	return writeFrame(event)
}

func newOfficialRunControlCapabilityEvent(cfg wrapConfig) (*protocol.Event, error) {
	if cfg.ProtocolVersion != protocol.ProtocolVersionV2 {
		return nil, nil
	}
	proposalID, err := randomToken()
	if err != nil {
		return nil, fmt.Errorf("generate official run-control capability proposal: %w", err)
	}
	payload, err := json.Marshal(protocol.RunControlCapabilityPayload{
		SchemaVersion: 1, InterruptSupported: false, StopSupported: true,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal official run-control capability: %w", err)
	}
	return &protocol.Event{
		Type: "session.run.capabilities", SessionID: cfg.SessionID,
		Time: time.Now().UTC().UnixMilli(), Payload: payload, ProposalID: proposalID,
	}, nil
}

// stopOfficialCLI is called only after the user explicitly confirms an
// archive. Killing the immediate official CLI child is the interactive
// equivalent of the ACP supervisor stop: cmd.Wait then publishes session.ended
// and the archive caller polls that authoritative state before archiving.
func stopOfficialCLI(command *protocol.Command, readFrame func(context.Context) (protocol.Frame, error), writeFrame func(protocol.Frame) error, cfg wrapConfig, process *os.Process, stopInProgress *atomic.Bool) error {
	if process == nil {
		return errors.New("official agent process is unavailable")
	}
	err := process.Kill()
	if errors.Is(err, os.ErrProcessDone) {
		err = nil
	}
	if err == nil && stopInProgress != nil {
		stopInProgress.Store(true)
	}
	// cmd.Wait() returns as soon as Kill lands and cancels the normal provider
	// loop. The stop receipt still needs a bounded independent context so the
	// Hub sees the accepted outcome before the terminal state is published.
	receiptCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return acknowledgeRunControl(receiptCtx, command, readFrame, writeFrame, cfg, "stop", "ended", err)
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
	seen := make(map[string]struct{})
	for {
		nextOffset, err := resetTranscriptOffsetIfRewritten(path, offset)
		if err != nil {
			if os.IsNotExist(err) {
				offset = 0
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(300 * time.Millisecond):
				}
				continue
			}
			return err
		}
		offset = nextOffset
		file, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				offset = 0
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(300 * time.Millisecond):
				}
				continue
			}
			return err
		}
		if _, err := file.Seek(offset, 0); err != nil {
			file.Close()
			return err
		}
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Bytes()
			offset += int64(len(line)) + 1
			if alreadyMirroredTranscriptLine(seen, line) {
				continue
			}
			// Skip user messages that were injected from the Hub; they are already
			// recorded by the Hub itself and would otherwise be mirrored twice.
			if text := provider.transcriptUserText(line); text != "" && injected != nil && injected.consume(text) {
				continue
			}
			events, err := provider.translateLine(cfg.SessionID, line)
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

// resetTranscriptOffsetIfRewritten rewinds when Claude compact/recap replaces or
// shortens the jsonl. Seeking past the new size would make the mirror idle
// forever while the TUI keeps talking.
func resetTranscriptOffsetIfRewritten(path string, offset int64) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return offset, err
	}
	if info.Size() < offset {
		return 0, nil
	}
	return offset, nil
}

func transcriptLineID(line []byte) string {
	var raw map[string]any
	if json.Unmarshal(line, &raw) != nil {
		return strings.TrimSpace(string(line))
	}
	if id := stringFieldFromAny(raw["uuid"]); id != "" {
		return id
	}
	if id := stringFieldFromAny(raw["id"]); id != "" {
		return id
	}
	return strings.TrimSpace(string(line))
}

func alreadyMirroredTranscriptLine(seen map[string]struct{}, line []byte) bool {
	if seen == nil {
		return false
	}
	id := transcriptLineID(line)
	if id == "" {
		return false
	}
	if _, ok := seen[id]; ok {
		return true
	}
	seen[id] = struct{}{}
	return false
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

func (t *injectedPromptTracker) hasPending(text string) bool {
	if t == nil {
		return false
	}
	key := strings.TrimSpace(text)
	if key == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pending[key] > 0
}

type officialPromptInjection struct {
	firstWait time.Duration
	retryWait time.Duration
	wakePause time.Duration
}

var defaultOfficialPromptInjection = officialPromptInjection{
	firstWait: 2500 * time.Millisecond,
	retryWait: 4 * time.Second,
	wakePause: 80 * time.Millisecond,
}

// confirmOfficialCLIPrompt watches the transcript mirror for the Hub-injected
// user line. If the idle Claude TUI swallowed the first Enter (recap/pager),
// it dismisses the overlay, resubmits twice, and publishes a visible miss
// instead of leaving the workbench looking connected with no Agent turn.
func confirmOfficialCLIPrompt(ctx context.Context, ptmx io.Writer, ptyMu *sync.Mutex, sessionID, promptText string, injected *injectedPromptTracker, writeFrame func(protocol.Frame) error, timing officialPromptInjection) {
	if waitInjectedPromptGone(ctx, injected, promptText, timing.firstWait) {
		return
	}
	for range 2 {
		if ptyMu != nil {
			ptyMu.Lock()
		}
		stillPending := injected.hasPending(promptText)
		if stillPending {
			if err := wakeOfficialCLIComposer(ptmx, timing.wakePause); err != nil {
				if ptyMu != nil {
					ptyMu.Unlock()
				}
				return
			}
			if err := writeOfficialCLIPrompt(ptmx, promptText); err != nil {
				if ptyMu != nil {
					ptyMu.Unlock()
				}
				return
			}
		}
		if ptyMu != nil {
			ptyMu.Unlock()
		}
		if !stillPending || waitInjectedPromptGone(ctx, injected, promptText, timing.retryWait) {
			return
		}
	}
	_ = publishOfficialInjectionMiss(writeFrame, sessionID)
}

func copyLocked(dst io.Writer, src io.Reader, mu *sync.Mutex) error {
	if dst == nil || src == nil {
		return errors.New("official cli pty copy is unavailable")
	}
	buf := make([]byte, 32*1024)
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			if mu != nil {
				mu.Lock()
			}
			_, writeErr := dst.Write(buf[:n])
			if mu != nil {
				mu.Unlock()
			}
			if writeErr != nil {
				return writeErr
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return readErr
		}
	}
}

func writeOfficialCLIPrompt(w io.Writer, promptText string) error {
	if w == nil {
		return errors.New("official cli pty is unavailable")
	}
	_, err := io.WriteString(w, "\x1b[200~"+promptText+"\x1b[201~\r")
	return err
}

func waitInjectedPromptGone(ctx context.Context, injected *injectedPromptTracker, text string, timeout time.Duration) bool {
	if injected == nil || !injected.hasPending(text) {
		return true
	}
	if timeout <= 0 {
		return !injected.hasPending(text)
	}
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return !injected.hasPending(text)
		case <-ticker.C:
			if !injected.hasPending(text) {
				return true
			}
			if !time.Now().Before(deadline) {
				return !injected.hasPending(text)
			}
		}
	}
}

func wakeOfficialCLIComposer(w io.Writer, pause time.Duration) error {
	if w == nil {
		return errors.New("official cli pty is unavailable")
	}
	if _, err := w.Write([]byte{0x1b}); err != nil {
		return err
	}
	if pause > 0 {
		timer := time.NewTimer(pause)
		<-timer.C
	}
	if _, err := w.Write([]byte{0x15}); err != nil {
		return err
	}
	if pause > 0 {
		timer := time.NewTimer(pause)
		<-timer.C
	}
	return nil
}

func publishOfficialInjectionMiss(writeFrame func(protocol.Frame) error, sessionID string) error {
	if writeFrame == nil || sessionID == "" {
		return errors.New("official injection miss publisher is unavailable")
	}
	payload, err := json.Marshal(map[string]any{
		"message_id": "official-idle-miss",
		"role":       "agent",
		"content": []map[string]string{{
			"kind": "text",
			"text": "The local Agent terminal did not accept this instruction after idle. Click that terminal, press Enter, then resend from the workbench. 本机 Agent 终端空闲后没有收下这条指令。请点一下那个终端窗口，按一次 Enter，再从工作台重发。",
		}},
	})
	if err != nil {
		return err
	}
	return writeFrame(&protocol.Event{
		Type: "session.message", SessionID: sessionID,
		Time: time.Now().UTC().UnixMilli(), Payload: payload,
	})
}
