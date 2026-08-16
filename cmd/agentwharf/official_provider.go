package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/winghv/agentwharf/adapter/core"
	"github.com/winghv/agentwharf/protocol"
)

// runOfficialProvider launches the native claude/codex CLI in the user's
// terminal (the official interactive TUI) and mirrors its session transcript to
// the Hub. It is used only for interactive wharf claude / wharf codex.
func runOfficialProvider(ctx context.Context, cfg wrapConfig, connection *hubConnection, masker *core.EventMasker, metrics *core.AdapterMetrics) error {
	writeFrame := func(frame protocol.Frame) error { return connection.write(ctx, frame) }
	publishState := func(state string) error {
		payload, err := json.Marshal(map[string]any{"state": state, "provider": cfg.Provider})
		if err != nil {
			return err
		}
		return writeFrame(&protocol.Event{Type: "session.state", SessionID: cfg.SessionID, Time: time.Now().UTC().UnixMilli(), Payload: payload})
	}

	readerDone := make(chan error, 1)
	go func() {
		for {
			frame, err := connection.read(ctx)
			if err != nil {
				readerDone <- ignoreContextError(err)
				return
			}
			switch typed := frame.(type) {
			case *protocol.Ping:
				if err := writeFrame(&protocol.Pong{Nonce: typed.Nonce}); err != nil {
					readerDone <- err
					return
				}
			case *protocol.Error:
				readerDone <- fmt.Errorf("hub error %s: %s", typed.Code, typed.Message)
				return
			}
		}
	}()

	if err := publishState("starting"); err != nil {
		return err
	}

	sessionID := newSessionUUID()
	command := officialAgentCommand(cfg.Agent)
	args := append([]string{}, cfg.ProviderCommand[1:]...)
	args = append(args, "--session-id", sessionID)

	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Dir = cfg.WorkingDirectory
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start official agent %s: %w", command, err)
	}

	if err := publishState("ready"); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return err
	}

	mirrorDone := make(chan error, 1)
	go func() {
		mirrorDone <- mirrorTranscript(ctx, cfg, sessionID, writeFrame)
	}()

	waitErr := cmd.Wait()
	_ = publishState("ended")
	_ = ignoreContextError(waitErr)

	select {
	case <-mirrorDone:
	case <-ctx.Done():
	}
	_ = ignoreContextError(<-readerDone)
	return nil
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

func mirrorTranscript(ctx context.Context, cfg wrapConfig, sessionID string, writeFrame func(protocol.Frame) error) error {
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
