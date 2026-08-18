package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/winghv/agentwharf/protocol"
)

// claudeProvider implements agentProvider for the native claude CLI.
type claudeProvider struct{}

func (claudeProvider) command() string { return "claude" }

func (claudeProvider) sessionArgs(sessionID string) []string {
	return []string{"--session-id", sessionID}
}

func (claudeProvider) transcriptPath(_ context.Context, cfg wrapConfig, sessionID string, _ time.Time) (string, error) {
	cwd := cfg.WorkingDirectory
	if cwd == "" {
		cwd = "."
	}
	return claudeTranscriptPath(cwd, sessionID)
}

// claudeTranscriptPath returns the Claude Code transcript file for a session.
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

// translateLine maps one Claude Code transcript entry to Hub events.
func (claudeProvider) translateLine(sessionID string, line []byte) ([]protocol.Event, error) {
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
			events = append(events, transcriptMessageEvents(sessionID, entryID, role, text)...)
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
				events = append(events, transcriptMessageEvents(sessionID, entryID, role, text)...)
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

// launchSettings parses Claude Code's --model / --permission-mode /
// --reasoning-effort flags.
func (claudeProvider) launchSettings(args []string) launchSettings {
	var settings launchSettings
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
			settings.model = nextValue()
		case "--permission-mode":
			settings.permission = nextValue()
		case "--reasoning-effort", "--effort":
			settings.reasoning = nextValue()
		}
	}
	return settings
}

// transcriptUserText returns the plain-text body of a Claude user entry.
func (claudeProvider) transcriptUserText(line []byte) string {
	var raw map[string]any
	if json.Unmarshal(line, &raw) != nil {
		return ""
	}
	if stringFieldFromAny(raw["type"]) != "user" {
		return ""
	}
	message, _ := raw["message"].(map[string]any)
	if message == nil || stringFieldFromAny(message["role"]) != "user" {
		return ""
	}
	text, _ := message["content"].(string)
	return text
}
