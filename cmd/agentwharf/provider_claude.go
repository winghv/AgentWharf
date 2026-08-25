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
	if entryType == "system" && stringFieldFromAny(raw["subtype"]) == "turn_duration" {
		return []protocol.Event{claudeTurnCompletedEvent(sessionID, raw)}, nil
	}
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
			if isAskUserQuestionTool(name) {
				events = append(events, transcriptQuestionPermissionRequest(sessionID, id, input))
			}
		case "tool_result":
			toolUseID := stringFieldFromAny(block["tool_use_id"])
			if toolUseID == "" {
				continue
			}
			content, isError := toolResultContent(block)
			events = append(events, transcriptToolResultEvent(sessionID, toolUseID, "", content, isError))
		}
	}
	return events, nil
}

func claudeTurnCompletedEvent(sessionID string, raw map[string]any) protocol.Event {
	payload := map[string]any{
		"kind":    "turn_completed",
		"turn_id": stringFieldFromAny(raw["uuid"]),
	}
	if providerSessionID := stringFieldFromAny(raw["sessionId"]); providerSessionID != "" {
		payload["provider_session_id"] = providerSessionID
	}
	if durationMS, ok := raw["durationMs"].(float64); ok && durationMS >= 0 {
		payload["duration_ms"] = durationMS
	}
	encoded, _ := json.Marshal(payload)
	return protocol.Event{
		Type:      "agent.activity",
		SessionID: sessionID,
		Time:      time.Now().UTC().UnixMilli(),
		Payload:   encoded,
	}
}

// toolResultContent extracts the text content and error flag from a claude
// transcript tool_result block. Content may be a plain string or an array of
// content blocks.
func toolResultContent(block map[string]any) ([]byte, bool) {
	isError := block["is_error"] == true
	switch content := block["content"].(type) {
	case string:
		return []byte(content), isError
	case []any:
		var parts []string
		for _, item := range content {
			if textBlock, ok := item.(map[string]any); ok {
				if text, ok := textBlock["text"].(string); ok && text != "" {
					parts = append(parts, text)
				}
			}
		}
		return []byte(strings.Join(parts, "\n")), isError
	default:
		encoded, _ := json.Marshal(block["content"])
		return encoded, isError
	}
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
	switch content := message["content"].(type) {
	case string:
		return content
	case []any:
		var text strings.Builder
		for _, item := range content {
			block, _ := item.(map[string]any)
			if block != nil && stringFieldFromAny(block["type"]) == "text" {
				text.WriteString(stringFieldFromAny(block["text"]))
			}
		}
		return text.String()
	default:
		return ""
	}
}
