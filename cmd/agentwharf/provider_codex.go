package main

import (
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/winghv/agentwharf/protocol"
)

// codexProvider implements agentProvider for the native codex CLI.
//
// Codex differs from Claude in every provider-specific detail: it has no
// --session-id flag, stores rollouts under a date-keyed directory
// (~/.codex/sessions/YYYY/MM/DD/rollout-<timestamp>-<uuid>.jsonl), and writes a
// different transcript shape (session_meta / event_msg / response_item).
type codexProvider struct{}

func (codexProvider) command() string { return "codex" }

func (codexProvider) sessionArgs(string) []string { return nil }

func (codexProvider) transcriptPath(ctx context.Context, _ wrapConfig, _ string, launchTime time.Time) (string, error) {
	dir, err := codexSessionsDir()
	if err != nil {
		return "", err
	}
	for {
		path, found, err := newestCodexRollout(dir, launchTime)
		if err != nil {
			return "", err
		}
		if found {
			return path, nil
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

func codexSessionsDir() (string, error) {
	codexHome := os.Getenv("CODEX_HOME")
	if codexHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		codexHome = filepath.Join(home, ".codex")
	}
	return filepath.Join(codexHome, "sessions"), nil
}

// newestCodexRollout returns the most recently modified rollout transcript at or
// after the launch time, ignoring read errors so a partially-written rollout or
// an unreadable subdirectory does not derail discovery.
func newestCodexRollout(dir string, after time.Time) (string, bool, error) {
	var newest string
	var newestTime time.Time
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".jsonl") {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return nil
		}
		if info.ModTime().Before(after) {
			return nil
		}
		if info.ModTime().After(newestTime) {
			newestTime = info.ModTime()
			newest = path
		}
		return nil
	})
	if err != nil {
		return "", false, err
	}
	if newest == "" {
		return "", false, nil
	}
	return newest, true, nil
}

// translateLine maps one Codex transcript line to Hub events.
func (codexProvider) translateLine(sessionID string, line []byte) ([]protocol.Event, error) {
	var raw map[string]any
	if err := json.Unmarshal(line, &raw); err != nil {
		return nil, err
	}
	payload, _ := raw["payload"].(map[string]any)
	if payload == nil {
		return nil, nil
	}
	switch stringFieldFromAny(raw["type"]) {
	case "event_msg":
		return translateCodexEventMsg(sessionID, payload)
	case "response_item":
		return translateCodexResponseItem(sessionID, payload)
	default:
		return nil, nil
	}
}

func translateCodexEventMsg(sessionID string, payload map[string]any) ([]protocol.Event, error) {
	switch stringFieldFromAny(payload["type"]) {
	case "user_message":
		if text := stringFieldFromAny(payload["message"]); text != "" {
			return transcriptMessageEvents(sessionID, "", "user", text), nil
		}
	case "agent_message":
		// Commentary is the agent's internal status narration, not its reply.
		if stringFieldFromAny(payload["phase"]) == "commentary" {
			return nil, nil
		}
		if text := stringFieldFromAny(payload["message"]); text != "" {
			return transcriptMessageEvents(sessionID, "", "agent", text), nil
		}
	case "item_completed":
		// Newer Codex releases wrap user/agent messages in item_completed
		// instead of emitting the legacy user_message/agent_message payloads.
		item, _ := payload["item"].(map[string]any)
		text := codexItemText(item)
		switch stringFieldFromAny(item["type"]) {
		case "UserMessage":
			if text != "" {
				return transcriptMessageEvents(sessionID, "", "user", text), nil
			}
		case "AgentMessage":
			if text != "" {
				return transcriptMessageEvents(sessionID, "", "agent", text), nil
			}
		}
	}
	return nil, nil
}

func codexItemText(item map[string]any) string {
	if item == nil {
		return ""
	}
	if text := stringFieldFromAny(item["text"]); text != "" {
		return text
	}
	content, _ := item["content"].([]any)
	var parts []string
	for _, entry := range content {
		block, _ := entry.(map[string]any)
		if text := stringFieldFromAny(block["text"]); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "")
}

func translateCodexResponseItem(sessionID string, payload map[string]any) ([]protocol.Event, error) {
	switch stringFieldFromAny(payload["type"]) {
	case "function_call":
		name := stringFieldFromAny(payload["name"])
		callID := stringFieldFromAny(payload["call_id"])
		input := json.RawMessage(stringFieldFromAny(payload["arguments"]))
		return []protocol.Event{transcriptToolCallEvent(sessionID, callID, "start", name, input)}, nil
	}
	return nil, nil
}

// launchSettings parses Codex's -m/--model, -a/--ask-for-approval, -s/--sandbox
// and -c reasoning_effort= flags.
func (codexProvider) launchSettings(args []string) launchSettings {
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
		case "--ask-for-approval", "-a":
			settings.permission = nextValue()
		case "--sandbox", "-s":
			settings.permission = nextValue()
		case "--reasoning-effort":
			settings.reasoning = nextValue()
		case "-c":
			raw := nextValue()
			switch {
			case strings.HasPrefix(raw, "reasoning_effort="):
				settings.reasoning = strings.TrimPrefix(raw, "reasoning_effort=")
			case strings.HasPrefix(raw, "approval_policy="):
				settings.permission = strings.TrimPrefix(raw, "approval_policy=")
			}
		}
	}
	return settings
}

// transcriptUserText returns the plain-text body of a Codex user message.
func (codexProvider) transcriptUserText(line []byte) string {
	var raw map[string]any
	if json.Unmarshal(line, &raw) != nil {
		return ""
	}
	if stringFieldFromAny(raw["type"]) != "event_msg" {
		return ""
	}
	payload, _ := raw["payload"].(map[string]any)
	if payload == nil {
		return ""
	}
	switch stringFieldFromAny(payload["type"]) {
	case "user_message":
		return stringFieldFromAny(payload["message"])
	case "item_completed":
		item, _ := payload["item"].(map[string]any)
		if stringFieldFromAny(item["type"]) == "UserMessage" {
			return codexItemText(item)
		}
	}
	return ""
}
