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

const (
	maxClaudeDiffTextBytes  = 4 * 1024
	maxClaudeDiffTotalBytes = 16 * 1024
	maxClaudeDiffs          = 8
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
	switch entryType {
	case "summary":
		return []protocol.Event{claudeCompactionEvent(sessionID, raw)}, nil
	case "system":
		subtype := stringFieldFromAny(raw["subtype"])
		if subtype == "turn_duration" {
			return []protocol.Event{claudeTurnCompletedEvent(sessionID, raw)}, nil
		}
		if subtype == "compact_boundary" || subtype == "status" {
			return []protocol.Event{claudeCompactionEvent(sessionID, raw)}, nil
		}
	}
	if entryType != "user" && entryType != "assistant" {
		return nil, nil
	}
	entryID := stringFieldFromAny(raw["uuid"])
	message, _ := raw["message"].(map[string]any)
	if message == nil {
		return nil, nil
	}
	role := canonicalTranscriptRole(stringFieldFromAny(message["role"]))
	content := message["content"]
	extra := claudeMessageExtra(raw, message)

	events := make([]protocol.Event, 0, 4)
	if text, ok := content.(string); ok {
		if text != "" {
			events = append(events, transcriptMessageEventsWithExtra(sessionID, entryID, role, text, extra)...)
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
				events = append(events, transcriptMessageEventsWithExtra(sessionID, entryID, role, text, extra)...)
			}
		case "thinking":
			if text := stringFieldFromAny(block["thinking"]); text != "" {
				events = append(events, claudeThinkingEvents(sessionID, entryID, text, extra)...)
			}
		case "tool_use":
			id := stringFieldFromAny(block["id"])
			name := stringFieldFromAny(block["name"])
			inputMap, _ := block["input"].(map[string]any)
			input, _ := json.Marshal(block["input"])
			toolExtra := map[string]any{}
			for key, value := range extra {
				toolExtra[key] = value
			}
			if diffs := claudeToolDiffs(name, inputMap); len(diffs) > 0 {
				toolExtra["diffs"] = diffs
			}
			events = append(events, transcriptToolCallEventWithExtra(sessionID, id, "start", name, input, toolExtra))
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

// claudeMessageExtra carries the evidence the transcript records on the
// assistant row itself: which model answered, how many tokens it used, and
// whether the row belongs to a sidechain (a subagent).
func claudeMessageExtra(raw, message map[string]any) map[string]any {
	extra := map[string]any{}
	if model := stringFieldFromAny(message["model"]); model != "" {
		extra["model"] = model
	}
	if usage, ok := message["usage"].(map[string]any); ok {
		trimmed := map[string]any{}
		for _, key := range []string{"input_tokens", "output_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"} {
			if value, ok := usage[key].(float64); ok && value >= 0 {
				trimmed[key] = int64(value)
			}
		}
		if len(trimmed) > 0 {
			extra["usage"] = trimmed
		}
	}
	if sidechain, _ := raw["isSidechain"].(bool); sidechain {
		extra["is_sidechain"] = true
	}
	if parent := stringFieldFromAny(raw["parent_tool_use_id"]); parent != "" {
		extra["parent_tool_call_id"] = parent
	}
	return extra
}

func claudeThinkingEvents(sessionID, turnID, text string, extra map[string]any) []protocol.Event {
	return splitTranscriptMessageEvents(text, encryptedEventPayloadBudget-2*1024, func(part string) protocol.Event {
		payload := map[string]any{
			"kind":                "thinking",
			"text":                part,
			"provider_session_id": turnID,
		}
		for key, value := range extra {
			if value != nil {
				payload[key] = value
			}
		}
		encoded, _ := json.Marshal(payload)
		return protocol.Event{Type: "agent.activity", SessionID: sessionID, Time: time.Now().UTC().UnixMilli(), Payload: encoded}
	})
}

// claudeCompactionEvent preserves the boundary/summary as live activity. The
// provider owns the compaction semantics; the workbench only says what
// happened and whether it is running, done or failed.
func claudeCompactionEvent(sessionID string, raw map[string]any) protocol.Event {
	phase := "done"
	text := "Conversation compacted"
	if stringFieldFromAny(raw["type"]) == "summary" {
		text = stringFieldFromAny(raw["summary"])
	} else {
		subtype := stringFieldFromAny(raw["subtype"])
		status := stringFieldFromAny(raw["status"])
		if subtype == "status" && status == "compacting" {
			phase = "running"
			text = "Compacting conversation…"
		} else if stringFieldFromAny(raw["compact_result"]) == "failed" {
			phase = "failed"
			text = "Compaction failed"
			if reason := stringFieldFromAny(raw["compact_error"]); reason != "" {
				text += ": " + reason
			}
		}
	}
	payload := map[string]any{
		"kind":  "compaction",
		"phase": phase,
		"text":  text,
	}
	if metadata, ok := raw["compact_metadata"].(map[string]any); ok {
		for _, key := range []string{"trigger", "pre_tokens"} {
			if value, ok := metadata[key]; ok {
				payload[key] = value
			}
		}
	}
	encoded, _ := json.Marshal(payload)
	return protocol.Event{Type: "agent.activity", SessionID: sessionID, Time: time.Now().UTC().UnixMilli(), Payload: encoded}
}

// claudeToolDiffs reads the change an Edit/Write applied. Only the reduced
// base name crosses the Adapter boundary; the provider's absolute host path
// stays in the input frame and never enters a durable diff.
func claudeToolDiffs(name string, input map[string]any) []map[string]any {
	if input == nil {
		return nil
	}
	lower := strings.ToLower(name)
	if lower != "edit" && lower != "write" && lower != "multi_edit" && lower != "multiedit" {
		return nil
	}
	path := filepath.Base(stringFieldFromAny(input["file_path"]))
	if path == "" || path == "." {
		return nil
	}
	if lower == "multi_edit" || lower == "multiedit" {
		edits, _ := input["edits"].([]any)
		diffs := make([]map[string]any, 0, minInt(len(edits), maxClaudeDiffs))
		budget := maxClaudeDiffTotalBytes
		for _, item := range edits {
			edit, _ := item.(map[string]any)
			diff := claudeDiffEntry(path, stringFieldFromAny(edit["old_string"]), stringFieldFromAny(edit["new_string"]))
			if diff == nil {
				continue
			}
			size := len(stringFieldFromAny(diff["old_text"])) + len(stringFieldFromAny(diff["new_text"]))
			if size > budget {
				break
			}
			budget -= size
			diffs = append(diffs, diff)
			if len(diffs) >= maxClaudeDiffs {
				break
			}
		}
		if len(diffs) > 0 {
			return diffs
		}
		return nil
	}
	oldText := ""
	newText := stringFieldFromAny(input["content"])
	if lower == "edit" {
		oldText = stringFieldFromAny(input["old_string"])
		newText = stringFieldFromAny(input["new_string"])
	}
	if diff := claudeDiffEntry(path, oldText, newText); diff != nil {
		return []map[string]any{diff}
	}
	return nil
}

func claudeDiffEntry(path, oldText, newText string) map[string]any {
	oldPreview, oldTruncated := boundedClaudeDiffText(oldText)
	newPreview, newTruncated := boundedClaudeDiffText(newText)
	if oldPreview == "" && newPreview == "" {
		return nil
	}
	diff := map[string]any{"path_base": path}
	if oldPreview != "" {
		diff["old_text"] = oldPreview
	}
	if newPreview != "" {
		diff["new_text"] = newPreview
	}
	if oldTruncated {
		diff["old_text_truncated"] = true
	}
	if newTruncated {
		diff["new_text_truncated"] = true
	}
	return diff
}

func boundedClaudeDiffText(text string) (string, bool) {
	text = strings.TrimRight(text, "\n")
	if len(text) <= maxClaudeDiffTextBytes {
		return text, false
	}
	return text[:transcriptUTF8PrefixEnd(text, maxClaudeDiffTextBytes)], true
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
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

// canonicalTranscriptRole maps a Provider transcript role to the Hub's
// session.message role vocabulary. The encrypted public projection accepts only
// user, agent, and system, and the Provider CLI calls its own replies
// "assistant".
func canonicalTranscriptRole(role string) string {
	if role == "assistant" {
		return "agent"
	}
	return role
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
