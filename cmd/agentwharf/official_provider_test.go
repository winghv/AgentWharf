package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func transcriptJSON(t *testing.T, value map[string]any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal transcript: %v", err)
	}
	return encoded
}

func TestTranslateTranscriptUserMessage(t *testing.T) {
	line := transcriptJSON(t, map[string]any{
		"type": "user", "uuid": "u1",
		"message": map[string]any{"role": "user", "content": "hello"},
	})
	events, err := translateTranscriptLine("ses_1", line)
	if err != nil {
		t.Fatalf("translateTranscriptLine() error = %v", err)
	}
	if len(events) != 1 || events[0].Type != "session.message" {
		t.Fatalf("events = %+v", events)
	}
	var payload map[string]any
	if err := json.Unmarshal(events[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	content := payload["content"].([]any)
	block := content[0].(map[string]any)
	if block["kind"] != "text" || block["text"] != "hello" {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestTranslateTranscriptAssistantToolCall(t *testing.T) {
	line := transcriptJSON(t, map[string]any{
		"type": "assistant", "uuid": "a1",
		"message": map[string]any{"role": "assistant", "content": []any{
			map[string]any{"type": "text", "text": "running"},
			map[string]any{"type": "tool_use", "id": "t1", "name": "Bash", "input": map[string]any{"cmd": "ls"}},
		}},
	})
	events, err := translateTranscriptLine("ses_1", line)
	if err != nil {
		t.Fatalf("translateTranscriptLine() error = %v", err)
	}
	var sawMessage, sawTool bool
	for _, event := range events {
		switch event.Type {
		case "session.message":
			sawMessage = true
		case "session.tool_call":
			sawTool = true
			var payload map[string]any
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if payload["name"] != "Bash" {
				t.Fatalf("tool payload = %+v", payload)
			}
		}
	}
	if !sawMessage || !sawTool {
		t.Fatalf("events = %+v, sawMessage=%t sawTool=%t", events, sawMessage, sawTool)
	}
}

func TestTranslateTranscriptSkipsInternalEntries(t *testing.T) {
	for _, value := range []map[string]any{
		{"type": "summary", "summary": "x", "leafUuid": "u1"},
		{"type": "system", "uuid": "u1"},
	} {
		events, err := translateTranscriptLine("ses_1", transcriptJSON(t, value))
		if err != nil || len(events) != 0 {
			t.Fatalf("translateTranscriptLine(%v) = %+v, %v", value, events, err)
		}
	}
}

func TestOfficialAgentCommand(t *testing.T) {
	if got := officialAgentCommand("claude"); got != "claude" {
		t.Fatalf("officialAgentCommand(claude) = %q", got)
	}
	if got := officialAgentCommand("claude-code"); got != "claude" {
		t.Fatalf("officialAgentCommand(claude-code) = %q", got)
	}
	if got := officialAgentCommand("codex"); got != "codex" {
		t.Fatalf("officialAgentCommand(codex) = %q", got)
	}
}

func TestClaudeTranscriptPathUsesProjectSlug(t *testing.T) {
	path, err := claudeTranscriptPath("/Users/me/proj", "ses-123")
	if err != nil {
		t.Fatalf("claudeTranscriptPath() error = %v", err)
	}
	if !strings.HasSuffix(path, "ses-123.jsonl") {
		t.Fatalf("path = %q", path)
	}
	if !strings.Contains(path, "-Users-me-proj") {
		t.Fatalf("path = %q, want project slug", path)
	}
}
