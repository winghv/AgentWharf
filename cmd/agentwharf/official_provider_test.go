package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/protocol"
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
	events, err := claudeProvider{}.translateLine("ses_1", line)
	if err != nil {
		t.Fatalf("translateLine() error = %v", err)
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

func TestTranslateTranscriptSplitsLargeMessage(t *testing.T) {
	text := strings.Repeat("大输出🙂 ", 20000)
	line := transcriptJSON(t, map[string]any{
		"type": "assistant", "uuid": "a-large",
		"message": map[string]any{"role": "assistant", "content": text},
	})
	events, err := claudeProvider{}.translateLine("ses_1", line)
	if err != nil {
		t.Fatalf("translateLine() error = %v", err)
	}
	if len(events) < 2 {
		t.Fatalf("events = %d, want split output", len(events))
	}
	var rebuilt strings.Builder
	for _, event := range events {
		if event.Type != "session.message" || len(event.Payload) > protocol.MaxEventPayloadBytes {
			t.Fatalf("event = type:%s payload_bytes:%d", event.Type, len(event.Payload))
		}
		var payload map[string]any
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		content := payload["content"].([]any)
		rebuilt.WriteString(content[0].(map[string]any)["text"].(string))
	}
	if rebuilt.String() != text {
		t.Fatalf("rebuilt text differs: got %d bytes, want %d", rebuilt.Len(), len(text))
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
	events, err := claudeProvider{}.translateLine("ses_1", line)
	if err != nil {
		t.Fatalf("translateLine() error = %v", err)
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
		events, err := claudeProvider{}.translateLine("ses_1", transcriptJSON(t, value))
		if err != nil || len(events) != 0 {
			t.Fatalf("translateLine(%v) = %+v, %v", value, events, err)
		}
	}
}

func TestOfficialProviderForAgent(t *testing.T) {
	if got := officialProviderForAgent("claude").command(); got != "claude" {
		t.Fatalf("officialProviderForAgent(claude).command() = %q", got)
	}
	if got := officialProviderForAgent("claude-code").command(); got != "claude" {
		t.Fatalf("officialProviderForAgent(claude-code).command() = %q", got)
	}
	if got := officialProviderForAgent("codex").command(); got != "codex" {
		t.Fatalf("officialProviderForAgent(codex).command() = %q", got)
	}
	if got := officialProviderForAgent("gemini").command(); got != "gemini" {
		t.Fatalf("officialProviderForAgent(gemini).command() = %q", got)
	}
	if got := officialProviderForAgent("unknown").command(); got != "unknown" {
		t.Fatalf("officialProviderForAgent(unknown).command() = %q", got)
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

func TestClaudeLaunchSettings(t *testing.T) {
	settings := claudeProvider{}.launchSettings([]string{
		"--model", "sonnet", "--permission-mode", "acceptEdits", "--reasoning-effort", "high",
	})
	if settings.model != "sonnet" || settings.permission != "acceptEdits" || settings.reasoning != "high" {
		t.Fatalf("parsed = %+v", settings)
	}

	settings = claudeProvider{}.launchSettings([]string{"--model=opus", "--permission-mode=default"})
	if settings.model != "opus" || settings.permission != "default" || settings.reasoning != "" {
		t.Fatalf("parsed = %+v", settings)
	}
}

func TestCodexLaunchSettings(t *testing.T) {
	settings := codexProvider{}.launchSettings([]string{
		"-m", "gpt-5", "-a", "never", "-c", "reasoning_effort=high",
	})
	if settings.model != "gpt-5" || settings.permission != "never" || settings.reasoning != "high" {
		t.Fatalf("parsed = %+v", settings)
	}

	settings = codexProvider{}.launchSettings([]string{"-s", "read-only", "-c", "approval_policy=on-request"})
	if settings.permission != "on-request" {
		t.Fatalf("parsed permission = %+v", settings)
	}
}

func TestPublishOfficialSettingsCapability(t *testing.T) {
	var frames []protocol.Event
	write := func(frame protocol.Frame) error {
		if event, ok := frame.(*protocol.Event); ok {
			frames = append(frames, *event)
		}
		return nil
	}
	settings := launchSettings{model: "sonnet", permission: "acceptEdits"}
	if err := publishOfficialSettingsCapability(write, "ses_1", settings); err != nil {
		t.Fatalf("publishOfficialSettingsCapability() error = %v", err)
	}
	if len(frames) != 1 || frames[0].Type != "session.settings.capabilities" {
		t.Fatalf("frames = %+v", frames)
	}
	if _, err := protocol.DecodeSettingsCapabilityPayload(frames[0].Payload); err != nil {
		t.Fatalf("capability payload is not canonical: %v", err)
	}
	var wire struct {
		ReasoningEfforts []json.RawMessage `json:"reasoning_efforts"`
	}
	if err := json.Unmarshal(frames[0].Payload, &wire); err != nil {
		t.Fatalf("decode capability wire payload: %v", err)
	}
	if wire.ReasoningEfforts == nil {
		t.Fatalf("reasoning_efforts = null, want []")
	}
}

func TestInjectedPromptTrackerDedupes(t *testing.T) {
	tracker := &injectedPromptTracker{pending: make(map[string]int)}
	tracker.add("hello")
	if !tracker.consume("hello") {
		t.Fatal("consume(hello) = false, want true")
	}
	if tracker.consume("hello") {
		t.Fatal("second consume(hello) = true, want false")
	}
	if tracker.consume("other") {
		t.Fatal("consume(other) = true, want false")
	}
}

func TestTranscriptUserText(t *testing.T) {
	line := transcriptJSON(t, map[string]any{
		"type": "user", "uuid": "u1",
		"message": map[string]any{"role": "user", "content": "hello"},
	})
	if got := (claudeProvider{}).transcriptUserText(line); got != "hello" {
		t.Fatalf("transcriptUserText = %q, want hello", got)
	}
	assistant := transcriptJSON(t, map[string]any{
		"type": "assistant", "uuid": "a1",
		"message": map[string]any{"role": "assistant", "content": "hi"},
	})
	if got := (claudeProvider{}).transcriptUserText(assistant); got != "" {
		t.Fatalf("transcriptUserText(assistant) = %q, want empty", got)
	}
	blocks := transcriptJSON(t, map[string]any{
		"type": "user", "uuid": "u2",
		"message": map[string]any{"role": "user", "content": []any{
			map[string]any{"type": "text", "text": "modern "},
			map[string]any{"type": "image", "source": "ignored"},
			map[string]any{"type": "text", "text": "prompt"},
		}},
	})
	if got := (claudeProvider{}).transcriptUserText(blocks); got != "modern prompt" {
		t.Fatalf("transcriptUserText(blocks) = %q, want modern prompt", got)
	}
}

func TestCodexTranslateLine(t *testing.T) {
	user := transcriptJSON(t, map[string]any{
		"timestamp": "2025-01-01T00:00:00Z",
		"type":      "event_msg",
		"payload":   map[string]any{"type": "user_message", "message": "hello"},
	})
	events, err := codexProvider{}.translateLine("ses_1", user)
	if err != nil || len(events) != 1 || events[0].Type != "session.message" {
		t.Fatalf("user events = %+v, %v", events, err)
	}

	currentUser := transcriptJSON(t, map[string]any{
		"type": "event_msg",
		"payload": map[string]any{"type": "item_completed", "item": map[string]any{
			"type":    "UserMessage",
			"content": []any{map[string]any{"type": "text", "text": "current hello"}},
		}},
	})
	events, err = codexProvider{}.translateLine("ses_1", currentUser)
	if err != nil || len(events) != 1 || events[0].Type != "session.message" {
		t.Fatalf("current user events = %+v, %v", events, err)
	}
	if got := (codexProvider{}).transcriptUserText(currentUser); got != "current hello" {
		t.Fatalf("current codex transcriptUserText = %q", got)
	}

	currentAgent := transcriptJSON(t, map[string]any{
		"type": "event_msg",
		"payload": map[string]any{"type": "item_completed", "item": map[string]any{
			"type":    "AgentMessage",
			"content": []any{map[string]any{"type": "Text", "text": "current done"}},
		}},
	})
	events, err = codexProvider{}.translateLine("ses_1", currentAgent)
	if err != nil || len(events) != 1 || events[0].Type != "session.message" {
		t.Fatalf("current agent events = %+v, %v", events, err)
	}

	commentary := transcriptJSON(t, map[string]any{
		"type":    "event_msg",
		"payload": map[string]any{"type": "agent_message", "phase": "commentary", "message": "thinking"},
	})
	events, err = codexProvider{}.translateLine("ses_1", commentary)
	if err != nil || len(events) != 0 {
		t.Fatalf("commentary events = %+v, %v", events, err)
	}

	agent := transcriptJSON(t, map[string]any{
		"type":    "event_msg",
		"payload": map[string]any{"type": "agent_message", "message": "done"},
	})
	events, err = codexProvider{}.translateLine("ses_1", agent)
	if err != nil || len(events) != 1 || events[0].Type != "session.message" {
		t.Fatalf("agent events = %+v, %v", events, err)
	}

	tool := transcriptJSON(t, map[string]any{
		"type":    "response_item",
		"payload": map[string]any{"type": "function_call", "name": "shell", "call_id": "c1", "arguments": "{}"},
	})
	events, err = codexProvider{}.translateLine("ses_1", tool)
	if err != nil || len(events) != 1 || events[0].Type != "session.tool_call" {
		t.Fatalf("tool events = %+v, %v", events, err)
	}
}

func TestCodexTranscriptUserText(t *testing.T) {
	user := transcriptJSON(t, map[string]any{
		"type":    "event_msg",
		"payload": map[string]any{"type": "user_message", "message": "hello"},
	})
	if got := (codexProvider{}).transcriptUserText(user); got != "hello" {
		t.Fatalf("codex transcriptUserText = %q, want hello", got)
	}
	sessionMeta := transcriptJSON(t, map[string]any{
		"type":    "session_meta",
		"payload": map[string]any{"session_id": "s1"},
	})
	if got := (codexProvider{}).transcriptUserText(sessionMeta); got != "" {
		t.Fatalf("codex transcriptUserText(session_meta) = %q, want empty", got)
	}
}

func TestNewestCodexRollout(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "2025", "01", "02")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	old := filepath.Join(sub, "rollout-old.jsonl")
	recent := filepath.Join(sub, "rollout-recent.jsonl")
	for _, p := range []string{old, recent} {
		if err := os.WriteFile(p, []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	base := time.Now().Add(-time.Hour)
	if err := os.Chtimes(old, base, base); err != nil {
		t.Fatal(err)
	}
	recentTime := base.Add(30 * time.Minute)
	if err := os.Chtimes(recent, recentTime, recentTime); err != nil {
		t.Fatal(err)
	}
	got, found, err := newestCodexRollout(dir, base.Add(-time.Minute))
	if err != nil || !found {
		t.Fatalf("newestCodexRollout = %q, %t, %v", got, found, err)
	}
	if got != recent {
		t.Fatalf("got %q, want %q", got, recent)
	}
}
