package jsonstream_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/adapter/fallback/jsonstream"
	"github.com/winghv/agentwharf/protocol"
)

func TestTranslatorMapsClaudeStreamJSONEvents(t *testing.T) {
	t.Parallel()

	translator, err := jsonstream.NewTranslator(jsonstream.Config{
		SessionID: "ses_1",
		Provider:  "claude-code",
		Now: func() time.Time {
			return time.UnixMilli(1700000000123)
		},
	})
	if err != nil {
		t.Fatalf("NewTranslator() error = %v", err)
	}

	input := strings.NewReader(strings.Join([]string{
		`{"type":"system","subtype":"init","session_id":"provider_ses","model":"claude-fable-5","permissionMode":"default","apiKeySource":"none","claude_code_version":"2.1.91"}`,
		`{"type":"system","subtype":"api_retry","attempt":1,"max_retries":10,"retry_delay_ms":60000,"error_status":502,"error":"server_error","session_id":"provider_ses"}`,
		`{"type":"assistant","message":{"id":"msg_1","content":[{"type":"thinking","thinking":"checking"},{"type":"text","text":"pong"},{"type":"tool_use","id":"tool_1","name":"bash","input":{"command":"pwd"}}]}}`,
		`{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"tool_1","content":"ok"}]}}`,
		`{"type":"result","subtype":"success","terminal_reason":"completed","session_id":"provider_ses"}`,
	}, "\n"))

	events, err := translator.TranslateReader(context.Background(), input)
	if err != nil {
		t.Fatalf("TranslateReader() error = %v", err)
	}
	if len(events) != 7 {
		t.Fatalf("events = %d, want 7: %+v", len(events), events)
	}

	assertEvent(t, events[0], "session.state", "ses_1", false)
	state := payloadMap(t, events[0])
	if state["state"] != "ready" || state["provider"] != "claude-code" || state["provider_session_id"] != "provider_ses" {
		t.Fatalf("state payload = %+v", state)
	}

	assertEvent(t, events[1], "agent.activity", "ses_1", false)
	retry := payloadMap(t, events[1])
	if retry["kind"] != "api_retry" || retry["error"] != "server_error" || retry["error_status"].(float64) != 502 {
		t.Fatalf("retry payload = %+v", retry)
	}

	assertEvent(t, events[2], "agent.activity", "ses_1", false)
	thinking := payloadMap(t, events[2])
	if thinking["kind"] != "thinking" || thinking["text"] != "checking" {
		t.Fatalf("thinking payload = %+v", thinking)
	}

	assertEvent(t, events[3], "session.message", "ses_1", false)
	message := payloadMap(t, events[3])
	if message["message_id"] != "msg_1" || message["role"] != "agent" {
		t.Fatalf("message payload = %+v", message)
	}
	content := message["content"].([]any)
	if len(content) != 1 || content[0].(map[string]any)["kind"] != "text" || content[0].(map[string]any)["text"] != "pong" {
		t.Fatalf("message content = %+v", content)
	}

	assertEvent(t, events[4], "session.tool_call", "ses_1", false)
	toolStart := payloadMap(t, events[4])
	if toolStart["tool_call_id"] != "tool_1" || toolStart["phase"] != "start" || toolStart["name"] != "bash" {
		t.Fatalf("tool start payload = %+v", toolStart)
	}

	assertEvent(t, events[5], "session.tool_call", "ses_1", false)
	toolResult := payloadMap(t, events[5])
	if toolResult["tool_call_id"] != "tool_1" || toolResult["phase"] != "result" {
		t.Fatalf("tool result payload = %+v", toolResult)
	}
	result := toolResult["result"].(map[string]any)
	if result["status"] != "ok" || result["output_preview"] != "ok" {
		t.Fatalf("tool result detail = %+v", result)
	}

	assertEvent(t, events[6], "session.state", "ses_1", false)
	done := payloadMap(t, events[6])
	if done["state"] != "ready" || done["reason"] != "completed" {
		t.Fatalf("done payload = %+v", done)
	}
}

func TestTranslatorIgnoresUnknownAndBlankLines(t *testing.T) {
	t.Parallel()

	translator, err := jsonstream.NewTranslator(jsonstream.Config{SessionID: "ses_1", Provider: "claude-code"})
	if err != nil {
		t.Fatalf("NewTranslator() error = %v", err)
	}

	events, err := translator.TranslateReader(context.Background(), strings.NewReader("\n{\"type\":\"unknown\"}\n"))
	if err != nil {
		t.Fatalf("TranslateReader() error = %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("events = %+v, want none", events)
	}
}

func TestTranslatorTruncatesLargeToolResults(t *testing.T) {
	t.Parallel()

	translator, err := jsonstream.NewTranslator(jsonstream.Config{SessionID: "ses_1", Provider: "claude-code"})
	if err != nil {
		t.Fatalf("NewTranslator() error = %v", err)
	}
	largeOutput := strings.Repeat("x", 5000)
	encoded, err := json.Marshal(map[string]any{
		"type": "user",
		"message": map[string]any{
			"content": []map[string]any{{
				"type":        "tool_result",
				"tool_use_id": "tool_1",
				"content":     largeOutput,
			}},
		},
	})
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}

	events, err := translator.TranslateLine(encoded)
	if err != nil {
		t.Fatalf("TranslateLine() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %+v, want one tool result", events)
	}
	payload := payloadMap(t, events[0])
	result := payload["result"].(map[string]any)
	if result["truncated"] != true {
		t.Fatalf("tool result = %+v, want truncated", result)
	}
	if got := len(result["output_preview"].(string)); got != 4096 {
		t.Fatalf("output_preview length = %d, want 4096", got)
	}
}

func TestTranslatorRejectsInvalidConfigAndJSON(t *testing.T) {
	t.Parallel()

	if _, err := jsonstream.NewTranslator(jsonstream.Config{}); !errors.Is(err, jsonstream.ErrInvalidConfig) {
		t.Fatalf("NewTranslator(empty) error = %v, want ErrInvalidConfig", err)
	}
	translator, err := jsonstream.NewTranslator(jsonstream.Config{SessionID: "ses_1", Provider: "claude-code"})
	if err != nil {
		t.Fatalf("NewTranslator() error = %v", err)
	}
	if _, err := translator.TranslateLine([]byte(`{"type":`)); !errors.Is(err, jsonstream.ErrInvalidStreamEvent) {
		t.Fatalf("TranslateLine(invalid) error = %v, want ErrInvalidStreamEvent", err)
	}
}

func assertEvent(t *testing.T, ev protocol.Event, typ string, sessionID string, durable bool) {
	t.Helper()
	if ev.Type != typ || ev.SessionID != sessionID || ev.Durable() != durable || ev.Time != 1700000000123 {
		t.Fatalf("event = %+v, want type=%s session=%s durable=%v", ev, typ, sessionID, durable)
	}
}

func payloadMap(t *testing.T, ev protocol.Event) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(ev.Payload, &out); err != nil {
		t.Fatalf("payload unmarshal error = %v; payload=%s", err, string(ev.Payload))
	}
	return out
}
