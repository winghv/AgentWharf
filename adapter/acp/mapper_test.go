package acp_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/winghv/agentwharf/adapter/acp"
	"github.com/winghv/agentwharf/protocol"
)

func TestMapperMapsT0_5ACPFrames(t *testing.T) {
	t.Parallel()

	mapper, err := acp.NewMapper(acp.Config{
		SessionID: "ses_1",
		Provider:  "claude-code",
		Now: func() time.Time {
			return time.UnixMilli(1700000000456)
		},
	})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}

	input := strings.NewReader(strings.Join([]string{
		`{"type":"initialize_response","session_id":"acp_ses_1","model":"claude-fable-5","permissionMode":"default","apiKeySource":"none","claude_code_version":"2.1.91"}`,
		`{"type":"new_session_response","session_id":"acp_ses_1","model":"claude-fable-5","permissionMode":"default","apiKeySource":"none"}`,
		`{"type":"session/update","session_id":"acp_ses_1","updates":[{"type":"available_commands_update","available_commands":["send","stop"]},{"type":"usage_update","input_tokens":12,"output_tokens":3},{"type":"agent_thought_chunk","text":"thinking"},{"type":"agent_message_chunk","text":"pong"},{"type":"prompt_response","text":"ack"}]}`,
	}, "\n"))

	events, err := mapper.MapReader(context.Background(), input)
	if err != nil {
		t.Fatalf("MapReader() error = %v", err)
	}
	if len(events) != 7 {
		t.Fatalf("events = %d, want 7: %+v", len(events), events)
	}

	assertEvent(t, events[0], "session.state")
	starting := payloadMap(t, events[0])
	if starting["state"] != "starting" || starting["provider"] != "claude-code" || starting["provider_session_id"] != "acp_ses_1" {
		t.Fatalf("starting payload = %+v", starting)
	}

	assertEvent(t, events[1], "session.state")
	ready := payloadMap(t, events[1])
	if ready["state"] != "ready" || ready["provider_session_id"] != "acp_ses_1" {
		t.Fatalf("ready payload = %+v", ready)
	}

	assertEvent(t, events[2], "agent.activity")
	commands := payloadMap(t, events[2])
	if commands["kind"] != "available_commands_update" {
		t.Fatalf("available_commands payload = %+v", commands)
	}

	assertEvent(t, events[3], "agent.activity")
	usage := payloadMap(t, events[3])
	if usage["kind"] != "usage_update" {
		t.Fatalf("usage payload = %+v", usage)
	}

	assertEvent(t, events[4], "agent.activity")
	thinking := payloadMap(t, events[4])
	if thinking["kind"] != "thinking" || thinking["text"] != "thinking" {
		t.Fatalf("thinking payload = %+v", thinking)
	}

	assertEvent(t, events[5], "session.message")
	message := payloadMap(t, events[5])
	if message["role"] != "agent" || message["message_id"] != "acp_ses_1" {
		t.Fatalf("message payload = %+v", message)
	}
	if got := message["content"].([]any); len(got) != 1 || got[0].(map[string]any)["text"] != "pong" {
		t.Fatalf("message content = %+v", got)
	}

	assertEvent(t, events[6], "session.message")
	prompt := payloadMap(t, events[6])
	if prompt["role"] != "agent" {
		t.Fatalf("prompt payload = %+v", prompt)
	}
}

func TestMapperIgnoresUnknownAndRejectsInvalidJSON(t *testing.T) {
	t.Parallel()

	mapper, err := acp.NewMapper(acp.Config{SessionID: "ses_1", Provider: "claude-code"})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}

	events, err := mapper.MapReader(context.Background(), strings.NewReader("\n{\"type\":\"unknown\"}\n"))
	if err != nil {
		t.Fatalf("MapReader() error = %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("events = %+v, want none", events)
	}
	if _, err := mapper.MapLine([]byte(`{"type":`)); !errors.Is(err, acp.ErrInvalidACPEvent) {
		t.Fatalf("MapLine(invalid) error = %v, want ErrInvalidACPEvent", err)
	}
}

func TestMapperSupportsJSONRPCSessionUpdateEnvelope(t *testing.T) {
	t.Parallel()

	mapper, err := acp.NewMapper(acp.Config{SessionID: "ses_1", Provider: "claude-code", Now: func() time.Time {
		return time.UnixMilli(1700000000456)
	}})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}

	events, err := mapper.MapLine([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"session_id":"acp_ses_1","update":{"type":"agent_message_chunk","text":"hello"}}}`))
	if err != nil {
		t.Fatalf("MapLine() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %+v, want one message", events)
	}
	assertEvent(t, events[0], "session.message")
	message := payloadMap(t, events[0])
	if message["message_id"] != "acp_ses_1" {
		t.Fatalf("message payload = %+v", message)
	}
}

func TestMapperSplitsLargeMessageChunksWithinEventLimit(t *testing.T) {
	t.Parallel()

	mapper, err := acp.NewMapper(acp.Config{SessionID: "ses_1", Provider: "claude-code"})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}
	text := strings.Repeat("大输出🙂 ", 20000)
	line, err := json.Marshal(map[string]any{
		"type":   "session/update",
		"update": map[string]any{"type": "agent_message_chunk", "text": text},
	})
	if err != nil {
		t.Fatalf("marshal update: %v", err)
	}
	events, err := mapper.MapLine(line)
	if err != nil {
		t.Fatalf("MapLine() error = %v", err)
	}
	if len(events) < 2 {
		t.Fatalf("events = %d, want split output", len(events))
	}
	var rebuilt strings.Builder
	for _, event := range events {
		if event.Type != "session.message" || len(event.Payload) > protocol.MaxEventPayloadBytes {
			t.Fatalf("event = type:%s payload_bytes:%d", event.Type, len(event.Payload))
		}
		payload := payloadMap(t, event)
		content, ok := payload["content"].([]any)
		if !ok || len(content) != 1 {
			t.Fatalf("content = %+v", payload["content"])
		}
		rebuilt.WriteString(content[0].(map[string]any)["text"].(string))
	}
	if rebuilt.String() != text {
		t.Fatalf("rebuilt text differs: got %d bytes, want %d", rebuilt.Len(), len(text))
	}
}

func TestMapperMapsJSONRPCSessionNewResponseToReady(t *testing.T) {
	t.Parallel()

	mapper, err := acp.NewMapper(acp.Config{SessionID: "ses_1", Provider: "claude-code", Now: func() time.Time {
		return time.UnixMilli(1700000000456)
	}})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}
	events, err := mapper.MapLine([]byte(`{"jsonrpc":"2.0","id":2,"result":{"sessionId":"acp_ses_1"}}`))
	if err != nil {
		t.Fatalf("MapLine() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %+v, want one ready state", events)
	}
	assertEvent(t, events[0], "session.state")
	payload := payloadMap(t, events[0])
	if payload["state"] != "ready" || payload["provider_session_id"] != "acp_ses_1" {
		t.Fatalf("ready payload = %+v", payload)
	}
}

func TestMapperDoesNotTreatJSONRPCErrorAsReady(t *testing.T) {
	t.Parallel()

	mapper, err := acp.NewMapper(acp.Config{SessionID: "ses_1", Provider: "claude-code", Now: func() time.Time {
		return time.UnixMilli(1700000000456)
	}})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}
	events, err := mapper.MapLine([]byte(`{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"session failed"}}`))
	if err != nil {
		t.Fatalf("MapLine() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %+v, want one agent message for the prompt error", events)
	}
	assertEvent(t, events[0], "session.message")
	payload := payloadMap(t, events[0])
	if payload["role"] != "agent" {
		t.Fatalf("error message role = %+v, want agent", payload)
	}
	content, _ := payload["content"].([]any)
	if len(content) == 0 {
		t.Fatalf("error message content = %+v", payload["content"])
	}
	part, _ := content[0].(map[string]any)
	text, _ := part["text"].(string)
	if !strings.Contains(text, "session failed") {
		t.Fatalf("error message text = %q, want the Provider error", text)
	}
}

func TestMapperDoesNotTreatInitializeResponseAsReady(t *testing.T) {
	t.Parallel()

	mapper, err := acp.NewMapper(acp.Config{SessionID: "ses_1", Provider: "claude-code"})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}
	events, err := mapper.MapLine([]byte(`{"jsonrpc":"2.0","id":1,"sessionId":"unexpected","result":null}`))
	if err != nil {
		t.Fatalf("MapLine() error = %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("events = %+v, want no ready state for initialize response", events)
	}
}

func TestMapperSupportsLiveACPCamelCaseSessionUpdate(t *testing.T) {
	t.Parallel()

	mapper, err := acp.NewMapper(acp.Config{SessionID: "ses_1", Provider: "claude-code", Now: func() time.Time {
		return time.UnixMilli(1700000000456)
	}})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}

	events, err := mapper.MapReader(context.Background(), strings.NewReader(strings.Join([]string{
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"available_commands_update","availableCommands":[{"name":"verify"}]}}}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"pong"},"messageId":"resp_1"}}}`,
	}, "\n")))
	if err != nil {
		t.Fatalf("MapReader() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %+v, want two events", events)
	}

	assertEvent(t, events[0], "agent.activity")
	activity := payloadMap(t, events[0])
	if activity["kind"] != "available_commands_update" || activity["provider_session_id"] != "acp_ses_1" {
		t.Fatalf("activity payload = %+v", activity)
	}

	assertEvent(t, events[1], "session.message")
	message := payloadMap(t, events[1])
	if message["message_id"] != "resp_1" {
		t.Fatalf("message payload = %+v", message)
	}
	if got := message["content"].([]any); len(got) != 1 || got[0].(map[string]any)["text"] != "pong" {
		t.Fatalf("message content = %+v", got)
	}
}

func TestMapperMapsACPCancelResponseToReadyState(t *testing.T) {
	t.Parallel()

	mapper, err := acp.NewMapper(acp.Config{SessionID: "ses_1", Provider: "claude-code"})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}
	events, err := mapper.MapLine([]byte(`{"type":"session/cancel_response","session_id":"acp_ses_1","stopReason":"cancelled"}`))
	if err != nil {
		t.Fatalf("MapLine() error = %v", err)
	}
	if len(events) != 1 || events[0].Type != "session.state" {
		t.Fatalf("cancel response events = %+v, want one session.state", events)
	}
	payload := payloadMap(t, events[0])
	if payload["state"] != "ready" || payload["provider_session_id"] != "acp_ses_1" {
		t.Fatalf("cancel response payload = %+v", payload)
	}
}

func TestMapperMapsACPPermissionRequest(t *testing.T) {
	t.Parallel()

	mapper, err := acp.NewMapper(acp.Config{SessionID: "ses_1", Provider: "claude-code", Now: func() time.Time {
		return time.UnixMilli(1700000000456)
	}})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}

	events, err := mapper.MapLine([]byte(`{"jsonrpc":"2.0","id":7,"method":"session/request_permission","params":{"sessionId":"acp_ses_1","action":"fs.write","riskLevel":"medium","summary":"Write a file","options":[{"kind":"reject","optionId":"reject_1"}]}}`))
	if err != nil {
		t.Fatalf("MapLine() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %+v, want tool call plus permission request", events)
	}
	assertEvent(t, events[0], "session.tool_call")
	tool := payloadMap(t, events[0])
	if tool["tool_call_id"] != "permission:7" || tool["phase"] != "start" ||
		tool["name"] != "fs.write" || tool["result"] != nil {
		t.Fatalf("tool payload = %+v", tool)
	}
	input := tool["input"].(map[string]any)
	if input["action"] != "fs.write" || input["risk_level"] != "medium" ||
		input["summary"] != "Write a file" || len(input["options"].([]any)) != 1 {
		t.Fatalf("tool input = %+v", input)
	}

	assertEvent(t, events[1], "permission.request")
	payload := payloadMap(t, events[1])
	if payload["request_id"] != "7" || payload["action"] != "fs.write" ||
		payload["risk_level"] != "medium" || payload["summary"] != "Write a file" {
		t.Fatalf("permission payload = %+v", payload)
	}
	detail := payload["detail"].(map[string]any)
	if detail["provider_session_id"] != "acp_ses_1" || len(detail["options"].([]any)) != 1 {
		t.Fatalf("permission detail = %+v", detail)
	}
}

func TestMapperMapsCorrelatedACPToolCallLifecycle(t *testing.T) {
	t.Parallel()

	mapper, err := acp.NewMapper(acp.Config{SessionID: "ses_1", Provider: "claude-code", Now: func() time.Time {
		return time.UnixMilli(1700000000456)
	}})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}

	events, err := mapper.MapReader(context.Background(), strings.NewReader(strings.Join([]string{
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"tool_call","toolCallId":"call_001","title":"Run tests","kind":"execute","status":"in_progress","rawInput":{"command":"go test ./..."}}}}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"tool_call_update","toolCallId":"call_001","status":"completed","rawOutput":"ok"}}}`,
	}, "\n")))
	if err != nil {
		t.Fatalf("MapReader() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %+v, want correlated start and result", events)
	}

	assertEvent(t, events[0], "session.tool_call")
	start := payloadMap(t, events[0])
	if start["tool_call_id"] != "call_001" || start["phase"] != "start" || start["name"] != "execute" || start["result"] != nil {
		t.Fatalf("start payload = %+v", start)
	}
	input := start["input"].(map[string]any)
	if input["command"] != "go test ./..." {
		t.Fatalf("start input = %+v", input)
	}

	assertEvent(t, events[1], "session.tool_call")
	result := payloadMap(t, events[1])
	if result["tool_call_id"] != "call_001" || result["phase"] != "result" {
		t.Fatalf("result payload = %+v", result)
	}
	if _, ok := result["name"]; ok {
		t.Fatalf("delta result unexpectedly overwrote the start name: %+v", result)
	}
	resultBody := result["result"].(map[string]any)
	if resultBody["status"] != "ok" || resultBody["output_preview"] != "ok" || resultBody["truncated"] != false {
		t.Fatalf("result body = %+v", resultBody)
	}
}

func TestMapperMapsACPToolCallFailureAndBoundsOutputPreview(t *testing.T) {
	t.Parallel()

	mapper, err := acp.NewMapper(acp.Config{SessionID: "ses_1", Provider: "claude-code", Now: func() time.Time {
		return time.UnixMilli(1700000000456)
	}})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}

	longOutput := strings.Repeat("界", 2000)
	line, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  "session/update",
		"params": map[string]any{
			"sessionId": "acp_ses_1",
			"update": map[string]any{
				"sessionUpdate": "tool_call_update",
				"toolCallId":    "call_failed",
				"kind":          "execute",
				"status":        "failed",
				"content":       longOutput,
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal test frame: %v", err)
	}
	events, err := mapper.MapLine(line)
	if err != nil {
		t.Fatalf("MapLine() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %+v, want one result", events)
	}
	result := payloadMap(t, events[0])
	if result["tool_call_id"] != "call_failed" || result["phase"] != "result" || result["name"] != "execute" {
		t.Fatalf("result payload = %+v", result)
	}
	resultBody := result["result"].(map[string]any)
	preview := resultBody["output_preview"].(string)
	if resultBody["status"] != "error" || resultBody["truncated"] != true || len(preview) > 4096 || !utf8.ValidString(preview) {
		t.Fatalf("bounded result body = status=%v truncated=%v bytes=%d valid_utf8=%v", resultBody["status"], resultBody["truncated"], len(preview), utf8.ValidString(preview))
	}
}

func TestMapperMapsProviderSettingsCapabilityAndEffectiveResult(t *testing.T) {
	t.Parallel()

	mapper, err := acp.NewMapper(acp.Config{SessionID: "ses_1", Provider: "claude-code", Now: func() time.Time {
		return time.UnixMilli(1700000000456)
	}})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}
	events, err := mapper.MapReader(context.Background(), strings.NewReader(strings.Join([]string{
		`{"type":"initialize_response","session_id":"acp_ses_1","model":"reasoning","permissionMode":"ask","models":[{"id":"reasoning","label":"Reasoning"},{"id":"balanced","label":"Balanced"}],"permissionModes":[{"id":"workspace","label":"Workspace"},{"id":"ask","label":"Ask first"}],"modelChange":true,"permissionChange":true}`,
		`{"type":"settings_change_response","cmd_id":"cmd_settings_1","request_fingerprint":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","effective_fingerprint":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","outcome":"rejected","effective_model_id":"balanced","effective_permission_mode_id":"ask","reason_code":"provider_rejected"}`,
	}, "\n")))
	if err != nil {
		t.Fatalf("MapReader() error = %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("events = %+v, want state plus settings events", events)
	}
	assertEvent(t, events[1], "session.settings.capabilities")
	capability, err := protocol.DecodeSettingsCapabilityPayload(events[1].Payload)
	if err != nil {
		t.Fatalf("capability payload error = %v; payload=%s", err, events[1].Payload)
	}
	if capability.EffectiveModelID != "reasoning" || capability.EffectivePermissionModeID != "ask" || capability.ModelChange != "allowed" || capability.PermissionChange != "allowed" {
		t.Fatalf("capability = %+v", capability)
	}
	if capability.Models[0].ID != "balanced" || capability.PermissionModes[0].ID != "ask" {
		t.Fatalf("capability choices are not canonical: %+v", capability)
	}
	assertEvent(t, events[2], "session.settings.effective")
	effective, err := protocol.DecodeSettingsEffectivePayload(events[2].Payload)
	if err != nil {
		t.Fatalf("effective payload error = %v; payload=%s", err, events[2].Payload)
	}
	if effective.CommandID != "cmd_settings_1" || effective.Outcome != "rejected" || effective.ReasonCode == nil || *effective.ReasonCode != "provider_rejected" {
		t.Fatalf("effective = %+v", effective)
	}
}

func TestMapperRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	if _, err := acp.NewMapper(acp.Config{}); !errors.Is(err, acp.ErrInvalidConfig) {
		t.Fatalf("NewMapper(empty) error = %v, want ErrInvalidConfig", err)
	}
}

func assertEvent(t *testing.T, ev protocol.Event, typ string) {
	t.Helper()
	if ev.Type != typ || ev.SessionID != "ses_1" || ev.Durable() {
		t.Fatalf("event = %+v, want type=%s session=ses_1 ephemeral", ev, typ)
	}
	if ev.Time != 1700000000456 {
		t.Fatalf("event time = %d, want 1700000000456", ev.Time)
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
