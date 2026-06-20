package acp_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

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
