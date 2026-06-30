package acp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/winghv/agentwharf/protocol"
)

var (
	ErrInvalidConfig   = errors.New("invalid acp mapper config")
	ErrInvalidACPEvent = errors.New("invalid acp event")
)

type Config struct {
	SessionID string
	Provider  string
	Now       func() time.Time
}

type Mapper struct {
	sessionID string
	provider  string
	now       func() time.Time
}

func NewMapper(cfg Config) (*Mapper, error) {
	if cfg.SessionID == "" {
		return nil, fmt.Errorf("%w: session_id is required", ErrInvalidConfig)
	}
	if cfg.Provider == "" {
		return nil, fmt.Errorf("%w: provider is required", ErrInvalidConfig)
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Mapper{
		sessionID: cfg.SessionID,
		provider:  cfg.Provider,
		now:       now,
	}, nil
}

func (m *Mapper) MapReader(ctx context.Context, r io.Reader) ([]protocol.Event, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var out []protocol.Event
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		events, err := m.MapLine(scanner.Bytes())
		if err != nil {
			return nil, err
		}
		out = append(out, events...)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("%w: scan stream: %w", ErrInvalidACPEvent, err)
	}
	return out, nil
}

func (m *Mapper) MapLine(line []byte) ([]protocol.Event, error) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return nil, nil
	}

	var raw map[string]any
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("%w: decode JSON line: %w", ErrInvalidACPEvent, err)
	}
	return m.mapFrame(raw, firstString(raw, "session_id", "sessionId")), nil
}

func (m *Mapper) mapFrame(raw map[string]any, providerSessionID string) []protocol.Event {
	switch frameName(raw) {
	case "initialize_response":
		return []protocol.Event{m.stateEvent("starting", providerSessionID, copyWithout(raw, "type", "method", "session_id"))}
	case "new_session_response":
		return []protocol.Event{m.stateEvent("ready", providerSessionID, copyWithout(raw, "type", "method", "session_id"))}
	case "session/update":
		return m.mapSessionUpdate(raw, providerSessionID)
	case "session/request_permission":
		return m.mapSessionPermissionRequest(raw, providerSessionID)
	default:
		return m.mapUpdate(raw, providerSessionID)
	}
}

func (m *Mapper) mapSessionUpdate(raw map[string]any, providerSessionID string) []protocol.Event {
	if providerSessionID == "" {
		providerSessionID = stringField(raw, "session_id")
	}
	source := raw
	if params := objectField(raw, "params"); params != nil {
		source = params
		if providerSessionID == "" {
			providerSessionID = firstString(params, "session_id", "sessionId")
		}
	}

	var out []protocol.Event
	for _, update := range updateObjects(source, "update") {
		out = append(out, m.mapUpdate(update, providerSessionID)...)
	}
	for _, update := range updateObjects(source, "updates") {
		out = append(out, m.mapUpdate(update, providerSessionID)...)
	}
	return out
}

func (m *Mapper) mapSessionPermissionRequest(raw map[string]any, providerSessionID string) []protocol.Event {
	source := raw
	if params := objectField(raw, "params"); params != nil {
		source = params
		if providerSessionID == "" {
			providerSessionID = firstString(params, "session_id", "sessionId")
		}
	}
	requestID := firstString(source, "request_id", "requestId", "id")
	if requestID == "" {
		requestID = stringFromAny(raw["id"])
	}
	events := []protocol.Event{m.permissionToolCallEvent(source, requestID)}
	return append(events, m.permissionRequestEvent(source, requestID, providerSessionID)...)
}

func (m *Mapper) mapUpdate(update map[string]any, providerSessionID string) []protocol.Event {
	switch frameName(update) {
	case "available_commands_update":
		payload := copyWithout(update, "type", "subtype", "kind", "sessionUpdate")
		payload["kind"] = "available_commands_update"
		payload["provider_session_id"] = providerSessionID
		return []protocol.Event{m.event("agent.activity", payload)}
	case "usage_update":
		payload := copyWithout(update, "type", "subtype", "kind", "sessionUpdate")
		payload["kind"] = "usage_update"
		payload["provider_session_id"] = providerSessionID
		return []protocol.Event{m.event("agent.activity", payload)}
	case "agent_thought_chunk":
		text := updateText(update)
		if text == "" {
			return nil
		}
		return []protocol.Event{m.event("agent.activity", map[string]any{
			"kind":                "thinking",
			"text":                text,
			"provider_session_id": providerSessionID,
		})}
	case "agent_message_chunk", "prompt_response":
		text := updateText(update)
		if text == "" {
			return nil
		}
		messageID := firstString(update, "message_id", "messageId", "id", "session_id", "sessionId")
		if messageID == "" {
			messageID = providerSessionID
		}
		return []protocol.Event{m.messageEvent(messageID, text)}
	case "tool_use", "tool_call":
		return []protocol.Event{m.event("session.tool_call", map[string]any{
			"tool_call_id": firstString(update, "tool_call_id", "toolCallId", "id"),
			"phase":        "start",
			"name":         stringField(update, "name"),
			"input":        objectOrNil(update["input"]),
			"result":       nil,
		})}
	case "permission_request":
		return m.permissionRequestEvent(update, firstString(update, "request_id", "requestId", "id"), providerSessionID)
	default:
		return nil
	}
}

func (m *Mapper) permissionRequestEvent(source map[string]any, requestID string, providerSessionID string) []protocol.Event {
	detail := map[string]any{}
	if existing, ok := objectOrNil(source["detail"]).(map[string]any); ok {
		for key, value := range existing {
			detail[key] = value
		}
	}
	if options, ok := source["options"]; ok {
		detail["options"] = options
	}
	if providerSessionID != "" {
		detail["provider_session_id"] = providerSessionID
	}
	return []protocol.Event{m.event("permission.request", map[string]any{
		"request_id": requestID,
		"action":     stringField(source, "action"),
		"risk_level": firstString(source, "risk_level", "riskLevel", "risk"),
		"summary":    stringField(source, "summary"),
		"detail":     detail,
		"expires_at": firstAny(source, "expires_at", "expiresAt"),
	})}
}

func (m *Mapper) permissionToolCallEvent(source map[string]any, requestID string) protocol.Event {
	toolCallID := "permission"
	if requestID != "" {
		toolCallID = "permission:" + requestID
	}
	action := stringField(source, "action")
	name := action
	if name == "" {
		name = "permission"
	}
	input := map[string]any{
		"action":     action,
		"risk_level": firstString(source, "risk_level", "riskLevel", "risk"),
		"summary":    stringField(source, "summary"),
	}
	if options, ok := source["options"]; ok {
		input["options"] = options
	}
	return m.event("session.tool_call", map[string]any{
		"tool_call_id": toolCallID,
		"phase":        "start",
		"name":         name,
		"input":        input,
		"result":       nil,
	})
}

func (m *Mapper) stateEvent(state string, providerSessionID string, metadata map[string]any) protocol.Event {
	return m.event("session.state", map[string]any{
		"state":               state,
		"provider":            m.provider,
		"provider_session_id": providerSessionID,
		"metadata":            metadata,
		"source":              "acp",
	})
}

func (m *Mapper) messageEvent(messageID string, text string) protocol.Event {
	return m.event("session.message", map[string]any{
		"message_id": messageID,
		"role":       "agent",
		"content": []map[string]any{{
			"kind": "text",
			"text": text,
		}},
	})
}

func (m *Mapper) event(eventType string, payload map[string]any) protocol.Event {
	encoded, err := json.Marshal(payload)
	if err != nil {
		encoded = []byte(`{"error":"payload_marshal_failed"}`)
	}
	return protocol.Event{
		Type:      eventType,
		SessionID: m.sessionID,
		Time:      m.now().UTC().UnixMilli(),
		Payload:   encoded,
	}
}

func updateObjects(raw map[string]any, key string) []map[string]any {
	value := raw[key]
	if object, ok := value.(map[string]any); ok {
		return []map[string]any{object}
	}
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		object, ok := item.(map[string]any)
		if ok {
			out = append(out, object)
		}
	}
	return out
}

func frameName(value map[string]any) string {
	if name := firstString(value, "type", "subtype", "kind", "method", "event", "sessionUpdate"); name != "" {
		return name
	}
	return ""
}

func objectField(value map[string]any, key string) map[string]any {
	object, ok := value[key].(map[string]any)
	if !ok {
		return nil
	}
	return object
}

func objectOrNil(value any) any {
	if _, ok := value.(map[string]any); ok {
		return value
	}
	return nil
}

func stringField(value map[string]any, key string) string {
	text, ok := value[key].(string)
	if !ok {
		return ""
	}
	return text
}

func firstString(value map[string]any, keys ...string) string {
	for _, key := range keys {
		if text := stringField(value, key); text != "" {
			return text
		}
	}
	return ""
}

func firstAny(value map[string]any, keys ...string) any {
	for _, key := range keys {
		if item, ok := value[key]; ok {
			return item
		}
	}
	return nil
}

func stringFromAny(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case nil:
		return ""
	default:
		return fmt.Sprint(typed)
	}
}

func updateText(value map[string]any) string {
	if text := firstString(value, "text", "chunk", "content"); text != "" {
		return text
	}
	if content := objectField(value, "content"); content != nil {
		return firstString(content, "text", "chunk")
	}
	return ""
}

func copyWithout(value map[string]any, keys ...string) map[string]any {
	out := make(map[string]any, len(value))
	for key, item := range value {
		if contains(keys, key) {
			continue
		}
		out[key] = item
	}
	return out
}

func contains(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}
