package jsonstream

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
	ErrInvalidConfig      = errors.New("invalid jsonstream translator config")
	ErrInvalidStreamEvent = errors.New("invalid jsonstream event")
)

const maxOutputPreviewBytes = 4096

type Config struct {
	SessionID string
	Provider  string
	Now       func() time.Time
}

type Translator struct {
	sessionID string
	provider  string
	now       func() time.Time
}

func NewTranslator(cfg Config) (*Translator, error) {
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
	return &Translator{
		sessionID: cfg.SessionID,
		provider:  cfg.Provider,
		now:       now,
	}, nil
}

func (t *Translator) TranslateReader(ctx context.Context, r io.Reader) ([]protocol.Event, error) {
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
		events, err := t.TranslateLine(scanner.Bytes())
		if err != nil {
			return nil, err
		}
		out = append(out, events...)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("%w: scan stream: %w", ErrInvalidStreamEvent, err)
	}
	return out, nil
}

func (t *Translator) TranslateLine(line []byte) ([]protocol.Event, error) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return nil, nil
	}

	var raw map[string]any
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("%w: decode JSON line: %w", ErrInvalidStreamEvent, err)
	}

	switch stringField(raw, "type") {
	case "system":
		return t.translateSystem(raw)
	case "assistant":
		return t.translateAssistant(raw)
	case "user":
		return t.translateUser(raw)
	case "result":
		return t.translateResult(raw)
	default:
		return nil, nil
	}
}

func (t *Translator) translateSystem(raw map[string]any) ([]protocol.Event, error) {
	switch stringField(raw, "subtype") {
	case "init":
		metadata := copyWithout(raw, "type", "subtype", "session_id")
		return []protocol.Event{t.event("session.state", map[string]any{
			"state":               "ready",
			"provider":            t.provider,
			"provider_session_id": stringField(raw, "session_id"),
			"metadata":            metadata,
			"source":              "claude.stream_json",
		})}, nil
	case "api_retry":
		payload := copyWithout(raw, "type", "subtype")
		payload["kind"] = "api_retry"
		payload["source"] = "claude.stream_json"
		return []protocol.Event{t.event("agent.activity", payload)}, nil
	default:
		return nil, nil
	}
}

func (t *Translator) translateAssistant(raw map[string]any) ([]protocol.Event, error) {
	message := objectField(raw, "message")
	if message == nil {
		message = raw
	}

	var (
		out          []protocol.Event
		messageParts []map[string]any
	)
	flushMessage := func() {
		if len(messageParts) == 0 {
			return
		}
		out = append(out, t.event("session.message", map[string]any{
			"message_id": firstString(message, "id", "message_id"),
			"role":       "agent",
			"content":    messageParts,
		}))
		messageParts = nil
	}
	for _, block := range contentBlocks(message) {
		switch stringField(block, "type") {
		case "thinking":
			flushMessage()
			text := firstString(block, "thinking", "text")
			if text != "" {
				out = append(out, t.event("agent.activity", map[string]any{
					"kind": "thinking",
					"text": text,
				}))
			}
		case "text":
			text := stringField(block, "text")
			if text != "" {
				messageParts = append(messageParts, map[string]any{
					"kind": "text",
					"text": text,
				})
			}
		case "tool_use":
			flushMessage()
			out = append(out, t.event("session.tool_call", map[string]any{
				"tool_call_id": firstString(block, "id", "tool_call_id"),
				"phase":        "start",
				"name":         stringField(block, "name"),
				"input":        objectOrNil(block["input"]),
				"result":       nil,
			}))
		}
	}
	flushMessage()
	return out, nil
}

func (t *Translator) translateUser(raw map[string]any) ([]protocol.Event, error) {
	message := objectField(raw, "message")
	if message == nil {
		message = raw
	}

	var out []protocol.Event
	for _, block := range contentBlocks(message) {
		if stringField(block, "type") != "tool_result" {
			continue
		}
		preview, truncated := outputPreview(block["content"])
		out = append(out, t.event("session.tool_call", map[string]any{
			"tool_call_id": firstString(block, "tool_use_id", "id", "tool_call_id"),
			"phase":        "result",
			"name":         stringField(block, "name"),
			"input":        nil,
			"result": map[string]any{
				"status":         "ok",
				"output_preview": preview,
				"truncated":      truncated,
			},
		}))
	}
	return out, nil
}

func (t *Translator) translateResult(raw map[string]any) ([]protocol.Event, error) {
	if stringField(raw, "subtype") == "success" {
		return []protocol.Event{t.event("session.state", map[string]any{
			"state":               "ready",
			"reason":              firstString(raw, "terminal_reason", "subtype"),
			"provider_session_id": stringField(raw, "session_id"),
			"source":              "claude.stream_json",
		})}, nil
	}

	payload := copyWithout(raw, "type")
	payload["source"] = "claude.stream_json"
	return []protocol.Event{t.event("session.error", payload)}, nil
}

func (t *Translator) event(eventType string, payload map[string]any) protocol.Event {
	encoded, err := json.Marshal(payload)
	if err != nil {
		encoded = []byte(`{"error":"payload_marshal_failed"}`)
	}
	return protocol.Event{
		Type:      eventType,
		SessionID: t.sessionID,
		Time:      t.now().UTC().UnixMilli(),
		Payload:   encoded,
	}
}

func contentBlocks(message map[string]any) []map[string]any {
	content, ok := message["content"].([]any)
	if !ok {
		return nil
	}
	blocks := make([]map[string]any, 0, len(content))
	for _, item := range content {
		block, ok := item.(map[string]any)
		if ok {
			blocks = append(blocks, block)
		}
	}
	return blocks
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

func outputPreview(value any) (string, bool) {
	var preview string
	switch typed := value.(type) {
	case string:
		preview = typed
	case []any:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return "", false
		}
		preview = string(encoded)
	default:
		if value == nil {
			return "", false
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return "", false
		}
		preview = string(encoded)
	}
	if len(preview) <= maxOutputPreviewBytes {
		return preview, false
	}
	return preview[:maxOutputPreviewBytes], true
}
