package acp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/winghv/agentwharf/protocol"
)

var (
	ErrInvalidConfig   = errors.New("invalid acp mapper config")
	ErrInvalidACPEvent = errors.New("invalid acp event")
)

const (
	maxToolOutputPreviewBytes = 4096
	// Leave room for the protocol envelope within Hub's 64 KiB WebSocket limit.
	maxMessagePayloadBytes = protocol.MaxEventPayloadBytes - 4*1024
	maxTextChunkBytes      = 8 * 1024
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
		events := []protocol.Event{m.stateEvent("starting", providerSessionID, copyWithout(raw, "type", "method", "session_id"))}
		if event := m.settingsCapabilityEvent(raw); event != nil {
			events = append(events, *event)
		}
		return events
	case "new_session_response":
		events := []protocol.Event{m.stateEvent("ready", providerSessionID, copyWithout(raw, "type", "method", "session_id"))}
		if event := m.settingsCapabilityEvent(raw); event != nil {
			events = append(events, *event)
		}
		return events
	case "session/cancel_response", "cancel_response":
		return []protocol.Event{m.stateEvent("ready", providerSessionID, copyWithout(raw, "type", "method", "session_id"))}
	case "settings_change_response", "session/settings/change_response", "settings_effective":
		if event := m.settingsEffectiveEvent(raw); event != nil {
			return []protocol.Event{*event}
		}
		return nil
	case "session/update":
		return m.mapSessionUpdate(raw, providerSessionID)
	case "session/request_permission":
		return m.mapSessionPermissionRequest(raw, providerSessionID)
	default:
		if responseSessionID := sessionIDFromResponse(raw); responseSessionID != "" {
			return []protocol.Event{m.stateEvent("ready", responseSessionID, copyWithout(raw, "type", "method", "session_id", "sessionId"))}
		}
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
	case "settings_capabilities", "settings_capability_update", "settings/update":
		if event := m.settingsCapabilityEvent(update); event != nil {
			return []protocol.Event{*event}
		}
		return nil
	case "settings_change_response", "settings_effective":
		if event := m.settingsEffectiveEvent(update); event != nil {
			return []protocol.Event{*event}
		}
		return nil
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
		return splitMessageEvents(text, maxMessagePayloadBytes, func(part string) protocol.Event {
			return m.event("agent.activity", map[string]any{
				"kind":                "thinking",
				"text":                part,
				"provider_session_id": providerSessionID,
			})
		})
	case "agent_message_chunk", "prompt_response":
		text := updateText(update)
		if text == "" {
			return nil
		}
		messageID := firstString(update, "message_id", "messageId", "id", "session_id", "sessionId")
		if messageID == "" {
			messageID = providerSessionID
		}
		return m.messageEvents(messageID, text)
	case "tool_use", "tool_call":
		return m.toolCallEvents(update, false)
	case "tool_call_update", "tool_result":
		return m.toolCallEvents(update, true)
	case "permission_request":
		return m.permissionRequestEvent(update, firstString(update, "request_id", "requestId", "id"), providerSessionID)
	default:
		return nil
	}
}

func isACPAskUserQuestion(name string) bool {
	switch name {
	case "AskUserQuestion", "ask_user_question", "askUserQuestion":
		return true
	default:
		return false
	}
}

func acpQuestionItems(input any) any {
	if object, ok := input.(map[string]any); ok {
		if questions, ok := object["questions"]; ok {
			return questions
		}
	}
	return []any{}
}

func (m *Mapper) toolCallEvents(update map[string]any, isUpdate bool) []protocol.Event {
	toolCallID := firstString(update, "tool_call_id", "toolCallId", "tool_use_id", "toolUseId", "id")
	if toolCallID == "" {
		return nil
	}

	status := firstString(update, "status", "phase")
	if !isUpdate || status == "pending" || status == "in_progress" || status == "start" {
		input := objectOrNil(firstAny(update, "rawInput", "raw_input", "input"))
		name := firstString(update, "name", "kind", "title")
		events := []protocol.Event{m.event("session.tool_call", map[string]any{
			"tool_call_id": toolCallID,
			"phase":        "start",
			"name":         name,
			"input":        input,
			"result":       nil,
		})}
		if isACPAskUserQuestion(name) {
			events = append(events, m.event("permission.request", map[string]any{
				"request_id": "question:" + toolCallID,
				"action":     "ask_user_question",
				"risk_level": "low",
				"summary":    "Agent asks a question",
				"detail":     map[string]any{"questions": acpQuestionItems(input)},
			}))
		}
		return events
	}

	resultStatus := ""
	switch status {
	case "completed", "result", "ok", "success":
		resultStatus = "ok"
	case "failed", "error":
		resultStatus = "error"
	case "cancelled", "canceled":
		resultStatus = "cancelled"
	default:
		return nil
	}

	preview, truncated := toolOutputPreview(firstAny(update, "rawOutput", "raw_output", "result", "content"))
	payload := map[string]any{
		"tool_call_id": toolCallID,
		"phase":        "result",
		"input":        nil,
		"result": map[string]any{
			"status":         resultStatus,
			"output_preview": preview,
			"truncated":      truncated,
		},
	}
	if name := firstString(update, "name", "kind", "title"); name != "" {
		payload["name"] = name
	}
	return []protocol.Event{m.event("session.tool_call", payload)}
}

func toolOutputPreview(value any) (string, bool) {
	if value == nil {
		return "", false
	}
	preview, ok := value.(string)
	if !ok {
		encoded, err := json.Marshal(value)
		if err != nil {
			return "", false
		}
		preview = string(encoded)
	}
	if len(preview) <= maxToolOutputPreviewBytes {
		return preview, false
	}
	limit := maxToolOutputPreviewBytes
	for limit > 0 && !utf8.ValidString(preview[:limit]) {
		limit--
	}
	return preview[:limit], true
}

func (m *Mapper) settingsCapabilityEvent(raw map[string]any) *protocol.Event {
	source := raw
	if params := objectField(raw, "params"); params != nil {
		source = params
	}
	models, modelOK := settingsChoices(source, 32, "models", "available_models", "model_options")
	permissions, permissionOK := settingsChoices(source, 16, "permission_modes", "permissionModes", "available_permission_modes", "permission_options")
	if !modelOK || !permissionOK || (!modelOK && !permissionOK) || (len(models) == 0 && len(permissions) == 0) {
		return nil
	}
	effectiveModel := firstString(source, "effective_model_id", "effectiveModelId", "model")
	effectivePermission := firstString(source, "effective_permission_mode_id", "effectivePermissionModeId", "permission_mode", "permissionMode")
	if effectiveModel == "" && len(models) > 0 {
		effectiveModel = models[0].ID
	}
	if effectivePermission == "" && len(permissions) > 0 {
		effectivePermission = permissions[0].ID
	}
	modelChange, modelReason := settingsChangeMode(source, len(models), "model_change", "modelChange")
	permissionChange, permissionReason := settingsChangeMode(source, len(permissions), "permission_change", "permissionChange")
	payload := protocol.SettingsCapabilityPayload{
		SchemaVersion:             1,
		Models:                    models,
		PermissionModes:           permissions,
		EffectiveModelID:          effectiveModel,
		EffectivePermissionModeID: effectivePermission,
		ModelChange:               modelChange,
		PermissionChange:          permissionChange,
		ModelReadOnlyReason:       modelReason,
		PermissionReadOnlyReason:  permissionReason,
	}
	payload.Fingerprint = settingsCapabilityFingerprint(payload)
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	if _, err := protocol.DecodeSettingsCapabilityPayload(encoded); err != nil {
		return nil
	}
	return m.rawEvent("session.settings.capabilities", encoded)
}

func (m *Mapper) settingsEffectiveEvent(raw map[string]any) *protocol.Event {
	source := raw
	if params := objectField(raw, "params"); params != nil {
		source = params
	}
	payload := map[string]any{
		"cmd_id":                       firstString(source, "cmd_id", "command_id", "commandId"),
		"request_fingerprint":          firstString(source, "request_fingerprint", "requestFingerprint"),
		"effective_fingerprint":        firstString(source, "effective_fingerprint", "effectiveFingerprint"),
		"outcome":                      firstString(source, "outcome", "status"),
		"effective_model_id":           firstString(source, "effective_model_id", "effectiveModelId", "model"),
		"effective_permission_mode_id": firstString(source, "effective_permission_mode_id", "effectivePermissionModeId", "permission_mode", "permissionMode"),
		"reason_code":                  firstAny(source, "reason_code", "reasonCode"),
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	if _, err := protocol.DecodeSettingsEffectivePayload(encoded); err != nil {
		return nil
	}
	return m.rawEvent("session.settings.effective", encoded)
}

func (m *Mapper) rawEvent(eventType string, payload json.RawMessage) *protocol.Event {
	return &protocol.Event{Type: eventType, SessionID: m.sessionID, Time: m.now().UTC().UnixMilli(), Payload: payload}
}

func settingsChoices(source map[string]any, max int, keys ...string) ([]protocol.SettingsCapabilityChoice, bool) {
	value := firstAny(source, keys...)
	if value == nil {
		return nil, false
	}
	items, ok := value.([]any)
	if !ok {
		return nil, false
	}
	if len(items) == 0 || len(items) > max {
		return nil, false
	}
	choices := make([]protocol.SettingsCapabilityChoice, 0, len(items))
	for _, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			return nil, false
		}
		id := firstString(object, "id", "value", "name")
		label := firstString(object, "label", "title", "name", "value")
		if id == "" || label == "" {
			return nil, false
		}
		choices = append(choices, protocol.SettingsCapabilityChoice{ID: id, Label: label})
	}
	sort.Slice(choices, func(i, j int) bool { return choices[i].ID < choices[j].ID })
	for i := 1; i < len(choices); i++ {
		if choices[i-1].ID == choices[i].ID {
			return nil, false
		}
	}
	return choices, true
}

func settingsChangeMode(source map[string]any, choiceCount int, keys ...string) (string, *string) {
	value := firstAny(source, keys...)
	allowed := false
	switch typed := value.(type) {
	case bool:
		allowed = typed
	case string:
		allowed = typed == "allowed"
	}
	if allowed && choiceCount >= 2 {
		return "allowed", nil
	}
	reason := "provider_unsupported"
	return "read_only", &reason
}

func settingsCapabilityFingerprint(capability protocol.SettingsCapabilityPayload) string {
	data := make([]byte, 0, 512)
	appendString := func(value string) {
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(len(value)))
		data = append(data, length[:]...)
		data = append(data, value...)
	}
	data = append(data, []byte("agentwharf.settings-capability.v1")...)
	data = append(data, 0, 1, byte(len(capability.Models)))
	for _, choice := range capability.Models {
		appendString(choice.ID)
		appendString(choice.Label)
	}
	data = append(data, byte(len(capability.PermissionModes)))
	for _, choice := range capability.PermissionModes {
		appendString(choice.ID)
		appendString(choice.Label)
	}
	appendString(capability.EffectiveModelID)
	appendString(capability.EffectivePermissionModeID)
	if capability.ModelChange == "allowed" {
		data = append(data, 1)
	} else {
		data = append(data, 0)
	}
	if capability.PermissionChange == "allowed" {
		data = append(data, 1)
	} else {
		data = append(data, 0)
	}
	if capability.ModelReadOnlyReason == nil {
		appendString("")
	} else {
		appendString(*capability.ModelReadOnlyReason)
	}
	if capability.PermissionReadOnlyReason == nil {
		appendString("")
	} else {
		appendString(*capability.PermissionReadOnlyReason)
	}
	digest := sha256.Sum256(data)
	return fmt.Sprintf("sha256:%x", digest[:])
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

func (m *Mapper) messageEvents(messageID string, text string) []protocol.Event {
	return splitMessageEvents(text, maxMessagePayloadBytes, func(part string) protocol.Event {
		return m.messageEvent(messageID, part)
	})
}

func splitMessageEvents(text string, maxPayloadBytes int, event func(string) protocol.Event) []protocol.Event {
	if text == "" || maxPayloadBytes < 1 || event == nil {
		return nil
	}
	remaining := text
	result := make([]protocol.Event, 0, 1)
	for len(remaining) > 0 {
		end := len(remaining)
		if end > maxTextChunkBytes {
			end = maxTextChunkBytes
		}
		end = utf8PrefixEnd(remaining, end)
		if end == 0 {
			end = 1
		}
		for end > 1 && len(event(remaining[:end]).Payload) > maxPayloadBytes {
			end = utf8PrefixEnd(remaining, end/2)
		}
		result = append(result, event(remaining[:end]))
		remaining = remaining[end:]
	}
	return result
}

func utf8PrefixEnd(text string, end int) int {
	if end >= len(text) {
		return len(text)
	}
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return end
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
	if name := firstString(value, "type", "subtype", "sessionUpdate", "kind", "method", "event"); name != "" {
		return name
	}
	return ""
}

// sessionIDFromResponse recognizes the JSON-RPC session/new result emitted by
// providers such as claude-agent-acp. It deliberately requires a result and a
// session ID, so initialize responses and error responses cannot mark an
// adapter ready before a session exists.
func sessionIDFromResponse(value map[string]any) string {
	if _, ok := value["id"]; !ok {
		return ""
	}
	if result := objectField(value, "result"); result != nil {
		return firstString(result, "session_id", "sessionId")
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
