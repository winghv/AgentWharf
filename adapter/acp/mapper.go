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
	"strings"
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
	sessionID         string
	provider          string
	now               func() time.Time
	nextMessageID     uint64
	providerSessionID string
	activeToolCallIDs map[string]struct{}
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
		sessionID:         cfg.SessionID,
		provider:          cfg.Provider,
		now:               now,
		activeToolCallIDs: make(map[string]struct{}),
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
	providerSessionID := firstString(raw, "session_id", "sessionId")
	if providerSessionID == "" {
		if params := objectField(raw, "params"); params != nil {
			providerSessionID = firstString(params, "session_id", "sessionId")
		}
	}
	if providerSessionID != "" {
		m.providerSessionID = providerSessionID
	}
	return m.mapFrame(raw, providerSessionID), nil
}

// Finalize closes provider-side work that was started but never given a result.
// This is used when an ACP process exits or its stdout stream breaks: without a
// terminal event, clients would correctly keep rendering the tool as running.
func (m *Mapper) Finalize(reason string) []protocol.Event {
	if m == nil || len(m.activeToolCallIDs) == 0 {
		return nil
	}
	ids := make([]string, 0, len(m.activeToolCallIDs))
	for id := range m.activeToolCallIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	events := make([]protocol.Event, 0, len(ids)+2)
	for _, id := range ids {
		events = append(events, m.toolCallResultEvent(id, "error", "Provider exited before returning a tool result."))
		delete(m.activeToolCallIDs, id)
	}
	events = append(events,
		m.messageEvent("acp-provider-error", "The Agent could not complete this turn: "+nonEmptyReason(reason, "provider output closed unexpectedly"), m.providerSessionID),
		m.stateEvent("error", m.providerSessionID, map[string]any{"reason": "provider_output_closed", "source": "acp"}),
	)
	return events
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
		if events := m.jsonRPCErrorEvents(raw); len(events) > 0 {
			return events
		}
		if responseSessionID := sessionIDFromResponse(raw); responseSessionID != "" {
			return []protocol.Event{m.stateEvent("ready", responseSessionID, copyWithout(raw, "type", "method", "session_id", "sessionId"))}
		}
		// A successful session/prompt response carries only {stopReason} — no
		// sessionId — so the response-frame path above cannot recognize it.
		// That response is the turn boundary: without publishing ready here the
		// session stays busy forever (the adapter publishes busy at prompt
		// delivery). Initialize/session/new results carry sessionId and are
		// matched by the path above; error responses are handled before this.
		if stopReasonFrame(raw) {
			return []protocol.Event{m.stateEvent("ready", m.providerSessionID, copyWithout(raw, "type", "method", "session_id", "sessionId"))}
		}
		return m.mapUpdate(raw, providerSessionID)
	}
}

// stopReasonFrame reports whether a JSON-RPC frame is a successful response
// carrying a prompt stopReason (session/prompt result). Both the top-level
// shape and the nested result shape are recognized.
func stopReasonFrame(value map[string]any) bool {
	if _, ok := value["stopReason"]; ok {
		return true
	}
	if _, ok := value["stop_reason"]; ok {
		return true
	}
	if result := objectField(value, "result"); result != nil {
		if _, ok := result["stopReason"]; ok {
			return true
		}
		if _, ok := result["stop_reason"]; ok {
			return true
		}
	}
	return false
}

// jsonRPCErrorEvents turns an unmatched Provider JSON-RPC error into visible
// terminal events. Prompt failures otherwise leave the last tool call open:
// the Adapter already acknowledged the Hub command, and an error response has
// no sessionId of its own.
func (m *Mapper) jsonRPCErrorEvents(raw map[string]any) []protocol.Event {
	if _, ok := raw["id"]; !ok || raw["method"] != nil {
		return nil
	}
	errObj := objectField(raw, "error")
	if errObj == nil {
		return nil
	}
	message := firstString(errObj, "message")
	if message == "" {
		message = "unknown error"
	}
	events := m.finishActiveToolCalls("Provider failed before returning a tool result.")
	events = append(events,
		m.messageEvent("acp-rpc-error", "The Agent could not complete this turn: "+message, m.providerSessionID),
		m.stateEvent("error", m.providerSessionID, map[string]any{"reason": "provider_rpc_error", "source": "acp"}),
	)
	return events
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
	case "plan":
		return m.planEvents(update, providerSessionID, nil)
	case "plan_update":
		return m.planUpdateEvents(update, providerSessionID)
	case "plan_removed":
		return m.planRemovedEvents(update, providerSessionID)
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
	case "session_info_update":
		if event := m.providerFailureEvent(update, providerSessionID); event != nil {
			events := []protocol.Event{*event}
			if isProviderFailureEvent(*event) {
				events = append(m.finishActiveToolCalls("Provider failed before returning a tool result."), events...)
				events = append(events, m.stateEvent("error", m.providerSessionID, map[string]any{"reason": "provider_failure", "source": "acp"}))
			}
			return events
		}
		return nil
	case "agent_message_chunk", "prompt_response":
		text := updateText(update)
		if text == "" {
			return nil
		}
		if m.provider == "codex" && isLegacyCodexWarning(text) {
			return []protocol.Event{m.providerWarningEvent(strings.TrimSpace(text), "", providerSessionID)}
		}
		messageID := firstString(update, "message_id", "messageId", "id", "session_id", "sessionId")
		if messageID == "" {
			messageID = m.nextMessageIDValue()
		}
		return m.messageEvents(messageID, text, providerSessionID)
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
		payload := map[string]any{
			"tool_call_id": toolCallID,
			"phase":        "start",
			"name":         name,
			"input":        input,
			"result":       nil,
		}
		if diffs := acpDiffs(update); len(diffs) > 0 {
			payload["diffs"] = diffs
		}
		if title := firstString(update, "title"); title != "" {
			payload["title"] = title
		}
		events := []protocol.Event{m.event("session.tool_call", payload)}
		m.activeToolCallIDs[toolCallID] = struct{}{}
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
	case "":
		// An ACP `tool_call_update` may arrive with no status at all: it
		// enriches the call rather than ending it. The Claude bridge sends
		// the tool's real input, title and file diff that way, once the
		// streamed tool_use block carries its input. Dropping these frames
		// lost every diff the provider reported.
		return m.toolCallEnrichmentEvents(update, toolCallID)
	default:
		return nil
	}

	delete(m.activeToolCallIDs, toolCallID)
	preview, truncated := toolOutputPreview(firstAny(update, "rawOutput", "raw_output", "result", "content"))
	event := m.toolCallResultEventWithOutput(toolCallID, resultStatus, preview, truncated, firstString(update, "name", "kind", "title"))
	// A provider may attach the change it made to the terminal frame instead of
	// the start frame, so both carry `diffs`.
	if diffs := acpDiffs(update); len(diffs) > 0 {
		var payload map[string]any
		if json.Unmarshal(event.Payload, &payload) == nil {
			payload["diffs"] = diffs
			if encoded, err := json.Marshal(payload); err == nil {
				event.Payload = encoded
			}
		}
	}
	return []protocol.Event{event}
}

// toolCallEnrichmentEvents publishes what a status-less `tool_call_update`
// adds to a tool call that is already on screen.
//
// The enrichment is a second `phase=start` frame for the same tool_call_id
// carrying the richer payload, so the durable stream keeps one logical tool
// call with a growing description instead of a separate row. A frame that
// adds nothing is ignored rather than emitted as an empty duplicate.
func (m *Mapper) toolCallEnrichmentEvents(update map[string]any, toolCallID string) []protocol.Event {
	diffs := acpDiffs(update)
	title := firstString(update, "title")
	input := objectOrNil(firstAny(update, "rawInput", "raw_input", "input"))
	if len(diffs) == 0 && title == "" && input == nil {
		return nil
	}
	payload := map[string]any{
		"tool_call_id": toolCallID,
		"phase":        "start",
		"name":         firstString(update, "name", "kind", "title"),
		"input":        input,
		"result":       nil,
	}
	if title != "" {
		payload["title"] = title
	}
	if len(diffs) > 0 {
		payload["diffs"] = diffs
	}
	return []protocol.Event{m.event("session.tool_call", payload)}
}

func (m *Mapper) finishActiveToolCalls(output string) []protocol.Event {
	if len(m.activeToolCallIDs) == 0 {
		return nil
	}
	ids := make([]string, 0, len(m.activeToolCallIDs))
	for id := range m.activeToolCallIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	events := make([]protocol.Event, 0, len(ids))
	for _, id := range ids {
		events = append(events, m.toolCallResultEvent(id, "error", output))
		delete(m.activeToolCallIDs, id)
	}
	return events
}

func (m *Mapper) toolCallResultEvent(toolCallID, status, output string) protocol.Event {
	return m.toolCallResultEventWithOutput(toolCallID, status, output, false, "")
}

func (m *Mapper) toolCallResultEventWithOutput(toolCallID, status, output string, truncated bool, name string) protocol.Event {
	payload := map[string]any{
		"tool_call_id": toolCallID,
		"phase":        "result",
		"input":        nil,
		"result": map[string]any{
			"status":         status,
			"output_preview": output,
			"truncated":      truncated,
		},
	}
	if name != "" {
		payload["name"] = name
	}
	return m.event("session.tool_call", payload)
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
	m.activeToolCallIDs[toolCallID] = struct{}{}
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

func (m *Mapper) messageEvent(messageID, text, providerSessionID string) protocol.Event {
	payload := map[string]any{
		"message_id": messageID,
		"role":       "agent",
		"content": []map[string]any{{
			"kind": "text",
			"text": text,
		}},
	}
	if providerSessionID != "" {
		payload["provider_session_id"] = providerSessionID
	}
	return m.event("session.message", payload)
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

func (m *Mapper) messageEvents(messageID, text, providerSessionID string) []protocol.Event {
	return splitMessageEvents(text, maxMessagePayloadBytes, func(part string) protocol.Event {
		return m.messageEvent(messageID, part, providerSessionID)
	})
}

func isLegacyCodexWarning(text string) bool {
	trimmed := strings.TrimSpace(text)
	return strings.HasPrefix(trimmed, "Warning: Model metadata for ") ||
		strings.HasPrefix(trimmed, "Config warning:")
}

func (m *Mapper) providerWarningEvent(text, warningID, providerSessionID string) protocol.Event {
	payload := map[string]any{
		"kind":                "provider_warning",
		"text":                text,
		"provider_session_id": providerSessionID,
	}
	if warningID != "" {
		payload["warning_id"] = warningID
	}
	return m.event("agent.activity", payload)
}

func isProviderFailureEvent(event protocol.Event) bool {
	var payload map[string]any
	if json.Unmarshal(event.Payload, &payload) != nil {
		return false
	}
	kind, _ := payload["kind"].(string)
	return kind == "provider_failure"
}

func (m *Mapper) providerFailureEvent(update map[string]any, providerSessionID string) *protocol.Event {
	meta := objectField(update, "_meta")
	jetbrains := objectField(meta, "jetbrains")
	air := objectField(jetbrains, "air")
	failure := objectField(air, "sessionFailure")
	if failure == nil {
		return nil
	}
	text := firstString(failure, "title", "message")
	if text == "" {
		return nil
	}
	kind := "provider_warning"
	if firstString(failure, "severity") == "error" {
		kind = "provider_failure"
	}
	warningID := firstString(failure, "id")
	payload := map[string]any{
		"kind":                kind,
		"text":                text,
		"provider_session_id": providerSessionID,
	}
	if warningID != "" {
		payload["warning_id"] = warningID
	}
	if category := firstString(failure, "category"); category != "" {
		payload["category"] = category
	}
	event := m.event("agent.activity", payload)
	return &event
}

func nonEmptyReason(reason, fallback string) string {
	if strings.TrimSpace(reason) != "" {
		return strings.TrimSpace(reason)
	}
	return fallback
}

func (m *Mapper) nextMessageIDValue() string {
	m.nextMessageID++
	return fmt.Sprintf("acp-message-%d", m.nextMessageID)
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

// ── ACP file diffs ───────────────────────────────────────────────────────────
//
// An editing tool reports what it changed as a `diff` block inside the tool
// call's `content`: a host path plus the replaced and inserted text. Bridges
// send one for every Write and Edit, so without this the workbench showed "Edit
// file" and nothing else.
//
// `old_text`/`new_text` are NOT necessarily whole files: the Claude bridge
// sends the search/replace fragments for an Edit and the entire new file for a
// Write. The payload therefore carries the change as given and says so, rather
// than claiming to be a file body.
//
// `Diff.path` is an absolute host path. Only the base name crosses the Adapter
// boundary (L0: host paths are reduced at the boundary, never forwarded); a
// durable transcript would otherwise carry the tenant's account name and
// directory layout to every reader and every replay.
const (
	maxToolCallDiffs       = 8
	maxDiffTextBytes       = 8 * 1024
	maxDiffSideTotalBytes  = 24 * 1024
	maxDiffBaseNameRuneLen = 128
)

func acpDiffs(update map[string]any) []map[string]any {
	raw, ok := firstAny(update, "content").([]any)
	if !ok {
		return nil
	}
	diffs := make([]map[string]any, 0, maxToolCallDiffs)
	budget := maxDiffSideTotalBytes
	for _, item := range raw {
		block, ok := item.(map[string]any)
		if !ok || firstString(block, "type") != "diff" {
			continue
		}
		if len(diffs) >= maxToolCallDiffs {
			break
		}
		name := reducedFileName(firstString(block, "path"))
		if name == "" {
			// Without a name the change cannot be attributed to a file, and an
			// empty card helps nobody.
			continue
		}
		if budget <= 0 {
			// Stop rather than emit empty cards for changes that no longer fit.
			break
		}
		diff := map[string]any{"path_base": truncateRunes(name, maxDiffBaseNameRuneLen)}
		// Split the remaining budget so the inserted side survives: an Edit that
		// only showed what it removed would be worse than no card at all.
		oldLimit := budget / 2
		if oldLimit > maxDiffTextBytes {
			oldLimit = maxDiffTextBytes
		}
		oldText, oldTruncated := boundText(strings.TrimRight(stringFromAny(block["oldText"]), "\n"), oldLimit)
		if oldText != "" {
			diff["old_text"] = oldText
		}
		if oldTruncated {
			diff["old_text_truncated"] = true
		}
		newLimit := budget - len(oldText)
		if newLimit > maxDiffTextBytes {
			newLimit = maxDiffTextBytes
		}
		newText, newTruncated := boundText(strings.TrimRight(stringFromAny(block["newText"]), "\n"), newLimit)
		if newText != "" {
			diff["new_text"] = newText
		}
		if newTruncated {
			diff["new_text_truncated"] = true
		}
		// A diff with no text on either side carries nothing: the provider
		// reported a change it could not describe (codex sends one of these
		// when the edit arrives as a patch without +/- lines). Publishing it
		// would render an empty change card, which reads as a broken card.
		if oldText == "" && newText == "" {
			continue
		}
		budget -= len(oldText) + len(newText)
		diffs = append(diffs, diff)
	}
	if len(diffs) == 0 {
		return nil
	}
	return diffs
}

func truncateRunes(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}

// ── ACP plans ───────────────────────────────────────────────────────────────
//
// ACP reports the agent's execution plan as whole-state updates: `plan`
// carries the complete entry list and the client replaces whatever it had, and
// the experimental `plan_update` / `plan_removed` pair does the same for a
// plan identified by id. The Adapter therefore emits one replace-semantics
// `agent.activity` frame per update and never tries to accumulate or reorder
// entries: the provider owns plan semantics (L0 P3) and the client keeps the
// newest frame.
//
// Both bridges in the deployed image are covered: claude-agent-acp sends
// structured `entries`, codex-acp sends a markdown document under `plan_update`.
const (
	// A plan frame shares the 64 KiB event budget with the envelope, so the
	// entry list is bounded before marshalling instead of being split: a split
	// plan would be read by clients as two different plans.
	maxPlanEntries        = 48
	maxPlanEntryTextBytes = 512
	maxPlanMarkdownBytes  = 16 * 1024
)

func (m *Mapper) planEvents(update map[string]any, providerSessionID string, plan map[string]any) []protocol.Event {
	source := update
	if plan != nil {
		source = plan
	}
	payload := map[string]any{
		"kind":                "plan",
		"provider_session_id": providerSessionID,
	}
	if id := planIdentifier(source); id != "" {
		payload["plan_id"] = id
	}
	switch {
	case source["entries"] != nil:
		entries, truncated := normalizePlanEntries(source["entries"])
		if len(entries) == 0 && !truncated {
			return nil
		}
		payload["entries"] = entries
		if truncated {
			payload["truncated"] = true
		}
	case firstString(source, "content") != "":
		markdown, truncated := boundText(firstString(source, "content"), maxPlanMarkdownBytes)
		if markdown == "" {
			return nil
		}
		payload["markdown"] = markdown
		if truncated {
			payload["truncated"] = true
		}
	case firstString(source, "uri") != "":
		// A file-backed plan is a host path. Only the file name crosses the
		// Adapter boundary (L0: host paths are reduced, never forwarded).
		payload["file_name"] = reducedFileName(firstString(source, "uri"))
		if payload["file_name"] == "" {
			return nil
		}
	default:
		return nil
	}
	return []protocol.Event{m.event("agent.activity", payload)}
}

// planUpdateEvents normalizes the experimental `plan_update` shapes. Unknown
// content types are ignored like any other unrecognized ACP update rather than
// being guessed at.
func (m *Mapper) planUpdateEvents(update map[string]any, providerSessionID string) []protocol.Event {
	plan := objectField(update, "plan")
	if plan == nil {
		return nil
	}
	switch firstString(plan, "type") {
	case "items":
		return m.planEvents(update, providerSessionID, plan)
	case "markdown":
		return m.planEvents(update, providerSessionID, plan)
	case "file":
		return m.planEvents(update, providerSessionID, plan)
	default:
		return nil
	}
}

func (m *Mapper) planRemovedEvents(update map[string]any, providerSessionID string) []protocol.Event {
	payload := map[string]any{
		"kind":                "plan_removed",
		"provider_session_id": providerSessionID,
	}
	if id := planIdentifier(update); id != "" {
		payload["plan_id"] = id
	}
	return []protocol.Event{m.event("agent.activity", payload)}
}

// planIdentifier reads the plan id. Codex emits `planId` where the ACP schema
// spells it `id`, so both are accepted; an empty id means "the current plan".
func planIdentifier(value map[string]any) string {
	return firstString(value, "id", "plan_id", "planId")
}

func normalizePlanEntries(value any) ([]map[string]string, bool) {
	list, ok := value.([]any)
	if !ok {
		return nil, false
	}
	entries := make([]map[string]string, 0, min(len(list), maxPlanEntries))
	truncated := false
	for _, item := range list {
		if len(entries) >= maxPlanEntries {
			truncated = true
			break
		}
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		content, _ := boundText(strings.TrimSpace(stringFromAny(entry["content"])), maxPlanEntryTextBytes)
		if content == "" {
			continue
		}
		// Status and priority keep the provider's own vocabulary so the
		// Adapter never invents execution semantics it does not own.
		entries = append(entries, map[string]string{
			"content":  content,
			"status":   boundedIdentifier(stringFromAny(entry["status"])),
			"priority": boundedIdentifier(stringFromAny(entry["priority"])),
		})
	}
	return entries, truncated
}

// reducedFileName keeps only the last path segment of a plan document
// reference, whether it arrives as a path or as a file:// URI.
func reducedFileName(reference string) string {
	trimmed := strings.TrimSpace(reference)
	if trimmed == "" {
		return ""
	}
	if index := strings.Index(trimmed, "://"); index >= 0 {
		trimmed = trimmed[index+3:]
	}
	trimmed = strings.TrimRight(strings.ReplaceAll(trimmed, "\\", "/"), "/")
	if index := strings.LastIndex(trimmed, "/"); index >= 0 {
		trimmed = trimmed[index+1:]
	}
	name, truncated := boundText(trimmed, maxPlanEntryTextBytes)
	if truncated {
		return ""
	}
	return name
}

func boundedIdentifier(value string) string {
	trimmed := strings.TrimSpace(value)
	if len(trimmed) > 64 {
		return ""
	}
	return trimmed
}

// boundText truncates on a rune boundary so a plan frame can never split a
// multi-byte character, and reports whether anything was dropped.
func boundText(value string, limit int) (string, bool) {
	if value == "" || limit <= 0 {
		return "", value != ""
	}
	if len(value) <= limit {
		return value, false
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}
	return value[:cut], true
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
