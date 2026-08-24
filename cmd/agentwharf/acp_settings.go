package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/winghv/agentwharf/protocol"
	"golang.org/x/text/unicode/norm"
)

const (
	acpSettingsOperationTimeout = 30 * time.Second
	acpSetConfigOptionMethod    = "session/set_config_option"
	acpModelCategory            = "model"
	acpReasoningCategory        = "reasoning_effort"
	acpThoughtLevelCategory     = "thought_level"
	acpPermissionCategory       = "mode"
)

var acpSettingsIdentifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$`)
var errACPSettingsCapabilityChanged = errors.New("acp settings capability changed before provider mutation")
var errACPSettingsReadbackSuperseded = errors.New("acp settings readback was superseded by a newer provider update")

type acpSettingsState struct {
	Capability         protocol.SettingsCapabilityPayload
	ModelConfigID      string
	ReasoningConfigID  string
	PermissionConfigID string
}

type acpSettingsTracker struct {
	mu               sync.Mutex
	current          *acpSettingsState
	mutation         *acpSettingsMutation
	mutationSequence uint64
	sourceSequence   uint64
}

type acpSettingsMutation struct {
	sequence      uint64
	kind          string
	expectedValue string
}

type acpSettingsMutationHandle struct {
	tracker  *acpSettingsTracker
	sequence uint64
	once     sync.Once
}

func newACPSettingsTracker(sessionResult map[string]any) *acpSettingsTracker {
	tracker := &acpSettingsTracker{}
	if state, err := acpSettingsStateFromConfigOptions(sessionResult["configOptions"]); err == nil {
		tracker.current = &state
	}
	return tracker
}

func (t *acpSettingsTracker) Current() (acpSettingsState, bool) {
	if t == nil {
		return acpSettingsState{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.current == nil {
		return acpSettingsState{}, false
	}
	return cloneACPSettingsState(*t.current), true
}

func (t *acpSettingsTracker) beginProviderMutation(expectedFingerprint, kind, value string) (*acpSettingsMutationHandle, acpSettingsState, string, error) {
	if t == nil {
		return nil, acpSettingsState{}, "", errors.New("acp settings tracker is unavailable")
	}
	if !acpSettingsIdentifier.MatchString(value) {
		return nil, acpSettingsState{}, "", errors.New("acp settings mutation value is invalid")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.current == nil {
		return nil, acpSettingsState{}, "", errors.New("acp settings capability is unavailable")
	}
	current := cloneACPSettingsState(*t.current)
	if current.Capability.Fingerprint != expectedFingerprint {
		return nil, current, "", errACPSettingsCapabilityChanged
	}
	if t.mutation != nil {
		return nil, current, "", errors.New("acp settings provider mutation is already pending")
	}
	configID := ""
	switch kind {
	case acpModelCategory:
		configID = current.ModelConfigID
	case acpReasoningCategory:
		configID = current.ReasoningConfigID
	case acpPermissionCategory:
		configID = current.PermissionConfigID
	default:
		return nil, current, "", errors.New("acp settings mutation kind is invalid")
	}
	if !acpSettingsIdentifier.MatchString(configID) {
		return nil, current, "", errors.New("acp settings config id is unavailable")
	}
	t.mutationSequence++
	t.mutation = &acpSettingsMutation{sequence: t.mutationSequence, kind: kind, expectedValue: value}
	return &acpSettingsMutationHandle{tracker: t, sequence: t.mutationSequence}, current, configID, nil
}

func (h *acpSettingsMutationHandle) finish() {
	if h == nil || h.tracker == nil {
		return
	}
	h.once.Do(func() {
		h.tracker.mu.Lock()
		if h.tracker.mutation != nil && h.tracker.mutation.sequence == h.sequence {
			h.tracker.mutation = nil
		}
		h.tracker.mu.Unlock()
	})
}

func (t *acpSettingsTracker) UpdateFromResult(result map[string]any, sourceSequence uint64) (acpSettingsState, bool, error) {
	if t == nil {
		return acpSettingsState{}, false, errors.New("acp settings tracker is unavailable")
	}
	if sourceSequence == 0 {
		return acpSettingsState{}, false, errors.New("acp settings response sequence is invalid")
	}
	state, err := acpSettingsStateFromConfigOptions(result["configOptions"])
	if err != nil {
		return acpSettingsState{}, false, err
	}
	t.mu.Lock()
	if sourceSequence < t.sourceSequence && t.current != nil {
		current := cloneACPSettingsState(*t.current)
		t.mu.Unlock()
		return current, false, nil
	}
	t.current = &state
	t.sourceSequence = sourceSequence
	t.mu.Unlock()
	return cloneACPSettingsState(state), true, nil
}

func (t *acpSettingsTracker) ObserveProviderLine(line []byte, providerSessionID string, sourceSequence uint64) (acpSettingsState, bool, error) {
	if t == nil {
		return acpSettingsState{}, false, nil
	}
	if sourceSequence == 0 {
		return acpSettingsState{}, false, errors.New("acp settings update sequence is invalid")
	}
	var envelope map[string]any
	if json.Unmarshal(line, &envelope) != nil || envelope["method"] != "session/update" {
		return acpSettingsState{}, false, nil
	}
	params, _ := envelope["params"].(map[string]any)
	if params == nil || stringFieldFromAny(params["sessionId"]) != providerSessionID {
		return acpSettingsState{}, false, nil
	}
	update, _ := params["update"].(map[string]any)
	if update == nil {
		return acpSettingsState{}, false, nil
	}
	switch stringFieldFromAny(update["sessionUpdate"]) {
	case "config_option_update":
		state, err := acpSettingsStateFromConfigOptions(update["configOptions"])
		if err != nil {
			return acpSettingsState{}, false, err
		}
		t.mu.Lock()
		if sourceSequence < t.sourceSequence && t.current != nil {
			current := cloneACPSettingsState(*t.current)
			t.mu.Unlock()
			return current, false, nil
		}
		changed := t.current == nil || t.current.Capability.Fingerprint != state.Capability.Fingerprint
		t.current = &state
		t.sourceSequence = sourceSequence
		t.mu.Unlock()
		return cloneACPSettingsState(state), changed, nil
	case "current_mode_update":
		currentModeID := stringFieldFromAny(update["currentModeId"])
		if !acpSettingsIdentifier.MatchString(currentModeID) {
			return acpSettingsState{}, false, errors.New("acp current_mode_update id is invalid")
		}
		t.mu.Lock()
		if t.current == nil {
			t.mu.Unlock()
			return acpSettingsState{}, false, nil
		}
		current := cloneACPSettingsState(*t.current)
		var mutation acpSettingsMutation
		mutationPending := t.mutation != nil
		if mutationPending {
			mutation = *t.mutation
		}
		t.mu.Unlock()
		if mutationPending {
			if mutation.kind == acpPermissionCategory && currentModeID != mutation.expectedValue {
				return acpSettingsState{}, false, errors.New("acp current_mode_update does not match the pending permission mutation")
			}
			// Claude ACP emits current_mode_update before the full
			// session/set_config_option response. During a mutation the response is
			// the authoritative readback; accepting this notification only as an
			// ordering signal avoids publishing a partially reconstructed tuple.
			return current, false, nil
		}
		if currentModeID == current.Capability.EffectivePermissionModeID {
			return current, false, nil
		}
		// Once mutable controls have been advertised, an incremental mode-only
		// update is not enough to prove the complete effective settings tuple.
		// Outside a fenced Provider mutation, force a reconnect so session/new can
		// provide a fresh configOptions readback instead of publishing a partly
		// reconstructed capability.
		return acpSettingsState{}, false, errors.New("acp current_mode_update lacks full configOptions readback")
	default:
		return acpSettingsState{}, false, nil
	}
}

func (t *acpSettingsTracker) MarkReadOnly(reason string) (acpSettingsState, bool) {
	if t == nil || !validACPSettingsReason(reason) {
		return acpSettingsState{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.current == nil {
		return acpSettingsState{}, false
	}
	state := cloneACPSettingsState(*t.current)
	state.Capability.ModelChange = "read_only"
	state.Capability.PermissionChange = "read_only"
	state.Capability.ModelReadOnlyReason = stringPointer(reason)
	state.Capability.PermissionReadOnlyReason = stringPointer(reason)
	if state.Capability.ReasoningEffortChange != "unsupported" {
		state.Capability.ReasoningEffortChange = "read_only"
		state.Capability.ReasoningEffortReadOnlyReason = stringPointer(reason)
	}
	state.Capability.Fingerprint = protocol.SettingsCapabilityFingerprint(state.Capability)
	t.current = &state
	return cloneACPSettingsState(state), true
}

func acpSettingsStateFromConfigOptions(value any) (acpSettingsState, error) {
	options := objectSlice(value)
	if len(options) == 0 {
		return acpSettingsState{}, errors.New("acp settings config options are unavailable")
	}
	modelOption := findACPSelectOption(options, acpModelCategory)
	permissionOption := findACPSelectOption(options, acpPermissionCategory)
	if modelOption == nil || permissionOption == nil {
		return acpSettingsState{}, errors.New("acp model or permission config option is unavailable")
	}
	modelID := stringFieldFromAny(modelOption["currentValue"])
	permissionID := stringFieldFromAny(permissionOption["currentValue"])
	modelConfigID := stringFieldFromAny(modelOption["id"])
	permissionConfigID := stringFieldFromAny(permissionOption["id"])
	if !acpSettingsIdentifier.MatchString(modelConfigID) || !acpSettingsIdentifier.MatchString(permissionConfigID) || modelConfigID == permissionConfigID {
		return acpSettingsState{}, errors.New("acp settings config ids are invalid")
	}
	models, err := normalizedACPSettingsChoices(modelOption["options"], modelID, 32)
	if err != nil {
		return acpSettingsState{}, fmt.Errorf("acp model settings: %w", err)
	}
	permissions, err := normalizedACPSettingsChoices(permissionOption["options"], permissionID, 16)
	if err != nil {
		return acpSettingsState{}, fmt.Errorf("acp permission settings: %w", err)
	}
	modelChange, modelReason := acpSettingsChangeMode(len(models))
	permissionChange, permissionReason := acpSettingsChangeMode(len(permissions))
	reasoningEfforts := make([]protocol.SettingsCapabilityChoice, 0)
	var effectiveReasoningEffortID *string
	reasoningChange := "unsupported"
	reasoningReason := stringPointer("provider_unsupported")
	reasoningConfigID := ""
	if reasoningOption := findACPReasoningOption(options); reasoningOption != nil {
		reasoningID := stringFieldFromAny(reasoningOption["currentValue"])
		reasoningConfigID = stringFieldFromAny(reasoningOption["id"])
		if !acpSettingsIdentifier.MatchString(reasoningConfigID) || reasoningConfigID == modelConfigID || reasoningConfigID == permissionConfigID {
			return acpSettingsState{}, errors.New("acp reasoning config id is invalid")
		}
		reasoningEfforts, err = normalizedACPSettingsChoices(reasoningOption["options"], reasoningID, 16)
		if err != nil {
			return acpSettingsState{}, fmt.Errorf("acp reasoning settings: %w", err)
		}
		effectiveReasoningEffortID = stringPointer(reasoningID)
		reasoningChange, reasoningReason = acpSettingsChangeMode(len(reasoningEfforts))
	}
	capability := protocol.SettingsCapabilityPayload{
		SchemaVersion:                 protocol.SettingsCapabilitySchemaVersion,
		Models:                        models,
		ReasoningEfforts:              reasoningEfforts,
		PermissionModes:               permissions,
		EffectiveModelID:              modelID,
		EffectiveReasoningEffortID:    effectiveReasoningEffortID,
		EffectivePermissionModeID:     permissionID,
		ModelChange:                   modelChange,
		ReasoningEffortChange:         reasoningChange,
		PermissionChange:              permissionChange,
		ModelReadOnlyReason:           modelReason,
		ReasoningEffortReadOnlyReason: reasoningReason,
		PermissionReadOnlyReason:      permissionReason,
	}
	capability.Fingerprint = protocol.SettingsCapabilityFingerprint(capability)
	encoded, err := json.Marshal(capability)
	if err != nil {
		return acpSettingsState{}, err
	}
	if _, err := protocol.DecodeSettingsCapabilityPayload(encoded); err != nil {
		return acpSettingsState{}, fmt.Errorf("acp settings capability is not canonical: %w", err)
	}
	return acpSettingsState{
		Capability:         capability,
		ModelConfigID:      modelConfigID,
		ReasoningConfigID:  reasoningConfigID,
		PermissionConfigID: permissionConfigID,
	}, nil
}

func findACPSelectOption(options []map[string]any, category string) map[string]any {
	for _, option := range options {
		if stringFieldFromAny(option["type"]) == "select" && stringFieldFromAny(option["category"]) == category {
			return option
		}
	}
	for _, option := range options {
		if stringFieldFromAny(option["type"]) == "select" && stringFieldFromAny(option["id"]) == category {
			return option
		}
	}
	return nil
}

func findACPReasoningOption(options []map[string]any) map[string]any {
	for _, option := range options {
		if stringFieldFromAny(option["type"]) == "select" && stringFieldFromAny(option["category"]) == acpThoughtLevelCategory {
			return option
		}
	}
	// ACP categories are optional metadata. These are the exact select IDs used
	// by the maintained Claude and Codex ACP Providers; no synthetic control or
	// choice is created when none of them is present.
	for _, knownID := range []string{"reasoning_effort", "effort", "thought_level"} {
		for _, option := range options {
			if stringFieldFromAny(option["type"]) == "select" && stringFieldFromAny(option["id"]) == knownID {
				return option
			}
		}
	}
	return nil
}

func normalizedACPSettingsChoices(value any, currentID string, maximum int) ([]protocol.SettingsCapabilityChoice, error) {
	if !acpSettingsIdentifier.MatchString(currentID) {
		return nil, errors.New("effective value is invalid")
	}
	choicesByID := make(map[string]protocol.SettingsCapabilityChoice)
	var visit func(any)
	visit = func(raw any) {
		for _, option := range objectSlice(raw) {
			if nested := option["options"]; nested != nil && stringFieldFromAny(option["value"]) == "" {
				visit(nested)
				continue
			}
			id := stringFieldFromAny(option["value"])
			if !acpSettingsIdentifier.MatchString(id) {
				continue
			}
			label := normalizedACPSettingsLabel(stringFieldFromAny(option["name"]), id)
			if _, exists := choicesByID[id]; !exists {
				choicesByID[id] = protocol.SettingsCapabilityChoice{ID: id, Label: label}
			}
		}
	}
	visit(value)
	if _, found := choicesByID[currentID]; !found {
		return nil, errors.New("effective value is not in the advertised options")
	}
	ids := make([]string, 0, len(choicesByID))
	for id := range choicesByID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) > maximum {
		bounded := make([]string, 0, maximum)
		bounded = append(bounded, currentID)
		for _, id := range ids {
			if id == currentID {
				continue
			}
			bounded = append(bounded, id)
			if len(bounded) == maximum {
				break
			}
		}
		ids = bounded
		sort.Strings(ids)
	}
	choices := make([]protocol.SettingsCapabilityChoice, 0, len(ids))
	for _, id := range ids {
		choices = append(choices, choicesByID[id])
	}
	return choices, nil
}

func normalizedACPSettingsLabel(value, fallback string) string {
	value = norm.NFC.String(strings.TrimSpace(value))
	if value == "" || len(value) > 128 || !utf8.ValidString(value) {
		return fallback
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return fallback
		}
	}
	return value
}

func acpSettingsChangeMode(choiceCount int) (string, *string) {
	if choiceCount > 1 {
		return "allowed", nil
	}
	return "read_only", stringPointer("provider_read_only")
}

func cloneACPSettingsState(state acpSettingsState) acpSettingsState {
	clone := state
	clone.Capability.Models = append([]protocol.SettingsCapabilityChoice(nil), state.Capability.Models...)
	clone.Capability.ReasoningEfforts = append([]protocol.SettingsCapabilityChoice(nil), state.Capability.ReasoningEfforts...)
	clone.Capability.PermissionModes = append([]protocol.SettingsCapabilityChoice(nil), state.Capability.PermissionModes...)
	if state.Capability.EffectiveReasoningEffortID != nil {
		clone.Capability.EffectiveReasoningEffortID = stringPointer(*state.Capability.EffectiveReasoningEffortID)
	}
	if state.Capability.ModelReadOnlyReason != nil {
		clone.Capability.ModelReadOnlyReason = stringPointer(*state.Capability.ModelReadOnlyReason)
	}
	if state.Capability.ReasoningEffortReadOnlyReason != nil {
		clone.Capability.ReasoningEffortReadOnlyReason = stringPointer(*state.Capability.ReasoningEffortReadOnlyReason)
	}
	if state.Capability.PermissionReadOnlyReason != nil {
		clone.Capability.PermissionReadOnlyReason = stringPointer(*state.Capability.PermissionReadOnlyReason)
	}
	return clone
}

func objectSlice(value any) []map[string]any {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	objects := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if object, ok := item.(map[string]any); ok {
			objects = append(objects, object)
		}
	}
	return objects
}

func settingsChoiceContains(choices []protocol.SettingsCapabilityChoice, id string) bool {
	for _, choice := range choices {
		if choice.ID == id {
			return true
		}
	}
	return false
}

func validateACPSettingsChange(state acpSettingsState, change protocol.SettingsChange) string {
	if change.CapabilityFingerprint != state.Capability.Fingerprint {
		return "stale_capability"
	}
	if change.RequestedModelID != nil {
		if state.Capability.ModelChange != "allowed" {
			return "settings_read_only"
		}
		if !settingsChoiceContains(state.Capability.Models, *change.RequestedModelID) {
			return "settings_unavailable"
		}
	}
	if change.RequestedReasoningEffortID != nil {
		if state.Capability.ReasoningEffortChange != "allowed" || state.Capability.EffectiveReasoningEffortID == nil {
			return "settings_read_only"
		}
		if !settingsChoiceContains(state.Capability.ReasoningEfforts, *change.RequestedReasoningEffortID) {
			return "settings_unavailable"
		}
	}
	if change.RequestedPermissionModeID != nil {
		if state.Capability.PermissionChange != "allowed" {
			return "settings_read_only"
		}
		if !settingsChoiceContains(state.Capability.PermissionModes, *change.RequestedPermissionModeID) {
			return "settings_unavailable"
		}
	}
	return ""
}

type acpSettingsReservation struct {
	Command  protocol.Command
	Change   protocol.SettingsChange
	Reserved acpSettingsState
	Deadline time.Time
}

type acpRPCResponse struct {
	result         map[string]any
	settingsState  *acpSettingsState
	sourceSequence uint64
	err            error
}

type acpRPCResponseHandler func(*acpRPCResponse)

type acpProviderRPCError struct {
	message string
}

func (e acpProviderRPCError) Error() string { return e.message }

type acpResponseRouter struct {
	mu      sync.Mutex
	pending map[string]acpPendingResponse
}

type acpPendingResponse struct {
	channel chan acpRPCResponse
	handler acpRPCResponseHandler
}

func newACPResponseRouter() *acpResponseRouter {
	return &acpResponseRouter{pending: make(map[string]acpPendingResponse)}
}

func (r *acpResponseRouter) register(id int64) (<-chan acpRPCResponse, func(), error) {
	return r.registerWithHandler(id, nil)
}

func (r *acpResponseRouter) registerWithHandler(id int64, handler acpRPCResponseHandler) (<-chan acpRPCResponse, func(), error) {
	if r == nil || id < 1 {
		return nil, nil, errors.New("invalid acp response registration")
	}
	key := acpNumericRPCIDKey(id)
	channel := make(chan acpRPCResponse, 1)
	r.mu.Lock()
	if _, exists := r.pending[key]; exists {
		r.mu.Unlock()
		return nil, nil, errors.New("duplicate acp response registration")
	}
	pending := acpPendingResponse{channel: channel, handler: handler}
	r.pending[key] = pending
	r.mu.Unlock()
	cancel := func() {
		r.mu.Lock()
		if registered, found := r.pending[key]; found && registered.channel == channel {
			delete(r.pending, key)
		}
		r.mu.Unlock()
	}
	return channel, cancel, nil
}

func (r *acpResponseRouter) Deliver(line []byte, sourceSequence uint64) bool {
	if r == nil {
		return false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(line, &fields) != nil || fields["id"] == nil || fields["method"] != nil || (fields["result"] == nil && fields["error"] == nil) {
		return false
	}
	key, ok := acpRPCIDKey(fields["id"])
	if !ok {
		return false
	}
	r.mu.Lock()
	pending, found := r.pending[key]
	if found {
		delete(r.pending, key)
	}
	r.mu.Unlock()
	if !found {
		return false
	}
	response := acpRPCResponse{sourceSequence: sourceSequence}
	if rawError := fields["error"]; rawError != nil && string(rawError) != "null" {
		if fields["result"] != nil {
			response.err = errors.New("acp response has both result and error")
		} else {
			response.err = acpProviderRPCError{message: fmt.Sprintf("acp request %s failed: %s", key, compactJSON(rawError))}
		}
	} else if fields["result"] == nil || json.Unmarshal(fields["result"], &response.result) != nil {
		response.err = errors.New("acp response missing result")
	}
	if pending.handler != nil {
		pending.handler(&response)
	}
	pending.channel <- response
	return true
}

func acpNumericRPCIDKey(id int64) string {
	return fmt.Sprintf("n:%d", id)
}

func acpRPCIDKey(raw json.RawMessage) (string, bool) {
	var numeric int64
	if json.Unmarshal(raw, &numeric) == nil {
		return acpNumericRPCIDKey(numeric), numeric >= 0
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return "s:" + text, text != ""
	}
	return "", false
}

type acpSettingsExecution struct {
	State             acpSettingsState
	Outcome           string
	ReasonCode        *string
	PublishResult     bool
	TerminateProvider bool
}

func executeACPSettingsChange(
	ctx context.Context,
	reservation acpSettingsReservation,
	providerSessionID string,
	stdin io.Writer,
	responses *acpResponseRouter,
	tracker *acpSettingsTracker,
	settingsMu *sync.Mutex,
	nextID *int64,
) acpSettingsExecution {
	if tracker == nil || settingsMu == nil || responses == nil || nextID == nil {
		return acpSettingsExecution{State: cloneACPSettingsState(reservation.Reserved), TerminateProvider: true}
	}
	settingsMu.Lock()
	state, available := tracker.Current()
	settingsMu.Unlock()
	if !available {
		return acpSettingsExecution{State: cloneACPSettingsState(reservation.Reserved), TerminateProvider: true}
	}
	if state.Capability.Fingerprint != reservation.Reserved.Capability.Fingerprint {
		return staleACPSettingsExecution(state)
	}
	appliedOperation := false
	apply := func(kind, value string) (acpSettingsState, error) {
		settingsMu.Lock()
		mutation, current, configID, err := tracker.beginProviderMutation(state.Capability.Fingerprint, kind, value)
		if err != nil {
			settingsMu.Unlock()
			return current, err
		}
		id := *nextID
		*nextID = *nextID + 1
		channel, cancelResponse, err := responses.registerWithHandler(id, func(response *acpRPCResponse) {
			defer mutation.finish()
			if response.err != nil {
				return
			}
			updated, accepted, updateErr := tracker.UpdateFromResult(response.result, response.sourceSequence)
			if updateErr != nil {
				response.err = updateErr
				return
			}
			response.settingsState = &updated
			if !accepted {
				response.err = errACPSettingsReadbackSuperseded
			}
		})
		if err != nil {
			mutation.finish()
			settingsMu.Unlock()
			return current, err
		}
		if err := writeACPRequest(stdin, id, acpSetConfigOptionMethod, map[string]any{
			"sessionId": providerSessionID,
			"configId":  configID,
			"value":     value,
		}); err != nil {
			cancelResponse()
			mutation.finish()
			settingsMu.Unlock()
			return current, err
		}
		settingsMu.Unlock()
		select {
		case <-ctx.Done():
			cancelResponse()
			mutation.finish()
			return current, ctx.Err()
		case response := <-channel:
			cancelResponse()
			if response.err != nil {
				return current, response.err
			}
			if response.settingsState == nil {
				return current, errors.New("acp settings response has no authoritative readback")
			}
			return cloneACPSettingsState(*response.settingsState), nil
		}
	}
	if requested := reservation.Change.RequestedModelID; requested != nil && *requested != state.Capability.EffectiveModelID {
		if state.Capability.ModelChange != "allowed" || state.ModelConfigID == "" {
			return mismatchedACPSettingsExecution(state)
		}
		updated, err := apply(acpModelCategory, *requested)
		if err != nil {
			if errors.Is(err, errACPSettingsReadbackSuperseded) {
				return mismatchedACPSettingsExecution(updated)
			}
			if errors.Is(err, errACPSettingsCapabilityChanged) {
				if appliedOperation {
					return mismatchedACPSettingsExecution(updated)
				}
				return staleACPSettingsExecution(updated)
			}
			return failedACPSettingsExecution(err, state, reservation.Reserved, appliedOperation)
		}
		state = updated
		appliedOperation = true
	}
	if requested := reservation.Change.RequestedReasoningEffortID; requested != nil && (state.Capability.EffectiveReasoningEffortID == nil || *requested != *state.Capability.EffectiveReasoningEffortID) {
		if state.Capability.ReasoningEffortChange != "allowed" || state.ReasoningConfigID == "" || !settingsChoiceContains(state.Capability.ReasoningEfforts, *requested) {
			return mismatchedACPSettingsExecution(state)
		}
		updated, err := apply(acpReasoningCategory, *requested)
		if err != nil {
			if errors.Is(err, errACPSettingsReadbackSuperseded) {
				return mismatchedACPSettingsExecution(updated)
			}
			if errors.Is(err, errACPSettingsCapabilityChanged) {
				if appliedOperation {
					return mismatchedACPSettingsExecution(updated)
				}
				return staleACPSettingsExecution(updated)
			}
			return failedACPSettingsExecution(err, state, reservation.Reserved, appliedOperation)
		}
		state = updated
		appliedOperation = true
	}
	if requested := reservation.Change.RequestedPermissionModeID; requested != nil && *requested != state.Capability.EffectivePermissionModeID {
		if state.Capability.PermissionChange != "allowed" || state.PermissionConfigID == "" || !settingsChoiceContains(state.Capability.PermissionModes, *requested) {
			return mismatchedACPSettingsExecution(state)
		}
		updated, err := apply(acpPermissionCategory, *requested)
		if err != nil {
			if errors.Is(err, errACPSettingsReadbackSuperseded) {
				return mismatchedACPSettingsExecution(updated)
			}
			if errors.Is(err, errACPSettingsCapabilityChanged) {
				if appliedOperation {
					return mismatchedACPSettingsExecution(updated)
				}
				return staleACPSettingsExecution(updated)
			}
			return failedACPSettingsExecution(err, state, reservation.Reserved, appliedOperation)
		}
		state = updated
	}
	modelMatches := reservation.Change.RequestedModelID == nil && state.Capability.EffectiveModelID == reservation.Reserved.Capability.EffectiveModelID ||
		reservation.Change.RequestedModelID != nil && state.Capability.EffectiveModelID == *reservation.Change.RequestedModelID
	reasoningMatches := reservation.Change.RequestedReasoningEffortID == nil && sameOptionalString(state.Capability.EffectiveReasoningEffortID, reservation.Reserved.Capability.EffectiveReasoningEffortID) ||
		reservation.Change.RequestedReasoningEffortID != nil && state.Capability.EffectiveReasoningEffortID != nil && *state.Capability.EffectiveReasoningEffortID == *reservation.Change.RequestedReasoningEffortID
	permissionMatches := reservation.Change.RequestedPermissionModeID == nil && state.Capability.EffectivePermissionModeID == reservation.Reserved.Capability.EffectivePermissionModeID ||
		reservation.Change.RequestedPermissionModeID != nil && state.Capability.EffectivePermissionModeID == *reservation.Change.RequestedPermissionModeID
	if !modelMatches || !reasoningMatches || !permissionMatches {
		return mismatchedACPSettingsExecution(state)
	}
	return acpSettingsExecution{State: state, Outcome: "applied", PublishResult: true}
}

func failedACPSettingsExecution(err error, state, reserved acpSettingsState, applied bool) acpSettingsExecution {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		// A timed-out Provider may still have crossed its mutation boundary. The
		// old state is not a fresh readback, so publish no terminal proposal and
		// terminate the Provider. Hub writer-loss recovery owns the eventual
		// outcome_unknown after a replacement Adapter publishes fresh truth.
		return acpSettingsExecution{State: state, TerminateProvider: true}
	}
	var providerError acpProviderRPCError
	if errors.As(err, &providerError) {
		if !applied {
			return acpSettingsExecution{State: reserved, Outcome: "rejected", ReasonCode: stringPointer("provider_rejected"), PublishResult: true}
		}
		// A prior operation was read back successfully and this operation was
		// explicitly rejected before acceptance. Publish that actual partial state
		// as a mismatch instead of pretending the whole request was rejected.
		return mismatchedACPSettingsExecution(state)
	}
	// Missing or invalid configOptions means the Provider mutation result is not
	// provable. Keep the old capability unpublished and force reconnect/readback.
	return acpSettingsExecution{State: state, TerminateProvider: true}
}

func mismatchedACPSettingsExecution(state acpSettingsState) acpSettingsExecution {
	return acpSettingsExecution{State: state, Outcome: "mismatched_effective", ReasonCode: stringPointer("provider_mismatched_effective"), PublishResult: true}
}

func staleACPSettingsExecution(state acpSettingsState) acpSettingsExecution {
	return acpSettingsExecution{State: state, Outcome: "stale_capability", ReasonCode: stringPointer("stale_capability"), PublishResult: true}
}

func reconcileACPSettingsExecution(execution acpSettingsExecution, current acpSettingsState) acpSettingsExecution {
	if execution.State.Capability.Fingerprint == current.Capability.Fingerprint {
		return execution
	}
	execution.State = current
	if execution.Outcome != "stale_capability" {
		execution.Outcome = "mismatched_effective"
		execution.ReasonCode = stringPointer("provider_mismatched_effective")
	}
	return execution
}

// applyACPLaunchSettings applies the requested launch settings to the freshly
// created ACP session before the Adapter publishes its initial capability and
// marks the Provider ready. It is best-effort: each requested value is validated
// against the advertised capability and any unsupported, unavailable, or
// provider-rejected value falls back to the provider default with a warning
// instead of failing the launch. The tracker is updated from the authoritative
// set_config_option readback so the subsequently published capability reflects
// the effective settings.
func applyACPLaunchSettings(ctx context.Context, tracker *acpSettingsTracker, providerSessionID string, stdin io.Writer, scanner *bufio.Scanner, settings wrapLaunchSettings, warn io.Writer) {
	if tracker == nil || stdin == nil || scanner == nil || !settings.requested() {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, acpSettingsOperationTimeout)
	defer cancel()

	state, available := tracker.Current()
	if !available {
		warnLaunchSettingSkipped(warn, "settings", "", "provider capability unavailable")
		return
	}

	type launchMutation struct {
		kind     string
		value    string
		configID string
	}
	var mutations []launchMutation
	request := func(kind, value, configID string, allowed, contains, matches bool) {
		if value == "" {
			return
		}
		if !allowed {
			warnLaunchSettingSkipped(warn, kind, value, "provider_read_only")
			return
		}
		if !contains {
			warnLaunchSettingSkipped(warn, kind, value, "settings_unavailable")
			return
		}
		if matches {
			return
		}
		if configID == "" {
			warnLaunchSettingSkipped(warn, kind, value, "settings_unavailable")
			return
		}
		mutations = append(mutations, launchMutation{kind: kind, value: value, configID: configID})
	}

	request(acpModelCategory, settings.ModelID, state.ModelConfigID,
		state.Capability.ModelChange == "allowed", settingsChoiceContains(state.Capability.Models, settings.ModelID),
		settings.ModelID == state.Capability.EffectiveModelID)
	request(acpReasoningCategory, settings.ReasoningEffortID, state.ReasoningConfigID,
		state.Capability.ReasoningEffortChange == "allowed", settingsChoiceContains(state.Capability.ReasoningEfforts, settings.ReasoningEffortID),
		state.Capability.EffectiveReasoningEffortID != nil && settings.ReasoningEffortID == *state.Capability.EffectiveReasoningEffortID)
	request(acpPermissionCategory, settings.PermissionModeID, state.PermissionConfigID,
		state.Capability.PermissionChange == "allowed", settingsChoiceContains(state.Capability.PermissionModes, settings.PermissionModeID),
		settings.PermissionModeID == state.Capability.EffectivePermissionModeID)

	var nextID int64
	var sequence uint64
	for _, mutation := range mutations {
		if err := ctx.Err(); err != nil {
			warnLaunchSettingSkipped(warn, mutation.kind, mutation.value, err.Error())
			return
		}
		nextID++
		if err := writeACPRequest(stdin, nextID, acpSetConfigOptionMethod, map[string]any{
			"sessionId": providerSessionID,
			"configId":  mutation.configID,
			"value":     mutation.value,
		}); err != nil {
			warnLaunchSettingSkipped(warn, mutation.kind, mutation.value, err.Error())
			return
		}
		result, err := readACPResponse(ctx, scanner, nextID)
		if err != nil {
			warnLaunchSettingSkipped(warn, mutation.kind, mutation.value, err.Error())
			return
		}
		sequence++
		if _, _, err := tracker.UpdateFromResult(result, sequence); err != nil {
			warnLaunchSettingSkipped(warn, mutation.kind, mutation.value, err.Error())
			return
		}
	}
}

func warnLaunchSettingSkipped(warn io.Writer, kind, value, reason string) {
	if warn == nil {
		return
	}
	if value != "" {
		_, _ = fmt.Fprintf(warn, "wharf: launch setting %s=%s was not applied: %s\n", kind, value, reason)
		return
	}
	_, _ = fmt.Fprintf(warn, "wharf: launch settings were not applied: %s\n", reason)
}

func publishACPSettingsCapability(writeFrame func(protocol.Frame) error, sessionID string, state acpSettingsState) error {
	event, err := newACPSettingsCapabilityEvent(sessionID, state)
	if err != nil {
		return err
	}
	if writeFrame == nil || event == nil {
		return errors.New("settings capability publisher is unavailable")
	}
	return writeFrame(event)
}

func newACPSettingsCapabilityEvent(sessionID string, state acpSettingsState) (*protocol.Event, error) {
	if sessionID == "" {
		return nil, errors.New("settings capability publisher is unavailable")
	}
	payload, err := json.Marshal(state.Capability)
	if err != nil {
		return nil, err
	}
	if _, err := protocol.DecodeSettingsCapabilityPayload(payload); err != nil {
		return nil, err
	}
	proposalID, err := randomToken()
	if err != nil {
		return nil, err
	}
	return &protocol.Event{
		Type: "session.settings.capabilities", SessionID: sessionID,
		Time: time.Now().UTC().UnixMilli(), Payload: payload, ProposalID: proposalID,
	}, nil
}

func publishACPSettingsEffective(writeFrame func(protocol.Frame) error, sessionID string, reservation acpSettingsReservation, execution acpSettingsExecution) error {
	if writeFrame == nil || sessionID == "" {
		return errors.New("settings effective publisher is unavailable")
	}
	payload, err := json.Marshal(map[string]any{
		"cmd_id":                        reservation.Command.CommandID,
		"request_fingerprint":           reservation.Change.CapabilityFingerprint,
		"effective_fingerprint":         execution.State.Capability.Fingerprint,
		"outcome":                       execution.Outcome,
		"effective_model_id":            execution.State.Capability.EffectiveModelID,
		"effective_reasoning_effort_id": execution.State.Capability.EffectiveReasoningEffortID,
		"effective_permission_mode_id":  execution.State.Capability.EffectivePermissionModeID,
		"reason_code":                   execution.ReasonCode,
	})
	if err != nil {
		return err
	}
	if _, err := protocol.DecodeSettingsEffectivePayload(payload); err != nil {
		return err
	}
	proposalID, err := randomToken()
	if err != nil {
		return err
	}
	return writeFrame(&protocol.Event{
		Type: "session.settings.effective", SessionID: sessionID,
		Time: time.Now().UTC().UnixMilli(), Payload: payload, ProposalID: proposalID,
	})
}

func validACPSettingsReason(value string) bool {
	if value == "" || value[0] < 'a' || value[0] > 'z' || len(value) > 64 {
		return false
	}
	for _, r := range value[1:] {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}

func stringPointer(value string) *string { return &value }

func sameOptionalString(left, right *string) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}

func compactJSON(value json.RawMessage) string {
	var compacted bytes.Buffer
	if json.Compact(&compacted, value) == nil {
		return compacted.String()
	}
	return "invalid_error"
}
