package core

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/winghv/agentwharf/masking"
	"github.com/winghv/agentwharf/protocol"
)

var ErrInvalidEventPayload = errors.New("invalid event payload")

type EventMasker struct {
	masker *masking.Masker
}

func NewEventMasker(secrets []string) *EventMasker {
	return &EventMasker{masker: masking.New(secrets)}
}

func (m *EventMasker) MaskEvent(ev protocol.Event) (protocol.Event, error) {
	if len(ev.Payload) == 0 {
		return cloneEventPayload(ev, nil), nil
	}

	var payload any
	if err := json.Unmarshal(ev.Payload, &payload); err != nil {
		return protocol.Event{}, fmt.Errorf("%w: %w", ErrInvalidEventPayload, err)
	}
	masked := maskJSONStrings(m.masker, payload)
	encoded, err := json.Marshal(masked)
	if err != nil {
		return protocol.Event{}, fmt.Errorf("marshal masked event payload: %w", err)
	}
	return cloneEventPayload(ev, encoded), nil
}

func maskJSONStrings(masker *masking.Masker, value any) any {
	switch typed := value.(type) {
	case map[string]any:
		masked := make(map[string]any, len(typed))
		for key, nested := range typed {
			masked[key] = maskJSONStrings(masker, nested)
		}
		return masked
	case []any:
		masked := make([]any, len(typed))
		for i, nested := range typed {
			masked[i] = maskJSONStrings(masker, nested)
		}
		return masked
	case string:
		return masker.MaskString(typed)
	default:
		return typed
	}
}

func cloneEventPayload(ev protocol.Event, payload json.RawMessage) protocol.Event {
	cloned := protocol.Event{
		Type:      ev.Type,
		SessionID: ev.SessionID,
		Time:      ev.Time,
		Payload:   append(json.RawMessage(nil), payload...),
	}
	if ev.Seq != nil {
		seq := *ev.Seq
		cloned.Seq = &seq
	}
	return cloned
}
