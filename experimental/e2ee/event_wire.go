package e2ee

import (
	"context"
	"encoding/json"
)

// EventWire preserves the endpoint-assigned message identity independently of
// Hub seq. Hub seq is allocated only after persistence; it is not a signing ID.
type EventWire struct {
	Version   int           `json:"version"`
	Scope     string        `json:"scope"`
	KeyID     string        `json:"key_id"`
	Sender    string        `json:"sender"`
	MessageID string        `json:"message_id"`
	Type      string        `json:"type"`
	Packet    ContentPacket `json:"packet"`
}

// SealEventWire retains the vault's durable seal reservation. Its result must
// be kept unchanged for retries rather than re-sealing under the same ID.
func (v *SessionKeyVault) SealEventWire(ctx context.Context, event Context, payload json.RawMessage) (EventWire, error) {
	packet, err := v.SealEvent(ctx, event, payload)
	if err != nil {
		return EventWire{}, err
	}
	return EventWire{Version: 1, Scope: "event", KeyID: event.KeyID, Sender: event.Sender, MessageID: event.MessageID, Type: event.Type, Packet: packet}, nil
}
