package postgres

import (
	"encoding/json"

	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

func eventProjection(event store.PendingEvent, encrypted bool) (attentionProjection, error) {
	if !encrypted {
		return attentionEventProjection(event), nil
	}
	if event.Type == "session.command" {
		var header struct {
			Type      string `json:"type"`
			MessageID string `json:"message_id"`
		}
		if err := json.Unmarshal(event.Payload, &header); err != nil {
			return attentionProjection{}, err
		}
		if _, err := protocol.DecodeEncryptedPacketCarrier(event.Payload, "command", header.Type, header.MessageID); err != nil {
			return attentionProjection{}, err
		}
		if !store.ValidEncryptedCommandType(header.Type) {
			return attentionProjection{}, protocol.ErrEncryptedPacket
		}
		// A client request is not an endpoint-confirmed permission or state outcome.
		return attentionProjection{}, nil
	}
	carrier, err := protocol.DecodeEncryptedPacketCarrier(event.Payload, "event", event.Type, "")
	if err != nil {
		return attentionProjection{}, err
	}
	// Only the bounded public projection is interpreted. The original event
	// payload, including ciphertext and signature, is passed unchanged to SQL.
	projection, err := json.Marshal(carrier.Packet.Public)
	if err != nil {
		return attentionProjection{}, err
	}
	return attentionEventProjection(store.PendingEvent{Type: event.Type, Time: event.Time, Payload: projection}), nil
}
