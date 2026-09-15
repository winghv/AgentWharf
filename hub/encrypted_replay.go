package hub

import (
	"encoding/json"
	"errors"

	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

// Validate routing and opaque structure before required history leaves the Hub.
// Signature verification and plaintext interpretation remain endpoint-owned.
func validateRequiredReplayEvent(event store.Event, session string) error {
	if event.SessionID != session || event.Seq < 1 {
		return errors.New("invalid encrypted replay routing")
	}
	if event.Type == "session.command" {
		var header struct {
			Type      string `json:"type"`
			MessageID string `json:"message_id"`
		}
		if json.Unmarshal(event.Payload, &header) != nil {
			return errors.New("invalid encrypted command history")
		}
		carrier, err := protocol.DecodeEncryptedPacketCarrier(event.Payload, "command", header.Type, header.MessageID)
		if err != nil || !store.ValidEncryptedCommandType(carrier.Type) {
			return errors.New("invalid encrypted command history")
		}
		return nil
	}
	// Internal lifecycle events (state/capabilities/outcome) may be Store-generated
	// plaintext records with no content (for example a recovered reservation
	// outcome). They must replay so the client sees no sequence gap; content
	// events still require a sealed carrier.
	if internalLifecycleEvent(event.Type) {
		if _, err := protocol.DecodeEncryptedPacketCarrier(event.Payload, "event", event.Type, ""); err == nil {
			return nil
		}
		var plain map[string]json.RawMessage
		if json.Unmarshal(event.Payload, &plain) == nil {
			return nil
		}
		return errors.New("invalid encrypted event history")
	}
	_, err := protocol.DecodeEncryptedPacketCarrier(event.Payload, "event", event.Type, "")
	if err != nil {
		return errors.New("invalid encrypted event history")
	}
	return nil
}

func internalLifecycleEvent(eventType string) bool {
	// Only the Store-generated recovery outcome is plaintext today; state and
	// capability events for a required Session stay sealed.
	return eventType == "session.run.outcome"
}
