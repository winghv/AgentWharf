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
	// outcome, or a session the platform ended). They must replay so the client
	// sees no sequence gap; content events still require a sealed carrier.
	if internalLifecycleEvent(event.Type) {
		if _, err := protocol.DecodeEncryptedPacketCarrier(event.Payload, "event", event.Type, ""); err == nil {
			return nil
		}
		if err := validatePlaintextLifecycleProjection(event.Type, event.Payload); err != nil {
			return err
		}
		return nil
	}
	_, err := protocol.DecodeEncryptedPacketCarrier(event.Payload, "event", event.Type, "")
	if err != nil {
		return errors.New("invalid encrypted event history")
	}
	return nil
}

// validatePlaintextLifecycleProjection checks that a plaintext lifecycle record
// carries only the fields that type may carry.
//
// The Hub is zero-knowledge: it never learns what a session said. But a
// lifecycle record the platform itself wrote -- a session it ended, a machine
// it revoked -- has no content, only a state, and it must still replay. A
// client therefore learns the state, which is exactly what the state says.
//
// The shape is still closed. A plaintext record may not smuggle a field the
// type never defines, and every value it does define is checked against the
// same vocabulary the sealed projection uses. A content event stays sealed.
func validatePlaintextLifecycleProjection(eventType string, payload []byte) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil {
		return errors.New("invalid encrypted event history")
	}
	switch eventType {
	case "session.state":
		for name := range fields {
			switch name {
			case "state", "reason":
			default:
				return errors.New("invalid encrypted event history")
			}
		}
		var state struct {
			State string `json:"state"`
		}
		if json.Unmarshal(payload, &state) != nil || state.State == "" {
			return errors.New("invalid encrypted event history")
		}
		return nil
	default:
		// The run-control outcome has its own strict projection validator.
		if eventType == "session.run.outcome" {
			return protocol.ValidateRunControlOutcomeProjection(payload)
		}
		return errors.New("invalid encrypted event history")
	}
}

func internalLifecycleEvent(eventType string) bool {
	// Store-generated records with no content: the recovered run-control
	// outcome, and the session state the platform writes when it ends a
	// Session or revokes a machine. Both must replay, because a client that
	// cannot read a session's own terminal state cannot tell an ended session
	// from a broken one. Everything else -- every content event, and every
	// capability the provider published -- stays sealed.
	return eventType == "session.run.outcome" || eventType == "session.state"
}
