package protocol

import (
	"encoding/json"
	"errors"
)

const MaxEncryptedPacketCarrierBytes = 49 * 1024

var ErrEncryptedPacket = errors.New("invalid encrypted packet carrier")

// EncryptedPacketCarrier is opaque endpoint content with bounded public routing
// fields. Parsing is not signature verification or endpoint command authority.
type EncryptedPacketCarrier struct {
	Version   int                 `json:"version"`
	Scope     string              `json:"scope"`
	KeyID     string              `json:"key_id"`
	Sender    string              `json:"sender"`
	MessageID string              `json:"message_id"`
	Type      string              `json:"type"`
	Packet    OpaqueContentPacket `json:"packet"`
}
type OpaqueContentPacket struct {
	Version   int                   `json:"version"`
	Public    EncryptedProjection   `json:"public"`
	Encrypted OpaqueContentEnvelope `json:"encrypted"`
}
type EncryptedProjection struct {
	State     string `json:"state,omitempty"`
	Role      string `json:"role,omitempty"`
	RequestID string `json:"request_id,omitempty"`
	Decision  string `json:"decision,omitempty"`
	// Run-control outcomes expose only minimal lifecycle control fields so the
	// zero-knowledge Hub can finalize its reservation and project the Session
	// state; the endpoint-owned payload stays sealed.
	Operation       string `json:"operation,omitempty"`
	Outcome         string `json:"outcome,omitempty"`
	CompletionState string `json:"completion_state,omitempty"`
	ReasonCode      string `json:"reason_code,omitempty"`
	// CommandID binds the outcome to the Hub's run-control reservation. It is
	// optional because endpoint builds sealed outcomes before it existed, and the
	// Hub then falls back to its own pending reservation.
	CommandID string `json:"command_id,omitempty"`
	// Run-control capabilities expose only the supported operations so the Hub
	// can register the endpoint's current capability without the sealed payload.
	InterruptSupported bool `json:"interrupt_supported,omitempty"`
	StopSupported      bool `json:"stop_supported,omitempty"`
}
type OpaqueContentEnvelope struct {
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
	Signature  string `json:"signature"`
}

// DecodeEncryptedPacketCarrier binds the carrier to the transport scope/type.
// Commands also bind message_id to cmd_id; event message IDs are independent of
// Hub seq, so event callers pass an empty commandID. No plaintext is accepted.
func DecodeEncryptedPacketCarrier(data []byte, scope, contentType, commandID string) (EncryptedPacketCarrier, error) {
	invalid := func() (EncryptedPacketCarrier, error) { return EncryptedPacketCarrier{}, ErrEncryptedPacket }
	if len(data) > MaxEncryptedPacketCarrierBytes || (scope != "event" && scope != "command") || !validProtocolIdentifier(contentType) ||
		(scope == "command" && !validProtocolIdentifier(commandID)) || (scope == "event" && commandID != "") {
		return invalid()
	}
	fields, err := encryptedObject(data, "version", "scope", "key_id", "sender", "message_id", "type", "packet")
	if err != nil {
		return invalid()
	}
	packet, err := encryptedObject(fields["packet"], "version", "public", "encrypted")
	if err != nil {
		return invalid()
	}
	if _, err = encryptedObject(packet["encrypted"], "nonce", "ciphertext", "signature"); err != nil {
		return invalid()
	}
	if err = validateEncryptedProjection(contentType, packet["public"]); err != nil {
		return invalid()
	}
	var result EncryptedPacketCarrier
	if json.Unmarshal(data, &result) != nil || result.Version != 1 || result.Packet.Version != 1 || result.Scope != scope || result.Type != contentType ||
		!validProtocolIdentifier(result.KeyID) || !validProtocolIdentifier(result.Sender) || !validProtocolIdentifier(result.MessageID) ||
		(scope == "command" && result.MessageID != commandID) {
		return invalid()
	}
	envelope := result.Packet.Encrypted
	if !validBase64URL(envelope.Nonce, 12, 12) || !validBase64URL(envelope.Ciphertext, 16, 32768+16) || !validBase64URL(envelope.Signature, 64, 64) {
		return invalid()
	}
	return result, nil
}

func encryptedObject(data []byte, names ...string) (map[string]json.RawMessage, error) {
	fields, err := strictObject(data)
	if err != nil || len(fields) != len(names) {
		return nil, ErrEncryptedPacket
	}
	for _, name := range names {
		if _, ok := fields[name]; !ok {
			return nil, ErrEncryptedPacket
		}
	}
	return fields, nil
}
func validateEncryptedProjection(contentType string, data []byte) error {
	fields, err := strictObject(data)
	if err != nil {
		return ErrEncryptedPacket
	}
	if contentType == "session.run.outcome" {
		return validateRunControlOutcomeProjection(fields)
	}
	if contentType == "session.run.capabilities" {
		for name, raw := range fields {
			if name != "interrupt_supported" && name != "stop_supported" {
				return ErrEncryptedPacket
			}
			var value bool
			if json.Unmarshal(raw, &value) != nil {
				return ErrEncryptedPacket
			}
		}
		return nil
	}
	var names []string
	switch contentType {
	case "session.state":
		names = []string{"state"}
	case "session.message":
		names = []string{"role"}
	case "permission.request":
		names = []string{"request_id"}
	case "permission.decision", "permission.respond":
		names = []string{"request_id", "decision"}
	}
	if len(fields) != len(names) {
		return ErrEncryptedPacket
	}
	for _, name := range names {
		var value string
		if json.Unmarshal(fields[name], &value) != nil || value == "" {
			return ErrEncryptedPacket
		}
		switch name {
		case "state":
			switch value {
			case "starting", "ready", "busy", "waiting_permission", "ended", "error":
			default:
				return ErrEncryptedPacket
			}
		case "role":
			if value != "user" && value != "agent" && value != "system" {
				return ErrEncryptedPacket
			}
		case "request_id":
			if !validProtocolIdentifier(value) {
				return ErrEncryptedPacket
			}
		case "decision":
			if value != "approve" && value != "deny" && value != "expired" {
				return ErrEncryptedPacket
			}
		}
	}
	return nil
}

// validateRunControlOutcomeProjection accepts the minimal run-control lifecycle
// fields. operation and outcome are required; completion_state and reason_code
// are present only for their respective outcomes.
func validateRunControlOutcomeProjection(fields map[string]json.RawMessage) error {
	for name := range fields {
		switch name {
		case "operation", "outcome", "completion_state", "reason_code", "command_id":
		default:
			return ErrEncryptedPacket
		}
	}
	read := func(name string) (string, bool, error) {
		raw, ok := fields[name]
		if !ok {
			return "", false, nil
		}
		var value string
		if json.Unmarshal(raw, &value) != nil || value == "" {
			return "", false, ErrEncryptedPacket
		}
		return value, true, nil
	}
	operation, ok, err := read("operation")
	if err != nil || !ok {
		return ErrEncryptedPacket
	}
	if operation != "interrupt" && operation != "stop" {
		return ErrEncryptedPacket
	}
	outcome, ok, err := read("outcome")
	if err != nil || !ok {
		return ErrEncryptedPacket
	}
	switch outcome {
	case "completed", "rejected", "timeout", "outcome_unknown":
	default:
		return ErrEncryptedPacket
	}
	completion, hasCompletion, err := read("completion_state")
	if err != nil {
		return ErrEncryptedPacket
	}
	if hasCompletion && completion != "ready" && completion != "ended" {
		return ErrEncryptedPacket
	}
	reason, hasReason, err := read("reason_code")
	if err != nil {
		return ErrEncryptedPacket
	}
	if hasReason && !validProtocolIdentifier(reason) {
		return ErrEncryptedPacket
	}
	commandID, hasCommand, err := read("command_id")
	if err != nil {
		return ErrEncryptedPacket
	}
	if hasCommand && !validProtocolIdentifier(commandID) {
		return ErrEncryptedPacket
	}
	return nil
}
