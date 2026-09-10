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
