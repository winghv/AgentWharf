package e2ee

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"io"
)

// PublicMetadata is an explicit projection, never arbitrary Provider metadata.
// Fields not required for Hub scheduling/permission routing remain encrypted.
type PublicMetadata struct {
	State              string `json:"state,omitempty"`
	Role               string `json:"role,omitempty"`
	RequestID          string `json:"request_id,omitempty"`
	Decision           string `json:"decision,omitempty"`
	Operation          string `json:"operation,omitempty"`
	Outcome            string `json:"outcome,omitempty"`
	CompletionState    string `json:"completion_state,omitempty"`
	ReasonCode         string `json:"reason_code,omitempty"`
	CommandID          string `json:"command_id,omitempty"`
	InterruptSupported bool   `json:"interrupt_supported,omitempty"`
	StopSupported      bool   `json:"stop_supported,omitempty"`
}

type ContentPacket struct {
	Version   int            `json:"version"`
	Public    PublicMetadata `json:"public"`
	Encrypted Envelope       `json:"encrypted"`
}

type packetContent struct {
	Public  PublicMetadata  `json:"public"`
	Payload json.RawMessage `json:"payload"`
}

func (p PublicMetadata) validate(eventType string) error {
	switch eventType {
	case "session.state":
		switch p.State {
		case "starting", "ready", "busy", "waiting_permission", "ended", "error":
		default:
			return ErrInvalid
		}
		if p.Role != "" || p.RequestID != "" || p.Decision != "" {
			return ErrInvalid
		}
	case "session.message":
		if p.Role != "user" && p.Role != "agent" && p.Role != "system" {
			return ErrInvalid
		}
		if p.State != "" || p.RequestID != "" || p.Decision != "" {
			return ErrInvalid
		}
	case "permission.request":
		if !identifier.MatchString(p.RequestID) || p.State != "" || p.Role != "" || p.Decision != "" {
			return ErrInvalid
		}
	case "permission.decision", "permission.respond":
		if !identifier.MatchString(p.RequestID) || p.State != "" || p.Role != "" {
			return ErrInvalid
		}
		if p.Decision != "approve" && p.Decision != "deny" && p.Decision != "expired" {
			return ErrInvalid
		}
	case "session.run.outcome":
		if (p.Operation != "interrupt" && p.Operation != "stop") || p.State != "" || p.Role != "" || p.RequestID != "" || p.Decision != "" {
			return ErrInvalid
		}
		if p.CommandID != "" && !identifier.MatchString(p.CommandID) {
			return ErrInvalid
		}
		switch p.Outcome {
		case "completed", "rejected", "timeout", "outcome_unknown":
		default:
			return ErrInvalid
		}
		if p.CompletionState != "" && p.CompletionState != "ready" && p.CompletionState != "ended" {
			return ErrInvalid
		}
		if p.ReasonCode != "" && !identifier.MatchString(p.ReasonCode) {
			return ErrInvalid
		}
	case "session.run.capabilities":
		if p.State != "" || p.Role != "" || p.RequestID != "" || p.Decision != "" || p.Operation != "" || p.Outcome != "" || p.CompletionState != "" || p.ReasonCode != "" || p.CommandID != "" {
			return ErrInvalid
		}
	default:
		if p != (PublicMetadata{}) {
			return ErrInvalid
		}
	}
	return nil
}

// ProjectPublicMetadata derives, rather than accepts, the public control fields
// from an ordinary domain payload. Everything else remains encrypted.
func ProjectPublicMetadata(eventType string, payload json.RawMessage) (PublicMetadata, error) {
	fields, err := packetFields(payload)
	if err != nil {
		return PublicMetadata{}, err
	}
	var result PublicMetadata
	read := func(name string, target *string) error {
		if fields[name] == nil || json.Unmarshal(fields[name], target) != nil || *target == "" {
			return ErrInvalid
		}
		return nil
	}
	// An optional public field may be absent or JSON null; an empty string is
	// omitted from the projection.
	readOptional := func(name string, target *string) error {
		raw := fields[name]
		if raw == nil || string(raw) == "null" {
			*target = ""
			return nil
		}
		if json.Unmarshal(raw, target) != nil {
			return ErrInvalid
		}
		return nil
	}
	// A capability boolean projection omits false, so absence means unsupported.
	readBool := func(name string, target *bool) error {
		raw := fields[name]
		if raw == nil || string(raw) == "null" {
			*target = false
			return nil
		}
		return json.Unmarshal(raw, target)
	}
	switch eventType {
	case "session.state":
		err = read("state", &result.State)
	case "session.message":
		err = read("role", &result.Role)
	case "permission.request":
		err = read("request_id", &result.RequestID)
	case "permission.decision", "permission.respond":
		err = read("request_id", &result.RequestID)
		if err == nil {
			err = read("decision", &result.Decision)
		}
	case "session.run.outcome":
		err = readOptional("cmd_id", &result.CommandID)
		if err == nil {
			err = read("operation", &result.Operation)
		}
		if err == nil {
			err = read("outcome", &result.Outcome)
		}
		if err == nil {
			err = readOptional("completion_state", &result.CompletionState)
		}
		if err == nil {
			err = readOptional("reason_code", &result.ReasonCode)
		}
	case "session.run.capabilities":
		err = readBool("interrupt_supported", &result.InterruptSupported)
		if err == nil {
			err = readBool("stop_supported", &result.StopSupported)
		}
	}
	if err != nil || result.validate(eventType) != nil {
		return PublicMetadata{}, ErrInvalid
	}
	return result, nil
}

// SealPacket keeps the domain payload unchanged inside the authenticated packet.
// A caller supplies only the control projection approved for the event type.
func SealPacket(ctx Context, key []byte, signer ed25519.PrivateKey, public PublicMetadata, payload json.RawMessage) (ContentPacket, error) {
	projected, err := ProjectPublicMetadata(ctx.Type, payload)
	if err != nil || !sameControlProjection(public, projected) || (public.CommandID != "" && public.CommandID != projected.CommandID) {
		return ContentPacket{}, ErrInvalid
	}
	plaintext, err := json.Marshal(packetContent{public, payload})
	if err != nil {
		return ContentPacket{}, ErrInvalid
	}
	defer clear(plaintext)
	sealed, err := Seal(ctx, key, signer, plaintext)
	if err != nil {
		return ContentPacket{}, err
	}
	return ContentPacket{1, public, sealed}, nil
}

func packetFields(data []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, ErrInvalid
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, ErrInvalid
		}
		name, ok := token.(string)
		if !ok {
			return nil, ErrInvalid
		}
		if _, exists := fields[name]; exists {
			return nil, ErrInvalid
		}
		var raw json.RawMessage
		if decoder.Decode(&raw) != nil {
			return nil, ErrInvalid
		}
		fields[name] = raw
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, ErrInvalid
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, ErrInvalid
	}
	return fields, nil
}

// OpenPacket verifies the outside projection against its authenticated copy
// before returning any payload. Hub metadata alone is never endpoint authority.
func OpenPacket(ctx Context, key []byte, signer ed25519.PublicKey, packet ContentPacket) (json.RawMessage, error) {
	if packet.Version != 1 || packet.Public.validate(ctx.Type) != nil {
		return nil, ErrInvalid
	}
	plaintext, err := Open(ctx, key, signer, packet.Encrypted)
	if err != nil {
		return nil, err
	}
	defer clear(plaintext)
	return openPacketPlaintext(ctx.Type, packet.Public, plaintext)
}

func openPacketPlaintext(eventType string, expected PublicMetadata, plaintext []byte) (json.RawMessage, error) {
	fields, err := packetFields(plaintext)
	if err != nil || len(fields) != 2 || fields["public"] == nil || fields["payload"] == nil {
		return nil, ErrInvalid
	}
	public, err := decodePublicMetadata(eventType, fields["public"])
	if err != nil {
		return nil, err
	}
	projected, err := ProjectPublicMetadata(eventType, fields["payload"])
	if err != nil || !sameControlProjection(projected, public) || public != expected || (public.CommandID != "" && public.CommandID != projected.CommandID) {
		return nil, ErrInvalid
	}
	return append(json.RawMessage(nil), fields["payload"]...), nil
}

// sameControlProjection compares a derived projection with a supplied or sealed
// one. command_id is an optional Hub routing hint introduced after endpoints had
// already sealed outcomes, so its absence is not a projection mismatch. The
// authenticated copy and the transport projection still compare exactly.
func sameControlProjection(left, right PublicMetadata) bool {
	left.CommandID, right.CommandID = "", ""
	return left == right
}

func decodePublicMetadata(eventType string, data []byte) (PublicMetadata, error) {
	projection, err := packetFields(data)
	if err != nil {
		return PublicMetadata{}, ErrInvalid
	}
	var public PublicMetadata
	for name, value := range projection {
		var target *string
		switch name {
		case "state":
			target = &public.State
		case "role":
			target = &public.Role
		case "request_id":
			target = &public.RequestID
		case "decision":
			target = &public.Decision
		case "operation":
			target = &public.Operation
		case "outcome":
			target = &public.Outcome
		case "completion_state":
			target = &public.CompletionState
		case "reason_code":
			target = &public.ReasonCode
		case "command_id":
			target = &public.CommandID
		case "interrupt_supported":
			var supported bool
			if json.Unmarshal(value, &supported) != nil {
				return PublicMetadata{}, ErrInvalid
			}
			public.InterruptSupported = supported
			continue
		case "stop_supported":
			var supported bool
			if json.Unmarshal(value, &supported) != nil {
				return PublicMetadata{}, ErrInvalid
			}
			public.StopSupported = supported
			continue
		default:
			return PublicMetadata{}, ErrInvalid
		}
		if json.Unmarshal(value, target) != nil || *target == "" {
			return PublicMetadata{}, ErrInvalid
		}
	}
	if public.validate(eventType) != nil {
		return PublicMetadata{}, ErrInvalid
	}
	return public, nil
}

// DecodeContentPacket is the bounded network decoder. A plain json.Unmarshal
// must not be used at this boundary: it silently accepts duplicate members.
func DecodeContentPacket(eventType string, data []byte) (ContentPacket, error) {
	if len(data) > 48*1024 {
		return ContentPacket{}, ErrInvalid
	}
	fields, err := packetFields(data)
	if err != nil || len(fields) != 3 || fields["version"] == nil || fields["public"] == nil || fields["encrypted"] == nil {
		return ContentPacket{}, ErrInvalid
	}
	var version int
	if json.Unmarshal(fields["version"], &version) != nil || version != 1 {
		return ContentPacket{}, ErrInvalid
	}
	public, err := decodePublicMetadata(eventType, fields["public"])
	if err != nil {
		return ContentPacket{}, err
	}
	envelope, err := DecodeEnvelope(fields["encrypted"])
	if err != nil {
		return ContentPacket{}, err
	}
	return ContentPacket{version, public, envelope}, nil
}
