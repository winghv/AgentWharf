package e2ee

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"time"
)

// DecodeCommandWire reconstructs authenticated context from explicit routing
// fields. Callers must still use CommandExecutor; decoding grants no authority.
func DecodeCommandWire(session, commandID, commandType string, data []byte) (Context, ContentPacket, error) {
	invalid := func() (Context, ContentPacket, error) { return Context{}, ContentPacket{}, ErrInvalid }
	if len(data) > 49*1024 || !identifier.MatchString(session) || !identifier.MatchString(commandID) || !identifier.MatchString(commandType) {
		return invalid()
	}
	fields, err := packetFields(data)
	if err != nil || len(fields) != 7 {
		return invalid()
	}
	var version int
	if json.Unmarshal(fields["version"], &version) != nil || version != 1 {
		return invalid()
	}
	values := map[string]string{}
	for _, name := range []string{"scope", "key_id", "sender", "message_id", "type"} {
		var value string
		if json.Unmarshal(fields[name], &value) != nil || !identifier.MatchString(value) {
			return invalid()
		}
		values[name] = value
	}
	if values["scope"] != "command" || values["message_id"] != commandID || values["type"] != commandType {
		return invalid()
	}
	packet, err := DecodeContentPacket(commandType, fields["packet"])
	if err != nil {
		return invalid()
	}
	return Context{Scope: "command", Session: session, Sender: values["sender"], KeyID: values["key_id"], MessageID: commandID, Type: commandType}, packet, nil
}

// CommandWire preserves the endpoint-assigned identity of a command the machine
// authors itself. It is local recovery evidence, never relayed user input.
type CommandWire struct {
	Version   int           `json:"version"`
	Scope     string        `json:"scope"`
	KeyID     string        `json:"key_id"`
	Sender    string        `json:"sender"`
	MessageID string        `json:"message_id"`
	Type      string        `json:"type"`
	Packet    ContentPacket `json:"packet"`
}

// SealCommandAsMachine seals a session.send carrier authored by the endpoint's
// own identity under the session's current key. It requires the machine's
// current control grant, so recovery satisfies the same current-grant invariant
// as a terminal-authored command and grants no access to a revoked terminal. It
// does not claim delivery; a process-start preflight still validates the result.
func (v *SessionKeyVault) SealCommandAsMachine(ctx context.Context, session, messageID string, payload json.RawMessage) (CommandWire, error) {
	const commandType = "session.send"
	if !identifier.MatchString(session) || !identifier.MatchString(messageID) {
		return CommandWire{}, ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var keyID string
	err := v.db.QueryRowContext(ctx, `SELECT s.key_id FROM e2ee_local_sessions s JOIN e2ee_local_grants g ON g.session=s.session WHERE s.session=? AND g.device=? AND g.control=1`, session, v.identity.Device).Scan(&keyID)
	if err != nil || !identifier.MatchString(keyID) {
		return CommandWire{}, ErrUnauthorized
	}
	command := Context{Scope: "command", Session: session, Sender: v.identity.Device, KeyID: keyID, MessageID: messageID, Type: commandType}
	if _, err := command.bytes(); err != nil {
		return CommandWire{}, err
	}
	key, err := v.Load(ctx, session, keyID)
	if err != nil {
		return CommandWire{}, err
	}
	defer clear(key)
	signer := ed25519.NewKeyFromSeed(v.identity.SigningSeed)
	defer clear(signer)
	packet, err := SealPacket(command, key, signer, PublicMetadata{}, payload)
	if err != nil {
		return CommandWire{}, err
	}
	return CommandWire{Version: 1, Scope: "command", KeyID: keyID, Sender: v.identity.Device, MessageID: messageID, Type: commandType, Packet: packet}, nil
}
