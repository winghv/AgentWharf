package e2ee

import "encoding/json"

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
