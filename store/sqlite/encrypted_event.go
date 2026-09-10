package sqlite

import (
	"encoding/json"

	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

// Projection input is separate from the original opaque payload written to SQL.
func sqliteProjectionEvent(event store.PendingEvent, encrypted bool) (store.PendingEvent, error) {
	if !encrypted {
		return event, nil
	}
	carrier, err := protocol.DecodeEncryptedPacketCarrier(event.Payload, "event", event.Type, "")
	if err != nil {
		return store.PendingEvent{}, err
	}
	payload, err := json.Marshal(carrier.Packet.Public)
	if err != nil {
		return store.PendingEvent{}, err
	}
	return store.PendingEvent{Type: event.Type, Time: event.Time, Payload: payload}, nil
}
