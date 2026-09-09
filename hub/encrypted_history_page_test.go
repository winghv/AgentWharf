package hub

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

func TestRequiredHistoryPageRejectsMixedContent(t *testing.T) {
	encoded := func(n int) string { return base64.RawURLEncoding.EncodeToString(make([]byte, n)) }
	payload, err := json.Marshal(protocol.EncryptedPacketCarrier{Version: 1, Scope: "event", KeyID: "key", Sender: "machine", MessageID: "state", Type: "session.state", Packet: protocol.OpaqueContentPacket{Version: 1, Public: protocol.EncryptedProjection{State: "ready"}, Encrypted: protocol.OpaqueContentEnvelope{Nonce: encoded(12), Ciphertext: encoded(16), Signature: encoded(64)}}})
	if err != nil {
		t.Fatal(err)
	}
	handler := &webSocketHandler{}
	request := &protocol.HistoryPageRequest{SessionID: "session", Limit: 2}
	page := store.HistoryPage{LatestSeq: 2, RetentionState: store.RetentionComplete, Events: []store.Event{{SessionID: "session", Seq: 1, Type: "session.state", Payload: payload}, {SessionID: "session", Seq: 2, Type: "session.state", Payload: payload}}}
	if !handler.validHistoryPageMode(page, request, protocol.ContentModeRequired) {
		t.Fatal("valid opaque page refused")
	}
	page.Events[1].Payload = []byte(`{"state":"ready","private":"synthetic"}`)
	if handler.validHistoryPageMode(page, request, protocol.ContentModeRequired) {
		t.Fatal("mixed page accepted")
	}
	if !handler.validHistoryPageMode(page, request, protocol.ContentModeLegacy) {
		t.Fatal("legacy page behavior changed")
	}
	page.Events[1].Payload = payload
	page.Events[1].SessionID = "other"
	if handler.validHistoryPageMode(page, request, protocol.ContentModeRequired) {
		t.Fatal("cross-session page accepted")
	}
}
