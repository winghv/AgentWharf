package hub_test

import (
	"context"
	"testing"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/hub"
	"github.com/winghv/agentwharf/protocol"
	"nhooyr.io/websocket"
)

type requiredTransportAuth struct{ fakeAuth }

func (requiredTransportAuth) SessionContentMode(context.Context, auth.Principal, string) (string, error) {
	return protocol.ContentModeRequired, nil
}

func TestRequiredTransportCannotEnterLegacyWebSocketDispatcher(t *testing.T) {
	a := requiredTransportAuth{fakeAuth{token: "client", principal: auth.Principal{Subject: "client", Scopes: []auth.Scope{auth.SessionView("ses_1")}}}}
	handshake := hub.NewHandshake(hub.HandshakeConfig{Authenticator: a, EventStore: fakeStore{latest: map[string]int64{"ses_1": 1}}})
	hello := &protocol.Hello{ProtocolVersion: 2, Role: protocol.RoleClient, Token: "client", ContentMode: protocol.ContentModeRequired, Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}}}
	if _, _, err := handshake.HandleHello(context.Background(), hello); err != nil {
		t.Fatalf("mode policy should accept before transport guard: %v", err)
	}
	server := newWebSocketTestServer(t, handshake)
	conn := dialWebSocket(t, server.URL)
	defer conn.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, conn, hello)
	frame := readFrame(t, conn)
	failure, ok := frame.(*protocol.Error)
	if !ok || failure.Code != "encrypted_transport_unavailable" {
		t.Fatalf("got %T %+v, want explicit unavailable error before ack/replay", frame, frame)
	}
}
