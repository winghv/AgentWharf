package hub_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/hub"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store/sqlite"
	"nhooyr.io/websocket"
)

type requiredTransportAuth struct{ websocketTestAuth }

func (requiredTransportAuth) SessionContentMode(context.Context, auth.Principal, string) (string, error) {
	return protocol.ContentModeRequired, nil
}

func TestRequiredTransportUsesEncryptedDispatcher(t *testing.T) {
	ctx := context.Background()
	database, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "required-transport.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	backend := &settingsWebSocketStore{Store: database}
	expiresAt := time.Now().Add(time.Hour).UTC()
	a := requiredTransportAuth{websocketTestAuth{
		principals: map[string]auth.Principal{
			"client":  {Subject: "client", Scopes: []auth.Scope{auth.SessionView("ses_1"), auth.SessionControl("ses_1")}},
			"adapter": {Subject: "adapter", Scopes: []auth.Scope{auth.SessionAdapter("ses_1")}},
		},
		credentials: map[string]adapterCredentialEvidence{"adapter": {Generation: 1, ExpiresAt: expiresAt, AllowInitialize: true}},
	}}
	handshake := hub.NewHandshake(hub.HandshakeConfig{Authenticator: a, EventStore: backend})
	server := newWebSocketTestServer(t, handshake, func(cfg *hub.WebSocketConfig) { cfg.EventStore = backend })

	adapter := dialWebSocket(t, server.URL)
	defer adapter.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, adapter, &protocol.Hello{ProtocolVersion: 2, Role: protocol.RoleAdapter, Token: "adapter", SessionID: "ses_1", Provider: "claude-code", ContentMode: protocol.ContentModeRequired})
	adapterFrame := readFrame(t, adapter)
	ack, ok := adapterFrame.(*protocol.HelloAck)
	if !ok || ack.ProtocolVersion != protocol.ProtocolVersionV2 || ack.ContentMode != protocol.ContentModeRequired || ack.ConnectionAuthority == nil {
		t.Fatalf("required adapter hello was not accepted: %T %+v", adapterFrame, adapterFrame)
	}

	client := dialWebSocket(t, server.URL)
	defer client.Close(websocket.StatusNormalClosure, "")
	writeFrame(t, client, &protocol.Hello{ProtocolVersion: 2, Role: protocol.RoleClient, Token: "client", ContentMode: protocol.ContentModeRequired, Subscriptions: []protocol.Subscription{{SessionID: "ses_1"}}})
	clientFrame := readFrame(t, client)
	ack, ok = clientFrame.(*protocol.HelloAck)
	if !ok || ack.ContentMode != protocol.ContentModeRequired {
		t.Fatalf("required client hello was not accepted: %T %+v", clientFrame, clientFrame)
	}
}
