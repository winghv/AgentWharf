package main

import (
	"testing"

	"github.com/winghv/agentwharf/adapter/core"
	"github.com/winghv/agentwharf/protocol"
)

func TestRequiredAdapterAcknowledgementCannotDowngrade(t *testing.T) {
	for _, mode := range []string{"", protocol.ContentModeLegacy, "unknown", protocol.ContentModeRequired} {
		t.Run(mode, func(t *testing.T) {
			state, err := core.NewAdapterConnectionState(core.AdapterConnectionConfig{SessionID: "session", Provider: "claude-code", Token: "synthetic", ProtocolVersion: 2, ContentMode: protocol.ContentModeRequired})
			if err != nil {
				t.Fatal(err)
			}
			if state.Hello().ContentMode != protocol.ContentModeRequired {
				t.Fatal("hello lost mode")
			}
			ack := reconnectHelloAck("session", 1, 1)
			ack.ContentMode = mode
			_, err = state.MarkAccepted(*ack)
			if (err == nil) != (mode == protocol.ContentModeRequired) {
				t.Fatalf("mode %q: %v", mode, err)
			}
			if state.Hello().Resume != (mode == protocol.ContentModeRequired) {
				t.Fatal("rejected ack marked accepted")
			}
		})
	}
}
