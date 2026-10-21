package acp_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/adapter/acp"
)

// The provider publishes the session options it knows, including the
// collaboration mode a command like /plan switches. Without them a client
// cannot say whether the switch is on, and sending the command again turns the
// mode off.
func TestMapperMapsObservedConfigOptionUpdateFrame(t *testing.T) {
	t.Parallel()

	mapper, err := acp.NewMapper(acp.Config{SessionID: "ses_1", Provider: "codex", Now: func() time.Time {
		return time.UnixMilli(1700000000456)
	}})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}

	events, err := mapper.MapReader(context.Background(), strings.NewReader(
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"config_option_update","configOptions":[{"type":"select","id":"collaboration_mode","name":"Collaboration mode","currentValue":"plan"}]}}}`,
	))
	if err != nil {
		t.Fatalf("MapReader() error = %v", err)
	}
	// The ephemeral option frame and the durable snapshot of the same options.
	if len(events) != 2 {
		t.Fatalf("events = %d, want the option frame and the durable snapshot", len(events))
	}
	payload := payloadMap(t, events[0])
	if payload["kind"] != "config_option_update" {
		t.Fatalf("kind = %v, want config_option_update", payload["kind"])
	}
	options, ok := payload["configOptions"].([]any)
	if !ok || len(options) != 1 {
		t.Fatalf("configOptions = %#v", payload["configOptions"])
	}
	option := options[0].(map[string]any)
	if option["id"] != "collaboration_mode" || option["currentValue"] != "plan" {
		t.Fatalf("option = %+v, want the provider's own option and value", option)
	}
}

// The provider announces its commands and options when the Session starts,
// and both announcements are ephemeral. The Adapter publishes them as one
// durable snapshot, so a client that opens the Session later still learns
// which commands the provider offers and whether a switch is on.
func TestMapperPublishesProviderCommandsAsADurableSnapshot(t *testing.T) {
	t.Parallel()

	mapper, err := acp.NewMapper(acp.Config{SessionID: "ses_1", Provider: "codex", Now: func() time.Time {
		return time.UnixMilli(1700000000456)
	}})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}

	events, err := mapper.MapReader(context.Background(), strings.NewReader(strings.Join([]string{
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"available_commands_update","commands":[{"name":"plan","description":"Turn plan mode on.","_meta":{"commandAction":{"kind":"setConfigOption","configId":"collaboration_mode","value":"plan","resetValue":"default","presentation":"state"}}}]}}}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"config_option_update","configOptions":[{"type":"select","id":"collaboration_mode","name":"Collaboration mode","currentValue":"plan"}]}}}`,
	}, "\n")))
	if err != nil {
		t.Fatalf("MapReader() error = %v", err)
	}

	var snapshots int
	for _, event := range events {
		if event.Type != "session.commands" {
			continue
		}
		snapshots++
		payload := payloadMap(t, event)
		commands, ok := payload["commands"].([]any)
		if !ok || len(commands) != 1 {
			t.Fatalf("snapshot commands = %#v", payload["commands"])
		}
		command := commands[0].(map[string]any)
		if command["name"] != "plan" {
			t.Fatalf("snapshot command = %+v, want the provider's command", command)
		}
	}
	if snapshots != 2 {
		t.Fatalf("durable snapshots = %d, want one per provider frame", snapshots)
	}
}
