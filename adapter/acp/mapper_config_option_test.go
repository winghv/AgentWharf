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
	if len(events) != 1 {
		t.Fatalf("events = %d, want the option frame", len(events))
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
