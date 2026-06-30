package core_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/winghv/agentwharf/adapter/core"
	"github.com/winghv/agentwharf/protocol"
)

func TestEventMaskerMasksUserVisibleEventPayloadStrings(t *testing.T) {
	t.Parallel()

	masker := core.NewEventMasker([]string{"secret-token", "db-password"})
	tests := []struct {
		name        string
		eventType   string
		payload     json.RawMessage
		wantPayload string
	}{
		{
			name:      "session message content",
			eventType: "session.message",
			payload: json.RawMessage(`{
				"message_id":"msg_1",
				"role":"agent",
				"content":[{"kind":"text","text":"Use secret-token carefully"}]
			}`),
			wantPayload: `{"content":[{"kind":"text","text":"Use [MASKED] carefully"}],"message_id":"msg_1","role":"agent"}`,
		},
		{
			name:      "tool call previews and input",
			eventType: "session.tool_call",
			payload: json.RawMessage(`{
				"tool_call_id":"tc_1",
				"phase":"result",
				"name":"bash",
				"input":{"command":"echo secret-token"},
				"result":{"status":"ok","output_preview":"db-password used","truncated":false}
			}`),
			wantPayload: `{"input":{"command":"echo [MASKED]"},"name":"bash","phase":"result","result":{"output_preview":"[MASKED] used","status":"ok","truncated":false},"tool_call_id":"tc_1"}`,
		},
		{
			name:      "permission request summary",
			eventType: "permission.request",
			payload: json.RawMessage(`{
				"request_id":"pr_1",
				"action":"network.connect",
				"risk_level":"medium",
				"summary":"connect with secret-token",
				"detail":{"reason":"contains db-password"}
			}`),
			wantPayload: `{"action":"network.connect","detail":{"reason":"contains [MASKED]"},"request_id":"pr_1","risk_level":"medium","summary":"connect with [MASKED]"}`,
		},
		{
			name:        "log tail",
			eventType:   "log.tail",
			payload:     json.RawMessage(`{"line":"secret-token printed"}`),
			wantPayload: `{"line":"[MASKED] printed"}`,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := masker.MaskEvent(protocol.Event{
				Type:      tt.eventType,
				SessionID: "ses_1",
				Payload:   tt.payload,
			})
			if err != nil {
				t.Fatalf("MaskEvent() error = %v", err)
			}
			assertJSONEqual(t, got.Payload, json.RawMessage(tt.wantPayload))
			if got.Type != tt.eventType || got.SessionID != "ses_1" {
				t.Fatalf("event metadata changed: %+v", got)
			}
		})
	}
}

func TestEventMaskerLeavesNonTextPayloadsAndNilSecretsAlone(t *testing.T) {
	t.Parallel()

	payload := json.RawMessage(`{"metric":12,"ok":true,"values":["plain"]}`)
	got, err := core.NewEventMasker(nil).MaskEvent(protocol.Event{
		Type:      "resource.sample",
		SessionID: "ses_1",
		Payload:   payload,
	})
	if err != nil {
		t.Fatalf("MaskEvent() error = %v", err)
	}
	assertJSONEqual(t, got.Payload, payload)
}

func TestEventMaskerRejectsInvalidJSONPayload(t *testing.T) {
	t.Parallel()

	_, err := core.NewEventMasker([]string{"secret-token"}).MaskEvent(protocol.Event{
		Type:      "session.message",
		SessionID: "ses_1",
		Payload:   json.RawMessage(`{"content":[`),
	})
	if !errors.Is(err, core.ErrInvalidEventPayload) {
		t.Fatalf("MaskEvent() error = %v, want ErrInvalidEventPayload", err)
	}
}

func assertJSONEqual(t *testing.T, got json.RawMessage, want json.RawMessage) {
	t.Helper()

	var gotValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("got payload invalid JSON %s: %v", string(got), err)
	}
	var wantValue any
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("want payload invalid JSON %s: %v", string(want), err)
	}
	gotEncoded, err := json.Marshal(gotValue)
	if err != nil {
		t.Fatalf("marshal got payload: %v", err)
	}
	wantEncoded, err := json.Marshal(wantValue)
	if err != nil {
		t.Fatalf("marshal want payload: %v", err)
	}
	if string(gotEncoded) != string(wantEncoded) {
		t.Fatalf("payload = %s, want %s", gotEncoded, wantEncoded)
	}
}
