package acp

import (
	"encoding/json"
	"testing"
)

// A claude-agent-acp / pi-acp session/prompt result carries only {stopReason}
// with no sessionId. The mapper must treat it as the turn boundary and publish
// ready; otherwise the session stays busy forever after the first reply.
func TestMapperPromptResponseFramePublishesReady(t *testing.T) {
	m, err := NewMapper(Config{SessionID: "hub_ses", Provider: "claude-code"})
	if err != nil {
		t.Fatal(err)
	}
	events, err := m.MapLine([]byte(`{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != "session.state" {
		t.Fatalf("events = %+v, want one session.state", events)
	}
	var payload struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(events[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.State != "ready" {
		t.Fatalf("state = %q, want ready", payload.State)
	}
}

// stopReasonFrame must recognize the prompt result shapes and reject frames
// that are not turn boundaries.
func TestMapperStopReasonFrameDetection(t *testing.T) {
	if !stopReasonFrame(map[string]any{"result": map[string]any{"stopReason": "end_turn"}}) {
		t.Fatal("nested result.stopReason not detected")
	}
	if !stopReasonFrame(map[string]any{"stopReason": "cancelled"}) {
		t.Fatal("top-level stopReason not detected")
	}
	if stopReasonFrame(map[string]any{"result": map[string]any{"sessionId": "s1"}}) {
		t.Fatal("session/new result must not be treated as a stop frame")
	}
	if stopReasonFrame(map[string]any{"error": map[string]any{}}) {
		t.Fatal("error frame must not be treated as a stop frame")
	}
	if stopReasonFrame(map[string]any{}) {
		t.Fatal("empty frame must not be treated as a stop frame")
	}
}

// Regression: session/new results (result carries sessionId) keep taking the
// recognized response-frame path; the stopReason fallback must not change it.
func TestMapperNewSessionResponseStillReady(t *testing.T) {
	m, err := NewMapper(Config{SessionID: "hub_ses", Provider: "claude-code"})
	if err != nil {
		t.Fatal(err)
	}
	events, err := m.MapLine([]byte(`{"jsonrpc":"2.0","id":2,"result":{"sessionId":"acp_ses_1"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != "session.state" {
		t.Fatalf("events = %+v", events)
	}
	var payload struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(events[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.State != "ready" {
		t.Fatalf("state = %q, want ready", payload.State)
	}
}
