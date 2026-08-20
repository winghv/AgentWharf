package main

import (
	"encoding/json"
	"testing"
)

func TestAskUserQuestionKeystrokes(t *testing.T) {
	questions := []askUserQuestion{
		{Question: "q", Options: []askUserQuestionOption{{Label: "A"}, {Label: "B"}, {Label: "C"}}},
	}
	got := string(askUserQuestionKeystrokes(questions, map[string]string{"q": "C"}))
	// down, down, enter (ESC [ B, ESC [ B, CR)
	want := string([]byte{0x1b, '[', 'B', 0x1b, '[', 'B', 13})
	if got != want {
		t.Fatalf("keystrokes = %q, want %q", got, want)
	}
}

func TestTranslateLineAskUserQuestion(t *testing.T) {
	provider := claudeProvider{}
	line := []byte(`{"type":"assistant","uuid":"u1","message":{"role":"assistant","content":[{"type":"tool_use","id":"tc_1","name":"AskUserQuestion","input":{"questions":[{"question":"Which?","options":[{"label":"A"},{"label":"B"}],"multiSelect":false}]}}]}}`)
	events, err := provider.translateLine("ses_1", line)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Type != "session.tool_call" || events[1].Type != "permission.request" {
		t.Fatalf("unexpected events: %d", len(events))
	}
}

func TestTranslateLineToolResult(t *testing.T) {
	provider := claudeProvider{}
	line := []byte(`{"type":"user","uuid":"u1","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tc_1","content":"done","is_error":false}]}}`)
	events, err := provider.translateLine("ses_1", line)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != "session.tool_call" {
		t.Fatalf("unexpected events: %d", len(events))
	}
	var payload map[string]any
	if json.Unmarshal(events[0].Payload, &payload) != nil {
		t.Fatal("invalid payload")
	}
	if payload["phase"] != "result" {
		t.Fatalf("unexpected phase: %v", payload["phase"])
	}
}
