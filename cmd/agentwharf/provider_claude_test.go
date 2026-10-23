package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
)

// The Claude Code transcript calls its own replies "assistant", but an
// encrypted session.message public projection accepts only user, agent, or
// system. Sealing the untranslated role fails and the interactive mirror
// goroutine exits, so the Console never receives the reply.
func TestClaudeTranscriptAssistantRoleProjectsAsAgent(t *testing.T) {
	line := []byte(`{"type":"assistant","uuid":"a2ec9ed0-f5db-4093-9630-00d411ab29a1","message":{"role":"assistant","content":[{"type":"text","text":"reply"}]}}`)
	events, err := claudeProvider{}.translateLine("session", line)
	if err != nil || len(events) != 1 {
		t.Fatalf("translateLine() = %d events, err %v", len(events), err)
	}
	if events[0].Type != "session.message" {
		t.Fatalf("event type = %q", events[0].Type)
	}
	metadata, err := e2ee.ProjectPublicMetadata(events[0].Type, events[0].Payload)
	if err != nil {
		t.Fatalf("ProjectPublicMetadata() error = %v", err)
	}
	if metadata.Role != "agent" {
		t.Fatalf("projected role = %q, want agent", metadata.Role)
	}
}

func TestCanonicalTranscriptRole(t *testing.T) {
	cases := map[string]string{"assistant": "agent", "user": "user", "system": "system", "": ""}
	for input, want := range cases {
		if got := canonicalTranscriptRole(input); got != want {
			t.Fatalf("canonicalTranscriptRole(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestClaudeTranscriptCarriesThinkingModelUsageAndEditDiff(t *testing.T) {
	line := []byte(`{"type":"assistant","uuid":"a1","message":{"role":"assistant","model":"claude-sonnet-4","usage":{"input_tokens":120,"output_tokens":30,"cache_read_input_tokens":40},"content":[{"type":"thinking","thinking":"inspect the file"},{"type":"text","text":"I will edit it."},{"type":"tool_use","id":"tool_1","name":"Edit","input":{"file_path":"/home/user/repo/app.ts","old_string":"port = 3000","new_string":"port = 8080"}}]}}`)
	events, err := claudeProvider{}.translateLine("session", line)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("events = %d, want thinking, text and tool_use", len(events))
	}
	var thinking map[string]any
	if err := json.Unmarshal(events[0].Payload, &thinking); err != nil {
		t.Fatal(err)
	}
	if thinking["kind"] != "thinking" || thinking["text"] != "inspect the file" {
		t.Fatalf("thinking payload = %+v", thinking)
	}
	var message map[string]any
	if err := json.Unmarshal(events[1].Payload, &message); err != nil {
		t.Fatal(err)
	}
	if message["model"] != "claude-sonnet-4" {
		t.Fatalf("message model = %v", message["model"])
	}
	usage, _ := message["usage"].(map[string]any)
	if usage["input_tokens"] != float64(120) || usage["output_tokens"] != float64(30) || usage["cache_read_input_tokens"] != float64(40) {
		t.Fatalf("message usage = %+v", usage)
	}
	var tool map[string]any
	if err := json.Unmarshal(events[2].Payload, &tool); err != nil {
		t.Fatal(err)
	}
	diffs, _ := tool["diffs"].([]any)
	if len(diffs) != 1 {
		t.Fatalf("tool diffs = %+v", tool["diffs"])
	}
	diff, _ := diffs[0].(map[string]any)
	if diff["path_base"] != "app.ts" || diff["old_text"] != "port = 3000" || diff["new_text"] != "port = 8080" {
		t.Fatalf("diff = %+v", diff)
	}
}

func TestClaudeTranscriptBoundsLargeToolResult(t *testing.T) {
	line, err := json.Marshal(map[string]any{
		"type": "user",
		"uuid": "u1",
		"message": map[string]any{
			"role": "user",
			"content": []map[string]any{{
				"type":        "tool_result",
				"tool_use_id": "tool_1",
				"content":     strings.Repeat("x", maxTranscriptToolPreviewBytes+100),
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	events, err := claudeProvider{}.translateLine("session", line)
	if err != nil || len(events) != 1 {
		t.Fatalf("translateLine = %+v, %v", events, err)
	}
	var payload map[string]any
	if err := json.Unmarshal(events[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	result, _ := payload["result"].(map[string]any)
	if result["truncated"] != true || result["output_bytes"] != float64(maxTranscriptToolPreviewBytes+100) {
		t.Fatalf("result = %+v", result)
	}
}

func TestClaudeTranscriptMapsCompactionBoundary(t *testing.T) {
	line := []byte(`{"type":"system","subtype":"compact_boundary","uuid":"c1","compact_metadata":{"trigger":"auto","pre_tokens":90000}}`)
	events, err := claudeProvider{}.translateLine("session", line)
	if err != nil || len(events) != 1 {
		t.Fatalf("translateLine = %+v, %v", events, err)
	}
	var payload map[string]any
	if err := json.Unmarshal(events[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["kind"] != "compaction" || payload["phase"] != "done" || payload["trigger"] != "auto" {
		t.Fatalf("compaction payload = %+v", payload)
	}
}

func TestClaudeTranscriptBoundsEscapedToolResultPayload(t *testing.T) {
	line, err := json.Marshal(map[string]any{
		"type": "user",
		"uuid": "u2",
		"message": map[string]any{
			"role": "user",
			"content": []map[string]any{{
				"type":        "tool_result",
				"tool_use_id": "tool_2",
				"content":     strings.Repeat("&", 20_000),
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	events, err := claudeProvider{}.translateLine("session", line)
	if err != nil || len(events) != 1 {
		t.Fatalf("translateLine = %+v, %v", events, err)
	}
	if len(events[0].Payload) > protocol.MaxEventPayloadBytes {
		t.Fatalf("payload = %d bytes, want <= %d", len(events[0].Payload), protocol.MaxEventPayloadBytes)
	}
	var payload map[string]any
	if err := json.Unmarshal(events[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	result, _ := payload["result"].(map[string]any)
	if result["truncated"] != true {
		t.Fatalf("result = %+v, want truncated", result)
	}
}
