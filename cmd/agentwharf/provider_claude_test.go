package main

import (
	"testing"

	"github.com/winghv/agentwharf/experimental/e2ee"
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
