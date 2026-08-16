package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/winghv/agentwharf/protocol"
)

func testLocalEvent(t *testing.T, eventType string, payload any) protocol.Event {
	t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return protocol.Event{Type: eventType, SessionID: "ses_1", Payload: encoded}
}

func TestLocalTerminalRendersMessageAndToolCall(t *testing.T) {
	var out bytes.Buffer
	terminal := newLocalTerminal(&out, nil, nil, nil, nil)

	terminal.render(testLocalEvent(t, "session.message", map[string]any{
		"content": []map[string]any{{"kind": "text", "text": "hello world"}},
	}))
	if !strings.Contains(out.String(), "hello world") {
		t.Fatalf("message not rendered: %q", out.String())
	}

	out.Reset()
	terminal.render(testLocalEvent(t, "session.tool_call", map[string]any{
		"phase": "start", "name": "Bash",
	}))
	if !strings.Contains(out.String(), "[tool] Bash") {
		t.Fatalf("tool call not rendered: %q", out.String())
	}
}

func TestLocalTerminalPermissionRoundTrip(t *testing.T) {
	var out bytes.Buffer
	var mu sync.Mutex
	pending := make(map[string]acpPendingPermission)
	var decidedRequest, decidedValue string
	var prompted string
	terminal := newLocalTerminal(&out, pending, &mu, func(text string) error {
		prompted = text
		return nil
	}, func(requestID, decision string) error {
		decidedRequest, decidedValue = requestID, decision
		return nil
	})

	terminal.render(testLocalEvent(t, "permission.request", map[string]any{
		"request_id": "req_1", "action": "Bash", "summary": "rm -rf /tmp/x",
	}))
	if !strings.Contains(out.String(), "[permission] Bash") {
		t.Fatalf("permission not rendered: %q", out.String())
	}

	terminal.readInput(context.Background(), strings.NewReader("allow\nhello\n"))
	if decidedRequest != "req_1" || decidedValue != "approve" {
		t.Fatalf("permission decision = (%q, %q)", decidedRequest, decidedValue)
	}
	if prompted != "hello" {
		t.Fatalf("prompt = %q", prompted)
	}
}

func TestLocalPermissionDecision(t *testing.T) {
	for _, approve := range []string{"allow", "y", "yes", "approve", "ok", "1"} {
		if got := localPermissionDecision(approve); got != "approve" {
			t.Fatalf("localPermissionDecision(%q) = %q, want approve", approve, got)
		}
	}
	for _, reject := range []string{"deny", "n", "no", "reject", "anything"} {
		if got := localPermissionDecision(reject); got != "reject" {
			t.Fatalf("localPermissionDecision(%q) = %q, want reject", reject, got)
		}
	}
}
