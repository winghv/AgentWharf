package acp_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/adapter/acp"
)

// The frame shape captured from the deployed Claude bridge: a plan snapshot
// with structured entries, emitted every time the agent creates or updates a
// task in the list. The client replaces the whole plan with each update, so
// the entries grow rather than accumulate.
func TestMapperMapsObservedClaudePlanFrame(t *testing.T) {
	t.Parallel()

	mapper, err := acp.NewMapper(acp.Config{SessionID: "ses_1", Provider: "claude-code", Now: func() time.Time {
		return time.UnixMilli(1700000000456)
	}})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}

	events, err := mapper.MapReader(context.Background(), strings.NewReader(strings.Join([]string{
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"plan","entries":[{"content":"Create a task list with three steps","status":"pending","priority":"medium"}]}}}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"plan","entries":[{"content":"Create a task list with three steps","status":"pending","priority":"medium"},{"content":"Create a file named claude-plan.txt","status":"pending","priority":"medium"}]}}}`,
	}, "\n")))
	if err != nil {
		t.Fatalf("MapReader() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want one frame per plan snapshot", len(events))
	}

	first := payloadMap(t, events[0])
	if first["kind"] != "plan" {
		t.Fatalf("kind = %v, want plan", first["kind"])
	}
	entries, ok := first["entries"].([]any)
	if !ok || len(entries) != 1 {
		t.Fatalf("entries = %#v", first["entries"])
	}
	entry := entries[0].(map[string]any)
	if entry["content"] != "Create a task list with three steps" {
		t.Fatalf("content = %v", entry["content"])
	}
	if entry["status"] != "pending" || entry["priority"] != "medium" {
		t.Fatalf("entry = %+v, want the provider's own status and priority", entry)
	}

	second := payloadMap(t, events[1])
	grown, ok := second["entries"].([]any)
	if !ok || len(grown) != 2 {
		t.Fatalf("second snapshot entries = %#v", second["entries"])
	}
}
