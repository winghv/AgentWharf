package acp_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/adapter/acp"
)

func TestEnrichmentCarriesTheDiff(t *testing.T) {
	mapper, err := acp.NewMapper(acp.Config{SessionID: "ses_1", Provider: "claude-code", Now: func() time.Time {
		return time.UnixMilli(1700000000456)
	}})
	if err != nil {
		t.Fatalf("NewMapper: %v", err)
	}
	events, err := mapper.MapReader(context.Background(), strings.NewReader(strings.Join([]string{
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"tool_call","toolCallId":"call_1","title":"Write","kind":"edit","status":"pending","rawInput":{},"content":[]}}}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"tool_call_update","toolCallId":"call_1","title":"Write capture-demo.txt","kind":"edit","rawInput":{"content":"hello capture","file_path":"/workspace/capture-demo.txt"},"content":[{"type":"diff","path":"/workspace/capture-demo.txt","oldText":null,"newText":"hello capture"}]}}}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"tool_call_update","toolCallId":"call_1","status":"completed","rawOutput":"File created successfully at: /workspace/capture-demo.txt"}}}`,
	}, "\n")))
	if err != nil {
		t.Fatalf("MapReader: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("events = %d, want start + enrichment + result", len(events))
	}
	enrichment := payloadMap(t, events[1])
	if enrichment["phase"] != "start" {
		t.Fatalf("enrichment phase = %v", enrichment["phase"])
	}
	diffs, ok := enrichment["diffs"].([]any)
	if !ok || len(diffs) != 1 {
		t.Fatalf("enrichment diffs = %#v", enrichment["diffs"])
	}
	first := diffs[0].(map[string]any)
	if first["path_base"] != "capture-demo.txt" {
		t.Fatalf("path_base = %v", first["path_base"])
	}
	if first["new_text"] != "hello capture" {
		t.Fatalf("new_text = %v", first["new_text"])
	}
	if enrichment["title"] != "Write capture-demo.txt" {
		t.Fatalf("title = %v", enrichment["title"])
	}
}
