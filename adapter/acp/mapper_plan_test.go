package acp_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/adapter/acp"
)

func planMapper(t *testing.T, provider string) *acp.Mapper {
	t.Helper()
	mapper, err := acp.NewMapper(acp.Config{SessionID: "ses_1", Provider: provider, Now: func() time.Time {
		return time.UnixMilli(1700000000456)
	}})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}
	return mapper
}

func mapPlanLine(t *testing.T, mapper *acp.Mapper, update string) map[string]any {
	t.Helper()
	events, err := mapper.MapLine([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":` + update + `}}`))
	if err != nil {
		t.Fatalf("MapLine() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want exactly one plan frame for %s", len(events), update)
	}
	assertEvent(t, events[0], "agent.activity")
	return payloadMap(t, events[0])
}

// The structured `plan` update is what claude-agent-acp emits from its task
// hooks, and it replaces the client's whole plan.
func TestMapperMapsStructuredPlanEntries(t *testing.T) {
	t.Parallel()

	mapper := planMapper(t, "claude-code")
	payload := mapPlanLine(t, mapper, `{"sessionUpdate":"plan","entries":[
		{"content":"Read the existing session store","status":"completed","priority":"high"},
		{"content":"Add the durable backfill path","status":"in_progress","priority":"high"},
		{"content":"Update the spec","status":"pending","priority":"low"}
	]}`)

	if payload["kind"] != "plan" {
		t.Fatalf("kind = %v, want plan", payload["kind"])
	}
	if payload["provider_session_id"] != "acp_ses_1" {
		t.Fatalf("provider_session_id = %v", payload["provider_session_id"])
	}
	entries, ok := payload["entries"].([]any)
	if !ok || len(entries) != 3 {
		t.Fatalf("entries = %#v", payload["entries"])
	}
	first := entries[0].(map[string]any)
	if first["content"] != "Read the existing session store" || first["status"] != "completed" || first["priority"] != "high" {
		t.Fatalf("first entry = %+v", first)
	}
	if entries[1].(map[string]any)["status"] != "in_progress" {
		t.Fatalf("second entry = %+v", entries[1])
	}
	if _, truncated := payload["truncated"]; truncated {
		t.Fatalf("small plan must not be marked truncated: %+v", payload)
	}
}

// codex-acp emits the experimental markdown form and spells the plan id
// `planId` where the ACP schema says `id`.
func TestMapperMapsCodexMarkdownPlanUpdate(t *testing.T) {
	t.Parallel()

	mapper := planMapper(t, "codex")
	payload := mapPlanLine(t, mapper, `{"sessionUpdate":"plan_update","plan":{"type":"markdown","planId":"plan-7","content":"1. Read the store\n2. Add the path"}}`)

	if payload["kind"] != "plan" || payload["plan_id"] != "plan-7" {
		t.Fatalf("payload = %+v", payload)
	}
	if payload["markdown"] != "1. Read the store\n2. Add the path" {
		t.Fatalf("markdown = %v", payload["markdown"])
	}
}

func TestMapperMapsPlanUpdateItemsShape(t *testing.T) {
	t.Parallel()

	mapper := planMapper(t, "codex")
	payload := mapPlanLine(t, mapper, `{"sessionUpdate":"plan_update","plan":{"type":"items","id":"plan-2","entries":[
		{"content":"Only entry","status":"pending","priority":"medium"}
	]}}`)

	entries, ok := payload["entries"].([]any)
	if !ok || len(entries) != 1 || entries[0].(map[string]any)["content"] != "Only entry" {
		t.Fatalf("payload = %+v", payload)
	}
	if payload["plan_id"] != "plan-2" {
		t.Fatalf("plan_id = %v", payload["plan_id"])
	}
}

func TestMapperReducesFilePlanToBaseName(t *testing.T) {
	t.Parallel()

	mapper := planMapper(t, "claude-code")
	payload := mapPlanLine(t, mapper, `{"sessionUpdate":"plan_update","plan":{"type":"file","id":"plan-9","uri":"file:///home/agent/private/work/docs/plan.md"}}`)

	if payload["file_name"] != "plan.md" {
		t.Fatalf("file_name = %v, want the base name only", payload["file_name"])
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if strings.Contains(string(encoded), "/home/agent") {
		t.Fatalf("plan payload leaked a host path: %s", encoded)
	}
}

func TestMapperMapsPlanRemoval(t *testing.T) {
	t.Parallel()

	mapper := planMapper(t, "claude-code")
	payload := mapPlanLine(t, mapper, `{"sessionUpdate":"plan_removed","id":"plan-3"}`)

	if payload["kind"] != "plan_removed" || payload["plan_id"] != "plan-3" {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestMapperDropsUnusablePlanUpdates(t *testing.T) {
	t.Parallel()

	mapper := planMapper(t, "claude-code")
	for name, update := range map[string]string{
		"empty entries":     `{"sessionUpdate":"plan","entries":[]}`,
		"blank entry text":  `{"sessionUpdate":"plan","entries":[{"content":"   ","status":"pending"}]}`,
		"entries not array": `{"sessionUpdate":"plan","entries":{"content":"nope"}}`,
		"no plan body":      `{"sessionUpdate":"plan_update"}`,
		"unknown plan type": `{"sessionUpdate":"plan_update","plan":{"type":"calendar","id":"p"}}`,
		"no shape at all":   `{"sessionUpdate":"plan"}`,
	} {
		events, err := mapper.MapLine([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":` + update + `}}`))
		if err != nil {
			t.Fatalf("%s: MapLine() error = %v", name, err)
		}
		if len(events) != 0 {
			t.Fatalf("%s: events = %+v, want none", name, events)
		}
	}
}

func TestMapperBoundsPlanSize(t *testing.T) {
	t.Parallel()

	mapper := planMapper(t, "claude-code")

	var many []string
	for index := 0; index < 200; index++ {
		many = append(many, `{"content":"step `+strings.Repeat("x", 40)+`","status":"pending","priority":"low"}`)
	}
	payload := mapPlanLine(t, mapper, `{"sessionUpdate":"plan","entries":[`+strings.Join(many, ",")+`]}`)
	entries, ok := payload["entries"].([]any)
	if !ok {
		t.Fatalf("entries = %#v", payload["entries"])
	}
	if len(entries) != 48 {
		t.Fatalf("entries = %d, want the 48-entry bound", len(entries))
	}
	if payload["truncated"] != true {
		t.Fatalf("dropped entries must be reported: %+v", payload)
	}

	longText := mapPlanLine(t, mapper, `{"sessionUpdate":"plan_update","plan":{"type":"markdown","content":"`+strings.Repeat("é", 9000)+`"}}`)
	markdown, _ := longText["markdown"].(string)
	if len(markdown) > 16*1024 {
		t.Fatalf("markdown = %d bytes, want the bounded size", len(markdown))
	}
	if longText["truncated"] != true {
		t.Fatalf("clamped markdown must be reported: %+v", longText)
	}
	if strings.ContainsRune(markdown, '�') {
		t.Fatal("markdown truncation split a multi-byte character")
	}
}

// Two updates in one ACP batch must produce two frames so the client can apply
// replace semantics; the newest wins.
func TestMapperEmitsOneFramePerPlanUpdate(t *testing.T) {
	t.Parallel()

	mapper := planMapper(t, "claude-code")
	events, err := mapper.MapReader(context.Background(), strings.NewReader(strings.Join([]string{
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"plan","entries":[{"content":"first","status":"pending","priority":"low"}]}}}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"plan","entries":[{"content":"second","status":"completed","priority":"high"}]}}}`,
	}, "\n")))
	if err != nil {
		t.Fatalf("MapReader() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want one frame per update", len(events))
	}
	last := payloadMap(t, events[1])
	entries := last["entries"].([]any)
	if entries[0].(map[string]any)["content"] != "second" {
		t.Fatalf("last frame = %+v", last)
	}
}
