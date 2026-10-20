package acp_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/adapter/acp"
)

func diffMapper(t *testing.T) *acp.Mapper {
	t.Helper()
	mapper, err := acp.NewMapper(acp.Config{SessionID: "ses_1", Provider: "claude-code", Now: func() time.Time {
		return time.UnixMilli(1700000000456)
	}})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}
	return mapper
}

func mapToolLine(t *testing.T, mapper *acp.Mapper, update string) map[string]any {
	t.Helper()
	events, err := mapper.MapLine([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":` + update + `}}`))
	if err != nil {
		t.Fatalf("MapLine() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %+v, want one tool frame", events)
	}
	return payloadMap(t, events[0])
}

// The Claude bridge sends a diff block for every Edit: the replaced fragment
// and the inserted one, plus the host path.
func TestMapperMapsToolCallDiffs(t *testing.T) {
	t.Parallel()

	payload := mapToolLine(t, diffMapper(t), `{"sessionUpdate":"tool_call","toolCallId":"call_1","title":"Edit src/app.ts","kind":"edit","status":"pending","content":[
		{"type":"diff","path":"/workspace/src/app.ts","oldText":"const port = 3000","newText":"const port = 8080"}
	]}`)

	diffs, ok := payload["diffs"].([]any)
	if !ok || len(diffs) != 1 {
		t.Fatalf("diffs = %#v", payload["diffs"])
	}
	first := diffs[0].(map[string]any)
	if first["path_base"] != "app.ts" {
		t.Fatalf("path_base = %v", first["path_base"])
	}
	if first["old_text"] != "const port = 3000" || first["new_text"] != "const port = 8080" {
		t.Fatalf("diff = %+v", first)
	}
}

// A Write arrives with no old text: the file is new, and the card must be able
// to say so rather than render an empty "before".
func TestMapperMapsWriteDiffWithoutOldText(t *testing.T) {
	t.Parallel()

	payload := mapToolLine(t, diffMapper(t), `{"sessionUpdate":"tool_call","toolCallId":"call_2","title":"Write notes.md","kind":"edit","status":"pending","content":[
		{"type":"diff","path":"/workspace/docs/notes.md","oldText":null,"newText":"# Notes\n"}
	]}`)

	first := payload["diffs"].([]any)[0].(map[string]any)
	if _, present := first["old_text"]; present {
		t.Fatalf("a new file must not carry old_text: %+v", first)
	}
	if first["new_text"] != "# Notes" {
		t.Fatalf("new_text = %v, want the trailing newline trimmed", first["new_text"])
	}
}

// `Diff.path` is an absolute host path and must not reach durable events.
func TestMapperReducesDiffPathToBaseName(t *testing.T) {
	t.Parallel()

	payload := mapToolLine(t, diffMapper(t), `{"sessionUpdate":"tool_call","toolCallId":"call_3","title":"Edit","kind":"edit","status":"pending","content":[
		{"type":"diff","path":"file:///home/tenant/Projects/secret-client/src/main.rs","oldText":"a","newText":"b"}
	]}`)

	first := payload["diffs"].([]any)[0].(map[string]any)
	if first["path_base"] != "main.rs" {
		t.Fatalf("path_base = %v", first["path_base"])
	}
	encoded, err := json.Marshal(payload["diffs"])
	if err != nil {
		t.Fatalf("marshal diffs: %v", err)
	}
	for _, leak := range []string{"/home/tenant", "Projects", "secret-client"} {
		if strings.Contains(string(encoded), leak) {
			t.Fatalf("diffs leaked %q: %s", leak, encoded)
		}
	}
}

// An editing tool may report the change on the terminal frame instead.
func TestMapperMapsDiffsOnTheResultFrame(t *testing.T) {
	t.Parallel()

	mapper := diffMapper(t)
	events, err := mapper.MapReader(context.Background(), strings.NewReader(strings.Join([]string{
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"tool_call","toolCallId":"call_4","title":"Edit","kind":"edit","status":"in_progress"}}}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"tool_call_update","toolCallId":"call_4","status":"completed","rawOutput":"ok","content":[{"type":"diff","path":"/workspace/a.ts","oldText":"x","newText":"y"}]}}}`,
	}, "\n")))
	if err != nil {
		t.Fatalf("MapReader() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want start and result", len(events))
	}
	result := payloadMap(t, events[1])
	if result["phase"] != "result" {
		t.Fatalf("second frame = %+v", result)
	}
	diffs, ok := result["diffs"].([]any)
	if !ok || len(diffs) != 1 {
		t.Fatalf("result diffs = %#v", result["diffs"])
	}
	if result["result"] == nil {
		t.Fatalf("attaching diffs must not drop the tool result: %+v", result)
	}
}

func TestMapperBoundsDiffContent(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("x", 40_000)
	payload := mapToolLine(t, diffMapper(t), `{"sessionUpdate":"tool_call","toolCallId":"call_5","title":"Edit","kind":"edit","status":"pending","content":[
		{"type":"diff","path":"/workspace/huge.ts","oldText":"`+long+`","newText":"`+long+`"}
	]}`)

	first := payload["diffs"].([]any)[0].(map[string]any)
	oldText, _ := first["old_text"].(string)
	newText, _ := first["new_text"].(string)
	if first["old_text_truncated"] != true || first["new_text_truncated"] != true {
		t.Fatalf("truncation must be reported: %+v", first)
	}
	// Both sides survive: a card that only shows what was removed is worse
	// than no card at all.
	if oldText == "" || newText == "" {
		t.Fatalf("both sides must survive the bound: %+v", first)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if len(encoded) > 64*1024 {
		t.Fatalf("payload = %d bytes, over the event budget", len(encoded))
	}
}

func TestMapperKeepsDiffsWithinTheFrameBudget(t *testing.T) {
	t.Parallel()

	var blocks []string
	for index := 0; index < 12; index++ {
		body := strings.Repeat("y", 8*1024)
		blocks = append(blocks, `{"type":"diff","path":"/workspace/f`+string(rune('a'+index))+`.ts","oldText":"","newText":"`+body+`"}`)
	}
	payload := mapToolLine(t, diffMapper(t), `{"sessionUpdate":"tool_call","toolCallId":"call_6","title":"Edit","kind":"edit","status":"pending","content":[`+strings.Join(blocks, ",")+`]}`)

	diffs := payload["diffs"].([]any)
	if len(diffs) == 0 || len(diffs) > 8 {
		t.Fatalf("diffs = %d, want a bounded, non-empty set", len(diffs))
	}
	// Every card that survives the byte budget must carry content: dropping the
	// rest beats rendering empty rows.
	for _, item := range diffs {
		entry := item.(map[string]any)
		if entry["new_text"] == nil && entry["old_text"] == nil {
			t.Fatalf("emitted an empty diff: %+v", entry)
		}
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if len(encoded) > 64*1024 {
		t.Fatalf("payload = %d bytes, over the event budget", len(encoded))
	}
}

func TestMapperDropsUnusableDiffBlocks(t *testing.T) {
	t.Parallel()

	for name, content := range map[string]string{
		"no path":       `[{"type":"diff","oldText":"a","newText":"b"}]`,
		"blank path":    `[{"type":"diff","path":"   ","oldText":"a","newText":"b"}]`,
		"not a diff":    `[{"type":"content","content":{"type":"text","text":"hi"}}]`,
		"empty content": `[]`,
		"not an array":  `{"type":"diff","path":"/workspace/a.ts"}`,
	} {
		events, err := diffMapper(t).MapLine([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"tool_call","toolCallId":"call_7","title":"Edit","kind":"edit","status":"pending","content":` + content + `}}}`))
		if err != nil {
			t.Fatalf("%s: MapLine() error = %v", name, err)
		}
		if len(events) != 1 {
			t.Fatalf("%s: events = %+v", name, events)
		}
		if _, present := payloadMap(t, events[0])["diffs"]; present {
			t.Fatalf("%s: an unusable diff must not reach the payload", name)
		}
	}
}

// A tool call without diffs must be byte-identical to before this change.
func TestMapperLeavesNonEditingToolCallsUnchanged(t *testing.T) {
	t.Parallel()

	payload := mapToolLine(t, diffMapper(t), `{"sessionUpdate":"tool_call","toolCallId":"call_8","title":"Run npm test","kind":"execute","status":"pending","rawInput":{"command":"npm test"}}`)
	if _, present := payload["diffs"]; present {
		t.Fatalf("a command tool call must not grow a diffs field: %+v", payload)
	}
	if payload["name"] != "execute" || payload["phase"] != "start" {
		t.Fatalf("payload = %+v", payload)
	}
}

// A provider can report a change it cannot describe: codex sends a diff block
// with no text on either side when the edit arrives as a patch without any
// +/- lines. Publishing it would render an empty change card, which reads as
// a broken card rather than as "no textual change".
func TestMapperDropsDiffsWithoutText(t *testing.T) {
	t.Parallel()

	events, err := diffMapper(t).MapLine([]byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"tool_call","toolCallId":"call_1","title":"Editing files","kind":"edit","status":"in_progress","content":[{"type":"diff","path":"/workspace/plan-demo.txt","oldText":null,"newText":""}]}}}`))
	if err != nil {
		t.Fatalf("MapLine() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want the tool call itself", len(events))
	}
	if _, present := payloadMap(t, events[0])["diffs"]; present {
		t.Fatalf("a diff without text must not reach the payload: %+v", payloadMap(t, events[0]))
	}
}
