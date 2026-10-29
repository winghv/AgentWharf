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
		{"type":"diff","path":"file:///home/user/Projects/secret-client/src/main.rs","oldText":"a","newText":"b"}
	]}`)

	first := payload["diffs"].([]any)[0].(map[string]any)
	if first["path_base"] != "main.rs" {
		t.Fatalf("path_base = %v", first["path_base"])
	}
	encoded, err := json.Marshal(payload["diffs"])
	if err != nil {
		t.Fatalf("marshal diffs: %v", err)
	}
	for _, leak := range []string{"/home/user", "Projects", "secret-client"} {
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

// pi-acp omits rawOutput when it attaches a structured diff to the completed
// update. Falling back to update["content"] serialized the raw diff block —
// including the unreduced host path — into output_preview and bloated the frame.
func TestMapperResultDiffOnlyContentDoesNotLeakHostPathOrDuplicatePreview(t *testing.T) {
	t.Parallel()

	mapper := diffMapper(t)
	oldCode := strings.Repeat("old line\n", 1800)
	newCode := strings.Repeat("new line\n", 2400)
	events, err := mapper.MapReader(context.Background(), strings.NewReader(strings.Join([]string{
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"tool_call","toolCallId":"call_pi_1","title":"write","kind":"edit","status":"in_progress"}}}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"tool_call_update","toolCallId":"call_pi_1","status":"completed","content":[{"type":"diff","path":"/Users/alice/projects/SqlIntOverflowUtils.java","oldText":` + mustJSONString(t, oldCode) + `,"newText":` + mustJSONString(t, newCode) + `}]}}}`,
	}, "\n")))
	if err != nil {
		t.Fatalf("MapReader() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want start and result", len(events))
	}
	if strings.Contains(string(events[1].Payload), "/Users/alice") {
		t.Fatalf("host path leaked into result payload: %s", string(events[1].Payload))
	}
	resultPayload := payloadMap(t, events[1])
	resultObj, _ := resultPayload["result"].(map[string]any)
	if preview, _ := resultObj["output_preview"].(string); preview != "" {
		t.Fatalf("diff-only content must not serialize into output_preview, got %d bytes", len(preview))
	}
	diffs, ok := resultPayload["diffs"].([]any)
	if !ok || len(diffs) != 1 {
		t.Fatalf("diffs = %#v, want 1 diff", resultPayload["diffs"])
	}
	firstDiff, _ := diffs[0].(map[string]any)
	if firstDiff["path_base"] != "SqlIntOverflowUtils.java" {
		t.Fatalf("path_base = %v, want SqlIntOverflowUtils.java", firstDiff["path_base"])
	}
}

func TestMapperHonorsConfiguredMaxPayloadBytes(t *testing.T) {
	t.Parallel()

	const budget = 31 * 1024
	mapper, err := acp.NewMapper(acp.Config{
		SessionID:       "ses_1",
		Provider:        "pi",
		MaxPayloadBytes: budget,
		Now: func() time.Time {
			return time.UnixMilli(1700000000456)
		},
	})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}

	largeFile := strings.Repeat("public final class SqlIntOverflowUtils {}\n", 1100)
	largeOutput := strings.Repeat("build output line with & escaping\n", 1400)
	largeMessage := strings.Repeat("assistant prose chunk & detail ", 1500)

	events, err := mapper.MapReader(context.Background(), strings.NewReader(strings.Join([]string{
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"tool_call","toolCallId":"call_budget_1","title":"write src/SqlIntOverflowUtils.java","kind":"edit","status":"in_progress","rawInput":{"path":"src/SqlIntOverflowUtils.java","content":` + mustJSONString(t, largeFile) + `},"content":[{"type":"diff","path":"/workspace/src/SqlIntOverflowUtils.java","oldText":null,"newText":` + mustJSONString(t, largeFile) + `}]}}}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"tool_call_update","toolCallId":"call_budget_1","status":"completed","rawOutput":` + mustJSONString(t, largeOutput) + `,"content":[{"type":"diff","path":"/workspace/src/SqlIntOverflowUtils.java","oldText":"old","newText":` + mustJSONString(t, largeFile) + `}]}}}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"agent_message_chunk","text":` + mustJSONString(t, largeMessage) + `}}}`,
	}, "\n")))
	if err != nil {
		t.Fatalf("MapReader() error = %v", err)
	}
	if len(events) < 3 {
		t.Fatalf("events = %d, want start, result, and message chunks", len(events))
	}
	for index, event := range events {
		if len(event.Payload) > budget {
			t.Fatalf("event[%d] (%s) payload = %d bytes, exceeds budget %d", index, event.Type, len(event.Payload), budget)
		}
	}

	startPayload := payloadMap(t, events[0])
	startInput, _ := startPayload["input"].(map[string]any)
	if startInput["_truncated"] != true || startInput["path"] != "src/SqlIntOverflowUtils.java" {
		t.Fatalf("compacted start input = %+v", startInput)
	}
	if diffs, ok := startPayload["diffs"].([]any); !ok || len(diffs) != 1 {
		t.Fatalf("compacting rawInput should preserve start diffs, got %#v", startPayload["diffs"])
	}

	resultPayload := payloadMap(t, events[1])
	resultObj, _ := resultPayload["result"].(map[string]any)
	if resultObj["truncated"] != true {
		t.Fatalf("result should report truncated output: %+v", resultObj)
	}
}

func mustJSONString(t *testing.T, value string) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal string: %v", err)
	}
	return string(encoded)
}
