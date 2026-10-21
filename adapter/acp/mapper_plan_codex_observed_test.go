package acp_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/winghv/agentwharf/adapter/acp"
)

// codexPlanFrame renders the frame the codex bridge sent for one plan
// snapshot. The captured run produced twelve of them for a single plan,
// growing from the first line to the whole document.
func codexPlanFrame(content string) string {
	return fmt.Sprintf(
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"acp_ses_1","update":{"sessionUpdate":"plan_update","plan":{"type":"markdown","planId":"01a15d38-d39f-76f1-bfe1-dd18f903aa06-plan","content":%q}}}}`,
		content,
	)
}

// The frame shape captured from the deployed codex bridge: a plan arrives as a
// markdown document under `plan_update`, streamed as the model writes it. The
// bridge only sends it to a client that declared the plan capability; without
// that declaration the same plan arrives as plain agent prose and never as a
// plan.
func TestMapperMapsObservedCodexPlanUpdateFrame(t *testing.T) {
	t.Parallel()

	mapper, err := acp.NewMapper(acp.Config{SessionID: "ses_1", Provider: "codex", Now: func() time.Time {
		return time.UnixMilli(1700000000456)
	}})
	if err != nil {
		t.Fatalf("NewMapper() error = %v", err)
	}

	first := "**创建 `hello.txt`**\n\n"
	whole := "**创建 `hello.txt`**\n\n**摘要**\n- 在 `/workspace` 下创建文件 `hello.txt`。\n- 写入文本内容 `hello`。\n\n"
	events, err := mapper.MapReader(context.Background(), strings.NewReader(strings.Join([]string{
		codexPlanFrame(first),
		codexPlanFrame(whole),
	}, "\n")))
	if err != nil {
		t.Fatalf("MapReader() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want one frame per plan snapshot", len(events))
	}

	firstPayload := payloadMap(t, events[0])
	if firstPayload["kind"] != "plan" {
		t.Fatalf("kind = %v, want plan", firstPayload["kind"])
	}
	if firstPayload["plan_id"] != "01a15d38-d39f-76f1-bfe1-dd18f903aa06-plan" {
		t.Fatalf("plan_id = %v, want the provider's plan id", firstPayload["plan_id"])
	}
	markdown, ok := firstPayload["markdown"].(string)
	if !ok || markdown == "" {
		t.Fatalf("markdown = %#v, want the plan document", firstPayload["markdown"])
	}
	if !strings.Contains(markdown, "创建 `hello.txt`") {
		t.Fatalf("markdown = %q, want the provider's own plan text", markdown)
	}

	lastPayload := payloadMap(t, events[1])
	if lastPayload["kind"] != "plan" {
		t.Fatalf("kind = %v, want plan", lastPayload["kind"])
	}
	grown, ok := lastPayload["markdown"].(string)
	if !ok || len(grown) <= len(markdown) {
		t.Fatalf("markdown = %q, want the grown plan document", grown)
	}
	if !strings.Contains(grown, "写入文本内容 `hello`") {
		t.Fatalf("markdown = %q, want the whole plan", grown)
	}
}
