package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/winghv/agentwharf/protocol"
)

// localTerminal renders a Session's translated events to the user's terminal
// and reads their inline prompts and permission decisions, so wharf claude /
// wharf codex behaves like the native agent CLI while the Hub mirrors the same
// Session. It is only used when stdin is a character device (a real terminal).
type localTerminal struct {
	out           io.Writer
	permissionMu  *sync.Mutex
	pending       map[string]acpPendingPermission
	permissionOut func(requestID, decision string) error
	promptOut     func(text string) error

	mu        sync.Mutex
	promptReq string
	promptIDs atomic.Int64
}

func newLocalTerminal(out io.Writer, pending map[string]acpPendingPermission, permissionMu *sync.Mutex, promptOut func(string) error, permissionOut func(string, string) error) *localTerminal {
	t := &localTerminal{out: out, pending: pending, permissionMu: permissionMu, promptOut: promptOut, permissionOut: permissionOut}
	t.promptIDs.Store(1 << 30)
	return t
}

func (t *localTerminal) nextPromptID() int64 {
	return t.promptIDs.Add(1)
}

func (t *localTerminal) showPermission(requestID string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.promptReq = requestID
	t.mu.Unlock()
}

func (t *localTerminal) takePermissionDecision() string {
	if t == nil {
		return ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	requestID := t.promptReq
	t.promptReq = ""
	return requestID
}

func (t *localTerminal) render(event protocol.Event) {
	if t == nil || t.out == nil {
		return
	}
	switch event.Type {
	case "session.message":
		t.renderMessage(event)
	case "session.tool_call":
		t.renderToolCall(event)
	case "permission.request":
		t.renderPermissionRequest(event)
	}
}

func (t *localTerminal) renderMessage(event protocol.Event) {
	var raw map[string]any
	if json.Unmarshal(event.Payload, &raw) != nil {
		return
	}
	content, _ := raw["content"].([]any)
	for _, item := range content {
		block, _ := item.(map[string]any)
		if stringFieldFromAny(block["kind"]) == "text" {
			text := stringFieldFromAny(block["text"])
			if strings.TrimSpace(text) != "" {
				fmt.Fprintln(t.out, text)
			}
		}
	}
}

func (t *localTerminal) renderToolCall(event protocol.Event) {
	var raw map[string]any
	if json.Unmarshal(event.Payload, &raw) != nil {
		return
	}
	name := stringFieldFromAny(raw["name"])
	if name == "" {
		return
	}
	switch stringFieldFromAny(raw["phase"]) {
	case "start":
		fmt.Fprintln(t.out, "[tool] "+name)
	default:
		if _, ok := raw["result"]; ok && raw["result"] != nil {
			fmt.Fprintln(t.out, "  [done]")
		}
	}
}

func (t *localTerminal) renderPermissionRequest(event protocol.Event) {
	var raw map[string]any
	if json.Unmarshal(event.Payload, &raw) != nil {
		return
	}
	requestID := stringFieldFromAny(raw["request_id"])
	if requestID == "" {
		return
	}
	action := stringFieldFromAny(raw["action"])
	if action == "" {
		action = "permission"
	}
	summary := stringFieldFromAny(raw["summary"])
	if summary != "" {
		fmt.Fprintln(t.out, "[permission] "+action+": "+summary)
	} else {
		fmt.Fprintln(t.out, "[permission] "+action)
	}
	fmt.Fprintln(t.out, "  allow / deny?")
	t.showPermission(requestID)
}

// readInput consumes the user's terminal until the session ends. Each line is
// either a permission decision (when a permission was just rendered) or a new
// prompt. It is a best-effort surface: input errors stop only this loop.
func (t *localTerminal) readInput(ctx context.Context, stdin io.Reader) {
	if t == nil || stdin == nil {
		return
	}
	scanner := bufio.NewScanner(stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if requestID := t.takePermissionDecision(); requestID != "" {
			if err := t.permissionOut(requestID, localPermissionDecision(line)); err != nil {
				fmt.Fprintln(t.out, "wharf: permission response failed: "+err.Error())
			}
			continue
		}
		if err := t.promptOut(line); err != nil {
			fmt.Fprintln(t.out, "wharf: send prompt failed: "+err.Error())
			return
		}
		fmt.Fprintln(t.out, "> "+line)
	}
}

// localPermissionDecision maps a terminal answer to the decision string the
// Hub/Adapter permission path expects ("approve" vs anything else).
func localPermissionDecision(line string) string {
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "allow", "y", "yes", "approve", "ok", "1":
		return "approve"
	default:
		return "reject"
	}
}
