package main

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/winghv/agentwharf/protocol"
)

// askUserQuestionOption is one selectable option in a question.
type askUserQuestionOption struct {
	Label       string `json:"label"`
	Description string `json:"description"`
}

// askUserQuestion mirrors claude-code's AskUserQuestion input shape.
type askUserQuestion struct {
	Question    string                  `json:"question"`
	Header      string                  `json:"header"`
	Options     []askUserQuestionOption `json:"options"`
	MultiSelect bool                    `json:"multiSelect"`
}

// isAskUserQuestionTool reports whether a tool name is claude's question tool.
func isAskUserQuestionTool(name string) bool {
	switch name {
	case "AskUserQuestion", "ask_user_question", "askUserQuestion":
		return true
	default:
		return false
	}
}

// parseAskUserQuestions parses the questions array from an AskUserQuestion input.
func parseAskUserQuestions(input json.RawMessage) []askUserQuestion {
	var raw struct {
		Questions []askUserQuestion `json:"questions"`
	}
	if len(input) == 0 || json.Unmarshal(input, &raw) != nil {
		return nil
	}
	return raw.Questions
}

// askUserQuestionOptionIndex returns the 0-based index of the option matching
// label, or -1 when absent.
func askUserQuestionOptionIndex(options []askUserQuestionOption, label string) int {
	for index, option := range options {
		if option.Label == label {
			return index
		}
	}
	return -1
}

// askUserQuestionKeystrokes returns the raw-mode keystrokes that select the
// given answers in the native claude TUI: down-arrow to the chosen option, then
// Enter, once per question (in question order). A missing or unknown answer
// falls back to the first option.
//
// UNTESTED against the live claude TUI layout: single-question prompts select
// correctly; multi-question prompts assume each Enter advances to the next
// question, which may differ across claude versions.
func askUserQuestionKeystrokes(questions []askUserQuestion, answers map[string]string) []byte {
	const down = "[B"
	var out []byte
	for _, question := range questions {
		label := answers[question.Question]
		index := askUserQuestionOptionIndex(question.Options, label)
		if index < 0 {
			index = 0
		}
		for i := 0; i < index; i++ {
			out = append(out, down...)
		}
		out = append(out, '')
	}
	return out
}

// transcriptToolResultEvent builds the session.tool_call result event from a
// claude transcript tool_result block.
func transcriptToolResultEvent(sessionID, toolCallID, name string, content []byte, isError bool) protocol.Event {
	status := "ok"
	if isError {
		status = "error"
	}
	payload := map[string]any{
		"tool_call_id": toolCallID,
		"phase":        "result",
		"input":        nil,
		"result": map[string]any{
			"status":         status,
			"output_preview": string(content),
			"truncated":      false,
		},
	}
	// The transcript result frame omits the name so the client merge keeps the
	// start frame's name; an explicit name (ACP path) is preserved when present.
	if name != "" {
		payload["name"] = name
	}
	encoded, _ := json.Marshal(payload)
	return protocol.Event{Type: "session.tool_call", SessionID: sessionID, Time: time.Now().UTC().UnixMilli(), Payload: encoded}
}

// transcriptQuestionPermissionRequest builds the permission.request event that
// surfaces an AskUserQuestion tool call as an answerable question. The
// request_id is stably derived from the tool_call_id.
func transcriptQuestionPermissionRequest(sessionID, toolCallID string, input json.RawMessage) protocol.Event {
	questions := parseAskUserQuestions(input)
	if questions == nil {
		questions = []askUserQuestion{}
	}
	payload, _ := json.Marshal(map[string]any{
		"request_id": "question:" + toolCallID,
		"action":     "ask_user_question",
		"risk_level": "low",
		"summary":    "Agent asks a question",
		"detail":     map[string]any{"questions": questions},
	})
	return protocol.Event{Type: "permission.request", SessionID: sessionID, Time: time.Now().UTC().UnixMilli(), Payload: payload}
}

// questionCache shares AskUserQuestion questions between the transcript mirror
// (writer) and the Hub command loop (reader).
type questionCache struct {
	mu        sync.RWMutex
	questions map[string][]askUserQuestion
}

func newQuestionCache() *questionCache {
	return &questionCache{questions: make(map[string][]askUserQuestion)}
}

func (c *questionCache) Set(toolCallID string, questions []askUserQuestion) {
	if toolCallID == "" || len(questions) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.questions[toolCallID] = questions
}

func (c *questionCache) Get(toolCallID string) []askUserQuestion {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.questions[toolCallID]
}

// cacheAskUserQuestionEvent caches the questions from a session.tool_call start
// event named AskUserQuestion, keyed by its tool_call_id.
func cacheAskUserQuestionEvent(cache *questionCache, event *protocol.Event) {
	if cache == nil || event == nil || event.Type != "session.tool_call" {
		return
	}
	var payload struct {
		ToolCallID string          `json:"tool_call_id"`
		Phase      string          `json:"phase"`
		Name       string          `json:"name"`
		Input      json.RawMessage `json:"input"`
	}
	if json.Unmarshal(event.Payload, &payload) != nil {
		return
	}
	if payload.Phase != "start" || !isAskUserQuestionTool(payload.Name) {
		return
	}
	if questions := parseAskUserQuestions(payload.Input); len(questions) > 0 {
		cache.Set(payload.ToolCallID, questions)
	}
}
