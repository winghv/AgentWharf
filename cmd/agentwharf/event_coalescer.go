package main

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/winghv/agentwharf/protocol"
)

// Provider bridges stream assistant prose in small chunks. Persisting every
// chunk as its own durable event makes history pages count transport frames
// instead of messages and forces every client to reassemble the reply. The
// coalescer folds consecutive agent text chunks into one bounded
// session.message before the masker and the Hub see them.
//
// It is deliberately synchronous: the provider output loop calls Push for each
// event, and a non-message event (tool call, state, permission) flushes the
// pending text first. A poll loop flushes at the end of each read batch, so a
// paused provider never leaves text buffered indefinitely.
const (
	providerMessageCoalesceWindow = 100 * time.Millisecond
	providerMessageCoalesceBytes  = 32 * 1024
)

type providerEventCoalescer struct {
	send     func(protocol.Event) error
	now      func() time.Time
	window   time.Duration
	maxBytes int
	mu       sync.Mutex
	pending  *protocol.Event
	started  time.Time
	timer    *time.Timer
}

func newProviderEventCoalescer(send func(protocol.Event) error) *providerEventCoalescer {
	return &providerEventCoalescer{
		send:     send,
		now:      time.Now,
		window:   providerMessageCoalesceWindow,
		maxBytes: providerMessageCoalesceBytes,
	}
}

// Push accepts one translated provider event. Non-message events pass through
// immediately after flushing the pending text, preserving Hub sequence order.
func (c *providerEventCoalescer) Push(event protocol.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !coalescibleAgentMessage(event) {
		if err := c.flushLocked(); err != nil {
			return err
		}
		return c.send(event)
	}
	if c.pending == nil {
		c.pending = &event
		c.started = c.now()
		if len(event.Payload) >= c.maxBytes {
			return c.flushLocked()
		}
		c.armTimerLocked()
		return nil
	}
	if !sameCoalescibleMessage(*c.pending, event) {
		if err := c.flushLocked(); err != nil {
			return err
		}
		c.pending = &event
		c.started = c.now()
		c.armTimerLocked()
		return nil
	}
	merged, ok := mergeCoalescedMessage(*c.pending, event)
	if !ok || len(merged.Payload) > c.maxBytes || c.now().Sub(c.started) >= c.window {
		if err := c.flushLocked(); err != nil {
			return err
		}
		c.pending = &event
		c.started = c.now()
		if len(event.Payload) >= c.maxBytes {
			return c.flushLocked()
		}
		c.armTimerLocked()
		return nil
	}
	merged.Time = event.Time
	c.pending = &merged
	c.armTimerLocked()
	return nil
}

// armTimerLocked flushes a lone delta even when the provider goes silent
// before the next chunk or terminal event. The caller holds c.mu.
func (c *providerEventCoalescer) armTimerLocked() {
	if c.timer != nil {
		c.timer.Stop()
	}
	c.timer = time.AfterFunc(c.window, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		_ = c.flushLocked()
	})
}

func (c *providerEventCoalescer) flushLocked() error {
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	if c.pending == nil {
		return nil
	}
	event := *c.pending
	c.pending = nil
	c.started = time.Time{}
	return c.send(event)
}

// Flush publishes the pending message, if any. It is safe to call repeatedly.
func (c *providerEventCoalescer) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flushLocked()
}

// Close flushes the tail of the last turn. It exists so call sites can make the
// shutdown contract explicit even though Flush is idempotent.
func (c *providerEventCoalescer) Close() error {
	return c.Flush()
}

func coalescibleAgentMessage(event protocol.Event) bool {
	if event.Type != "session.message" {
		return false
	}
	var payload map[string]any
	if json.Unmarshal(event.Payload, &payload) != nil {
		return false
	}
	role, _ := payload["role"].(string)
	if role != "agent" && role != "assistant" {
		return false
	}
	messageID, _ := payload["message_id"].(string)
	if messageID == "acp-rpc-error" || messageID == "acp-provider-error" {
		return false
	}
	return true
}

func sameCoalescibleMessage(left, right protocol.Event) bool {
	if left.Type != right.Type || left.SessionID != right.SessionID {
		return false
	}
	var leftPayload, rightPayload map[string]any
	if json.Unmarshal(left.Payload, &leftPayload) != nil || json.Unmarshal(right.Payload, &rightPayload) != nil {
		return false
	}
	leftProvider, _ := leftPayload["provider_session_id"].(string)
	rightProvider, _ := rightPayload["provider_session_id"].(string)
	return leftProvider == "" || rightProvider == "" || leftProvider == rightProvider
}

func mergeCoalescedMessage(left, right protocol.Event) (protocol.Event, bool) {
	var leftPayload, rightPayload map[string]any
	if json.Unmarshal(left.Payload, &leftPayload) != nil || json.Unmarshal(right.Payload, &rightPayload) != nil {
		return protocol.Event{}, false
	}
	leftContent := contentParts(leftPayload["content"])
	rightContent := contentParts(rightPayload["content"])
	mergedContent := make([]any, 0, len(leftContent)+len(rightContent))
	mergedContent = append(mergedContent, leftContent...)
	for _, part := range rightContent {
		last := len(mergedContent) - 1
		lastPart, lastOK := mergedContent[last].(map[string]any)
		nextPart, nextOK := part.(map[string]any)
		if lastOK && nextOK && textPart(lastPart) && textPart(nextPart) {
			lastPart["text"] = textOf(lastPart) + textOf(nextPart)
			mergedContent[last] = lastPart
			continue
		}
		mergedContent = append(mergedContent, part)
	}
	payload := make(map[string]any, len(leftPayload))
	for key, value := range leftPayload {
		payload[key] = value
	}
	for key, value := range rightPayload {
		if key == "content" {
			continue
		}
		payload[key] = value
	}
	payload["content"] = mergedContent
	if messageID, _ := leftPayload["message_id"].(string); messageID == "" {
		if rightID, _ := rightPayload["message_id"].(string); rightID != "" {
			payload["message_id"] = rightID
		}
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return protocol.Event{}, false
	}
	merged := left
	merged.Payload = encoded
	merged.Time = right.Time
	return merged, true
}

func contentParts(value any) []any {
	parts, ok := value.([]any)
	if !ok {
		return nil
	}
	return parts
}

func textPart(part map[string]any) bool {
	kind, _ := part["kind"].(string)
	return kind == "text"
}

func textOf(part map[string]any) string {
	text, _ := part["text"].(string)
	return text
}
