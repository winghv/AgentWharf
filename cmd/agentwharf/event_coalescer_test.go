package main

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/winghv/agentwharf/protocol"
)

func coalesceEvent(role, text string, timeMS int64) protocol.Event {
	payload, _ := json.Marshal(map[string]any{
		"message_id":          "message_1",
		"role":                role,
		"provider_session_id": "provider_1",
		"content": []map[string]any{{
			"kind": "text",
			"text": text,
		}},
	})
	return protocol.Event{
		Type:      "session.message",
		SessionID: "session_1",
		Time:      timeMS,
		Payload:   payload,
	}
}

func coalesceText(t *testing.T, event protocol.Event) string {
	t.Helper()
	var payload struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if len(payload.Content) != 1 {
		t.Fatalf("content = %+v, want one text part", payload.Content)
	}
	return payload.Content[0].Text
}

func TestProviderEventCoalescerMergesAdjacentAgentText(t *testing.T) {
	var sent []protocol.Event
	coalescer := newProviderEventCoalescer(func(event protocol.Event) error {
		sent = append(sent, event)
		return nil
	})
	if err := coalescer.Push(coalesceEvent("agent", "hello ", 1)); err != nil {
		t.Fatal(err)
	}
	if err := coalescer.Push(coalesceEvent("agent", "world", 2)); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 0 {
		t.Fatalf("sent before flush = %d, want 0", len(sent))
	}
	if err := coalescer.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 {
		t.Fatalf("sent = %d, want one coalesced message", len(sent))
	}
	if got := coalesceText(t, sent[0]); got != "hello world" {
		t.Fatalf("text = %q, want %q", got, "hello world")
	}
	if sent[0].Time != 2 {
		t.Fatalf("time = %d, want latest chunk time 2", sent[0].Time)
	}
}

func TestProviderEventCoalescerFlushesBeforeNonMessage(t *testing.T) {
	var sent []protocol.Event
	coalescer := newProviderEventCoalescer(func(event protocol.Event) error {
		sent = append(sent, event)
		return nil
	})
	if err := coalescer.Push(coalesceEvent("agent", "text", 1)); err != nil {
		t.Fatal(err)
	}
	state := protocol.Event{Type: "session.state", SessionID: "session_1", Time: 2, Payload: []byte(`{"state":"ready"}`)}
	if err := coalescer.Push(state); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 2 || sent[0].Type != "session.message" || sent[1].Type != "session.state" {
		t.Fatalf("sent order = %+v, want message then state", sent)
	}
}

func TestProviderEventCoalescerDoesNotMergeUserOrErrorMessages(t *testing.T) {
	var sent []protocol.Event
	coalescer := newProviderEventCoalescer(func(event protocol.Event) error {
		sent = append(sent, event)
		return nil
	})
	if err := coalescer.Push(coalesceEvent("user", "ask", 1)); err != nil {
		t.Fatal(err)
	}
	if err := coalescer.Push(coalesceEvent("agent", "answer", 2)); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 || sent[0].Type != "session.message" {
		t.Fatalf("user message was not sent immediately: %+v", sent)
	}
	if err := coalescer.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 2 {
		t.Fatalf("sent = %d, want user and agent", len(sent))
	}
}

func TestProviderEventCoalescerFlushesAtWindowBoundary(t *testing.T) {
	var sent []protocol.Event
	now := time.Unix(100, 0)
	coalescer := newProviderEventCoalescer(func(event protocol.Event) error {
		sent = append(sent, event)
		return nil
	})
	coalescer.now = func() time.Time { return now }
	if err := coalescer.Push(coalesceEvent("agent", "first", 1)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(200 * time.Millisecond)
	if err := coalescer.Push(coalesceEvent("agent", "second", 2)); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 || coalesceText(t, sent[0]) != "first" {
		t.Fatalf("sent = %+v, want the first window flushed alone", sent)
	}
	if err := coalescer.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 2 || coalesceText(t, sent[1]) != "second" {
		t.Fatalf("sent = %+v, want second message after flush", sent)
	}
}

func TestProviderEventCoalescerFlushesAtSizeBoundary(t *testing.T) {
	var sent []protocol.Event
	coalescer := newProviderEventCoalescer(func(event protocol.Event) error {
		sent = append(sent, event)
		return nil
	})
	coalescer.maxBytes = 120
	if err := coalescer.Push(coalesceEvent("agent", "1234567890", 1)); err != nil {
		t.Fatal(err)
	}
	if err := coalescer.Push(coalesceEvent("agent", "abcdefghij", 2)); err != nil {
		t.Fatal(err)
	}
	if len(sent) == 0 {
		t.Fatal("size boundary did not flush")
	}
}

func TestProviderEventCoalescerPropagatesSendError(t *testing.T) {
	boom := errors.New("boom")
	coalescer := newProviderEventCoalescer(func(protocol.Event) error { return boom })
	if err := coalescer.Push(coalesceEvent("user", "ask", 1)); !errors.Is(err, boom) {
		t.Fatalf("Push error = %v, want boom", err)
	}
}

func TestProviderEventCoalescerFlushesOnTimerWhenProviderGoesSilent(t *testing.T) {
	sent := make(chan protocol.Event, 1)
	coalescer := newProviderEventCoalescer(func(event protocol.Event) error {
		sent <- event
		return nil
	})
	coalescer.window = 20 * time.Millisecond
	if err := coalescer.Push(coalesceEvent("agent", "silent", 1)); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-sent:
		if got := coalesceText(t, event); got != "silent" {
			t.Fatalf("text = %q, want silent", got)
		}
	case <-time.After(time.Second):
		t.Fatal("coalescer did not flush a silent provider within its window")
	}
}
