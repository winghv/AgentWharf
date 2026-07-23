package protocol

import (
	"testing"
)

func TestDecodeAttentionFramesRejectsUnknownOrReplayFields(t *testing.T) {
	valid := []byte(`{"frame":"attention.subscribe","request_id":"attn_01H8X"}`)
	frame, err := Decode(valid)
	if err != nil {
		t.Fatalf("Decode(valid attention subscribe) error = %v", err)
	}
	if _, ok := frame.(*AttentionSubscribe); !ok {
		t.Fatalf("Decode(valid attention subscribe) = %T", frame)
	}
	for _, raw := range [][]byte{
		[]byte(`{"frame":"attention.subscribe","request_id":"attn_01H8X","session_id":"ses_1"}`),
		[]byte(`{"frame":"attention.subscribe","request_id":"attn_01H8X","last_seq":0}`),
		[]byte(`{"frame":"attention.summary","request_id":"attn_01H8X","kind":"snapshot","subscription_state":"complete","summaries":[],"events":[]}`),
	} {
		if _, err := Decode(raw); err == nil {
			t.Fatalf("Decode(%s) unexpectedly succeeded", raw)
		}
	}
}

func TestAttentionSummaryDoesNotPermitTranscriptFields(t *testing.T) {
	if _, err := Decode([]byte(`{"frame":"attention.summary","request_id":"attn_01H8X","kind":"snapshot","subscription_state":"complete","summaries":[{"session_id":"ses_1","latest_seq":1,"state":"ready","summary_version":1,"summary_state":"complete","payload":{"text":"secret"}}]}`)); err == nil {
		t.Fatal("attention summary with transcript-shaped payload was accepted")
	}
}
