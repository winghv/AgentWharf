package hub

import (
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
)

func TestAttentionSummaryFrameRedactsUngrantableBlockerReference(t *testing.T) {
	blocked := "ses_blocked"
	expires := time.Now().UTC().Add(time.Minute)
	frame := attentionSummaryFrame("attn_1", "snapshot", []store.SessionAttentionSummary{{
		SessionID: "ses_visible", State: "ready", StateOfProjection: "complete", SummaryVersion: 2,
		Blocker: &store.AttentionBlocker{Kind: "queued", Reason: pointerString("capacity"), ExpiresAt: &expires, BlockingSessionID: &blocked},
	}}, []string{"ses_visible"})
	if len(frame.Summaries) != 1 || frame.Summaries[0].Blocker == nil || frame.Summaries[0].Blocker.BlockingSessionID != nil {
		t.Fatalf("ungranted blocker reference = %+v", frame)
	}
	frame = attentionSummaryFrame("attn_1", "snapshot", []store.SessionAttentionSummary{{
		SessionID: "ses_visible", State: "ready", StateOfProjection: "complete", SummaryVersion: 2,
		Blocker: &store.AttentionBlocker{Kind: "queued", Reason: pointerString("capacity"), ExpiresAt: &expires, BlockingSessionID: &blocked},
	}}, []string{"ses_visible", "ses_blocked"})
	if len(frame.Summaries) != 2 || frame.Summaries[0].SessionID != "ses_blocked" || frame.Summaries[1].Blocker == nil || frame.Summaries[1].Blocker.BlockingSessionID == nil || *frame.Summaries[1].Blocker.BlockingSessionID != blocked {
		t.Fatalf("granted blocker reference = %+v", frame)
	}
}

func TestOldAttentionExpiryCannotRemoveRenewedSubscription(t *testing.T) {
	peer := &clientConnection{}
	handler := &webSocketHandler{
		attentionSubscribers:   make(map[string]map[*clientConnection]struct{}),
		attentionSubscriptions: make(map[*clientConnection]attentionSubscription),
	}
	handler.attentionSubscriptions[peer] = attentionSubscription{generation: 2, sessionIDs: []string{"ses_renewed"}}
	handler.attentionSubscribers["ses_renewed"] = map[*clientConnection]struct{}{peer: {}}

	// This is the exact callback path after the old timer was already runnable.
	handler.expireAttentionSubscription(peer, 1)
	current, found := handler.attentionSubscriptions[peer]
	if !found || current.generation != 2 {
		t.Fatalf("old expiry removed renewed subscription: %+v found=%v", current, found)
	}
	if _, found := handler.attentionSubscribers["ses_renewed"][peer]; !found {
		t.Fatal("old expiry removed renewed live membership")
	}
}

func pointerString(value string) *string { return &value }
