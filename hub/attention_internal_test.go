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

func pointerString(value string) *string { return &value }
