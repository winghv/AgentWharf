package hub

import (
	"context"
	"testing"

	"github.com/winghv/agentwharf/protocol"
)

func TestClientHelloReplayCoalescesEphemeralEvents(t *testing.T) {
	client := newClientConnection(nil, protocol.ProtocolVersion, []protocol.Subscription{{SessionID: "ses_1"}}, true, nil)
	for index := 0; index <= maxReplayBufferedEvents; index++ {
		if err := client.sendLiveEvent(context.Background(), protocol.Event{
			Type:      "agent.activity",
			SessionID: "ses_1",
			Time:      int64(index),
		}); err != nil {
			t.Fatalf("buffer ephemeral event %d: %v", index, err)
		}
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	state := client.subscriptions["ses_1"]
	if len(state.buffered) != 1 || state.buffered[0].Time != maxReplayBufferedEvents {
		t.Fatalf("coalesced replay buffer = %+v", state.buffered)
	}
}
