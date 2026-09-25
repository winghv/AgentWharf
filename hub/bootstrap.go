package hub

import (
	"context"
	"errors"

	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

// Sample only after registration: a control update committed while admission
// was reading the initial summary must be in either bootstrap or the live buffer.
func (h *webSocketHandler) prepareTailBootstrap(ctx context.Context, peer *clientConnection, accepted *AcceptedPeer, ack *protocol.HelloAck) error {
	for _, sub := range accepted.currentSubscriptions() {
		if !sub.SkipReplay {
			continue
		}
		latest, err := h.events.LatestSeq(ctx, sub.SessionID)
		if err != nil {
			return err
		}
		if latest < sub.LastSeq {
			return errors.New("bootstrap watermark moved backwards")
		}
		for index := range accepted.Subscribed {
			if accepted.Subscribed[index].SessionID == sub.SessionID {
				accepted.Subscribed[index].LastSeq = latest
			}
		}
		for index := range ack.Sessions {
			if ack.Sessions[index].SessionID == sub.SessionID {
				ack.Sessions[index].LatestSeq = latest
				ack.Sessions[index].ReplayFrom = latest + 1
			}
		}
		peer.mu.Lock()
		peer.subscriptions[sub.SessionID].lastSeq = latest
		peer.mu.Unlock()
	}
	return nil
}

func (h *webSocketHandler) replayBootstrap(ctx context.Context, peer *clientConnection, accepted AcceptedPeer, sub protocol.Subscription) error {
	bootstrap, ok := h.events.(store.BootstrapStore)
	if !ok {
		return errors.New("bootstrap store is unavailable")
	}
	events, err := bootstrap.Bootstrap(ctx, sub.SessionID, sub.LastSeq)
	if err != nil {
		return err
	}
	allowed := make(map[string]bool)
	for _, eventType := range store.BootstrapEventTypes() {
		allowed[eventType] = true
	}
	if len(events) > len(allowed) {
		return errors.New("bootstrap state exceeds bound")
	}
	var previous int64
	for _, event := range events {
		if event.SessionID != sub.SessionID || event.Seq <= previous || event.Seq > sub.LastSeq || !allowed[event.Type] {
			return errors.New("invalid bootstrap event")
		}
		delete(allowed, event.Type)
		previous = event.Seq
		if accepted.ContentMode == protocol.ContentModeRequired {
			if err := validateRequiredReplayEvent(event, sub.SessionID); err != nil {
				return err
			}
		}
	}
	for _, event := range events {
		seq := event.Seq
		if err := peer.writeReplayEvent(ctx, protocol.Event{
			Type: event.Type, SessionID: event.SessionID, Seq: &seq,
			Time: event.Time.UnixMilli(), Payload: event.Payload,
		}); err != nil {
			return err
		}
	}
	return nil
}
