package storetest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
)

type Harness func(t *testing.T) store.EventStore

type HistoryHarness struct {
	Open        func(t *testing.T) store.HistoryStore
	Reopen      func(t *testing.T, current store.HistoryStore) store.HistoryStore
	PruneBefore func(t *testing.T, current store.HistoryStore, sessionID string, beforeSeq int64)
}

type PendingCommandHarness struct {
	Open       func(t *testing.T) store.CommandLedgerStore
	Reopen     func(t *testing.T, current store.CommandLedgerStore) store.CommandLedgerStore
	Authority  func(t *testing.T, ledger store.CommandLedgerStore) store.CommandAuthority
	Invalidate func(t *testing.T, ledger store.CommandLedgerStore, kind CommandAuthorityFailure)
}

type AttachmentHarness struct {
	Open   func(t *testing.T) store.AttachmentStore
	Reopen func(t *testing.T, current store.AttachmentStore) store.AttachmentStore
}

type CommandAuthorityFailure string

const (
	CommandAuthoritySuperseded CommandAuthorityFailure = "superseded"
	CommandAuthorityRevoked    CommandAuthorityFailure = "revoked"
	CommandAuthorityExpired    CommandAuthorityFailure = "expired"
	CommandAuthorityTerminal   CommandAuthorityFailure = "terminal"
)

func Contract(t *testing.T, newStore Harness) {
	t.Helper()

	t.Run("append replay and latest", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		st := newStore(t)
		sessionID := "ses_contract_basic"
		first, err := st.Append(ctx, sessionID, []store.PendingEvent{
			pending("session.message", 1),
			pending("session.tool_call", 2),
		})
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
		if first != 1 {
			t.Fatalf("first seq = %d, want 1", first)
		}

		latest, err := st.LatestSeq(ctx, sessionID)
		if err != nil {
			t.Fatalf("LatestSeq() error = %v", err)
		}
		if latest != 2 {
			t.Fatalf("latest = %d, want 2", latest)
		}

		got := replayAll(t, st, sessionID, 0)
		assertSeqs(t, got, []int64{1, 2})
		if got[0].SessionID != sessionID || got[0].Type != "session.message" {
			t.Fatalf("first replayed event = %+v", got[0])
		}
		if wantTime := time.UnixMilli(1764937200001); !got[0].Time.Equal(wantTime) {
			t.Fatalf("first replayed event time = %s, want %s", got[0].Time, wantTime)
		}
		var payload struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(got[1].Payload, &payload); err != nil {
			t.Fatalf("payload is invalid JSON: %v", err)
		}
		if payload.N != 2 {
			t.Fatalf("payload.n = %d, want 2", payload.N)
		}
	})

	t.Run("replay after seq", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		st := newStore(t)
		sessionID := "ses_contract_replay"
		if _, err := st.Append(ctx, sessionID, []store.PendingEvent{
			pending("session.message", 1),
			pending("session.message", 2),
			pending("session.message", 3),
		}); err != nil {
			t.Fatalf("Append() error = %v", err)
		}

		got := replayAll(t, st, sessionID, 1)
		assertSeqs(t, got, []int64{2, 3})
	})

	t.Run("append empty batch", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		st := newStore(t)
		first, err := st.Append(ctx, "ses_contract_empty", nil)
		if err != nil {
			t.Fatalf("Append(empty) error = %v", err)
		}
		if first != 0 {
			t.Fatalf("first seq = %d, want 0", first)
		}
	})

	t.Run("sessions are independent", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		st := newStore(t)
		if first, err := st.Append(ctx, "ses_contract_a", []store.PendingEvent{pending("session.message", 1)}); err != nil || first != 1 {
			t.Fatalf("Append(session a) = %d, %v; want 1, nil", first, err)
		}
		if first, err := st.Append(ctx, "ses_contract_b", []store.PendingEvent{pending("session.message", 1)}); err != nil || first != 1 {
			t.Fatalf("Append(session b) = %d, %v; want 1, nil", first, err)
		}
	})

	t.Run("concurrent append has no gaps or duplicates", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		st := newStore(t)
		sessionID := "ses_contract_concurrent"
		const writers = 16
		const batchSize = 8

		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make(chan error, writers)
		for writer := range writers {
			wg.Add(1)
			go func(writer int) {
				defer wg.Done()
				<-start
				events := make([]store.PendingEvent, 0, batchSize)
				for i := range batchSize {
					events = append(events, pending("session.message", writer*batchSize+i))
				}
				_, err := st.Append(ctx, sessionID, events)
				if err != nil {
					errs <- err
				}
			}(writer)
		}

		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("Append() concurrent error = %v", err)
		}

		got := replayAll(t, st, sessionID, 0)
		wantTotal := writers * batchSize
		if len(got) != wantTotal {
			t.Fatalf("replayed %d events, want %d", len(got), wantTotal)
		}
		for i, ev := range got {
			wantSeq := int64(i + 1)
			if ev.Seq != wantSeq {
				t.Fatalf("event[%d].Seq = %d, want %d", i, ev.Seq, wantSeq)
			}
		}
	})

	t.Run("replay callback error stops scan", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		st := newStore(t)
		sessionID := "ses_contract_callback_error"
		if _, err := st.Append(ctx, sessionID, []store.PendingEvent{
			pending("session.message", 1),
			pending("session.message", 2),
		}); err != nil {
			t.Fatalf("Append() error = %v", err)
		}

		wantErr := errors.New("stop replay")
		var calls int
		err := st.Replay(ctx, sessionID, 0, func(store.Event) error {
			calls++
			return wantErr
		})
		if !errors.Is(err, wantErr) {
			t.Fatalf("Replay() error = %v, want %v", err, wantErr)
		}
		if calls != 1 {
			t.Fatalf("callback calls = %d, want 1", calls)
		}
	})
}

func HistoryContract(t *testing.T, harness HistoryHarness) {
	t.Helper()
	if harness.Open == nil || harness.Reopen == nil || harness.PruneBefore == nil {
		t.Fatal("history contract harness must provide open, reopen, and prune callbacks")
	}

	t.Run("empty page and limit bounds", func(t *testing.T) {
		st := harness.Open(t)
		page := historyPage(t, st, "ses_history_empty", nil, 1)
		assertHistoryPage(t, page, nil, 0, nil, store.RetentionComplete)
		for _, limit := range []int{0, 101} {
			if _, err := st.History(context.Background(), "ses_history_empty", nil, limit); err == nil {
				t.Fatalf("History(limit=%d) unexpectedly succeeded", limit)
			}
		}
	})

	t.Run("exclusive cursor preserves ascending bounded order", func(t *testing.T) {
		st := harness.Open(t)
		const sessionID = "ses_history_cursor"
		appendHistoryEvents(t, st, sessionID, 5)

		page := historyPage(t, st, sessionID, nil, 2)
		assertHistoryPage(t, page, []int64{4, 5}, 5, int64Pointer(4), store.RetentionComplete)
		page = historyPage(t, st, sessionID, page.NextBeforeSeq, 2)
		assertHistoryPage(t, page, []int64{2, 3}, 5, int64Pointer(2), store.RetentionComplete)
		page = historyPage(t, st, sessionID, page.NextBeforeSeq, 2)
		assertHistoryPage(t, page, []int64{1}, 5, nil, store.RetentionComplete)
	})

	t.Run("retention gap remains explicit after prune", func(t *testing.T) {
		st := harness.Open(t)
		const sessionID = "ses_history_gap"
		appendHistoryEvents(t, st, sessionID, 5)
		harness.PruneBefore(t, st, sessionID, 4)
		page := historyPage(t, st, sessionID, nil, 100)
		assertHistoryPage(t, page, []int64{4, 5}, 5, nil, store.RetentionGap)
		beforeRetainedHistory := int64(4)
		page = historyPage(t, st, sessionID, &beforeRetainedHistory, 100)
		assertHistoryPage(t, page, nil, 5, nil, store.RetentionGap)
	})

	t.Run("concurrent append remains sequenced and pageable", func(t *testing.T) {
		st := harness.Open(t)
		const (
			sessionID = "ses_history_concurrent"
			writers   = 8
			batchSize = 4
		)
		start := make(chan struct{})
		errs := make(chan error, writers)
		var wg sync.WaitGroup
		for writer := range writers {
			wg.Add(1)
			go func(writer int) {
				defer wg.Done()
				<-start
				events := make([]store.PendingEvent, 0, batchSize)
				for index := range batchSize {
					events = append(events, pending("session.message", writer*batchSize+index))
				}
				_, err := st.Append(context.Background(), sessionID, events)
				errs <- err
			}(writer)
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent Append() error = %v", err)
			}
		}

		page := historyPage(t, st, sessionID, nil, writers*batchSize)
		want := make([]int64, writers*batchSize)
		for index := range want {
			want[index] = int64(index + 1)
		}
		assertHistoryPage(t, page, want, int64(writers*batchSize), nil, store.RetentionComplete)
	})

	t.Run("reopen retains the same historical truth", func(t *testing.T) {
		st := harness.Open(t)
		const sessionID = "ses_history_reopen"
		appendHistoryEvents(t, st, sessionID, 3)
		st = harness.Reopen(t, st)
		page := historyPage(t, st, sessionID, nil, 100)
		assertHistoryPage(t, page, []int64{1, 2, 3}, 3, nil, store.RetentionComplete)
	})
}

func PendingCommandContract(t *testing.T, harness PendingCommandHarness) {
	t.Helper()
	if harness.Open == nil || harness.Reopen == nil || harness.Authority == nil || harness.Invalidate == nil {
		t.Fatal("pending command contract harness must provide open, reopen, authority, and invalidate callbacks")
	}

	t.Run("commit deduplicates one reference-only command", func(t *testing.T) {
		ledger := harness.Open(t)
		authority := harness.Authority(t, ledger)
		request := store.PendingCommandRequest{
			CommandID: "cmd_contract_1",
			Type:      "session.send",
			ExpiresAt: time.Now().Add(10 * time.Second),
		}
		result, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, userCommandEvent(1), request)
		if err != nil {
			t.Fatalf("CommitPendingCommand() error = %v", err)
		}
		assertPendingCommand(t, result.Command, "ses_command_1", request, 1, store.PendingCommandPending)
		if result.Duplicate {
			t.Fatal("initial pending command commit was duplicate")
		}
		duplicate, err := ledger.CommitPendingCommand(context.Background(), "ses_command_1", authority, userCommandEvent(2), request)
		if err != nil {
			t.Fatalf("duplicate CommitPendingCommand() error = %v", err)
		}
		if !duplicate.Duplicate {
			t.Fatal("duplicate pending command commit was not marked duplicate")
		}
		assertPendingCommand(t, duplicate.Command, "ses_command_1", request, 1, store.PendingCommandPending)
		var events []store.Event
		if err := ledger.Replay(context.Background(), "ses_command_1", 0, func(event store.Event) error {
			events = append(events, event)
			return nil
		}); err != nil {
			t.Fatalf("Replay() error = %v", err)
		}
		if len(events) != 1 || events[0].Seq != result.Command.EventSeq || events[0].Type != "session.message" || !bytes.Contains(events[0].Payload, []byte(`"role":"user"`)) {
			t.Fatalf("atomic pending-command event reference = %+v", events)
		}
	})

	t.Run("only one claimant advances pending delivery", func(t *testing.T) {
		ledger := harness.Open(t)
		authority := harness.Authority(t, ledger)
		request := store.PendingCommandRequest{CommandID: "cmd_contract_claim", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
		if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_claim", authority, userCommandEvent(1), request); err != nil {
			t.Fatalf("CommitPendingCommand() error = %v", err)
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		claims := make(chan store.PendingCommandClaim, 8)
		errs := make(chan error, 8)
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				claim, err := ledger.ClaimPendingCommand(context.Background(), "ses_command_claim", authority, request.CommandID)
				claims <- claim
				errs <- err
			}()
		}
		close(start)
		wg.Wait()
		close(claims)
		close(errs)
		var claimed int
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent ClaimPendingCommand() error = %v", err)
			}
		}
		for claim := range claims {
			if claim.Claimed {
				claimed++
				assertPendingCommand(t, claim.Command, "ses_command_claim", request, 1, store.PendingCommandReceived)
			}
		}
		if claimed != 1 {
			t.Fatalf("successful concurrent claims = %d, want 1", claimed)
		}
		claim, err := ledger.ClaimPendingCommand(context.Background(), "ses_command_claim", authority, request.CommandID)
		if err != nil || claim.Claimed {
			t.Fatalf("post-claim ClaimPendingCommand() = %+v, %v; want not claimed", claim, err)
		}
		assertPendingCommand(t, claim.Command, "ses_command_claim", request, 1, store.PendingCommandReceived)
		claim, err = ledger.ClaimPendingCommand(context.Background(), "ses_command_claim", authority, request.CommandID)
		if err != nil || claim.Claimed {
			t.Fatalf("second ClaimPendingCommand() = %+v, %v; want not claimed", claim, err)
		}
		resolved, err := ledger.ResolvePendingCommand(context.Background(), "ses_command_claim", authority, request.CommandID, store.PendingCommandCompleted)
		if err != nil {
			t.Fatalf("ResolvePendingCommand() error = %v", err)
		}
		assertPendingCommand(t, resolved, "ses_command_claim", request, 1, store.PendingCommandCompleted)
		if _, err := ledger.ResolvePendingCommand(context.Background(), "ses_command_claim", authority, request.CommandID, store.PendingCommandPending); err == nil {
			t.Fatal("ResolvePendingCommand(pending) unexpectedly succeeded")
		}
		if _, err := ledger.ResolvePendingCommand(context.Background(), "ses_command_claim", authority, request.CommandID, store.PendingCommandOutcomeUnknown); err == nil {
			t.Fatal("ResolvePendingCommand() rewrote a terminal outcome")
		}
	})

	t.Run("authority loss rejects every mutation without new event", func(t *testing.T) {
		for _, kind := range []CommandAuthorityFailure{CommandAuthoritySuperseded, CommandAuthorityRevoked, CommandAuthorityExpired, CommandAuthorityTerminal} {
			t.Run(string(kind), func(t *testing.T) {
				ledger := harness.Open(t)
				authority := harness.Authority(t, ledger)
				harness.Invalidate(t, ledger, kind)
				request := store.PendingCommandRequest{CommandID: "cmd_contract_stale_" + string(kind), Type: "session.send", ExpiresAt: time.Now().Add(time.Second)}
				if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_stale", authority, userCommandEvent(1), request); err == nil {
					t.Fatal("stale CommitPendingCommand() unexpectedly succeeded")
				}
				latest, err := ledger.LatestSeq(context.Background(), "ses_command_stale")
				if err != nil || latest != 0 {
					t.Fatalf("stale commit latest seq = %d, %v; want 0, nil", latest, err)
				}

				ledger = harness.Open(t)
				authority = harness.Authority(t, ledger)
				request.CommandID = "cmd_contract_stale_claim_" + string(kind)
				if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_stale", authority, userCommandEvent(1), request); err != nil {
					t.Fatalf("prepare stale claim: %v", err)
				}
				harness.Invalidate(t, ledger, kind)
				if _, err := ledger.ClaimPendingCommand(context.Background(), "ses_command_stale", authority, request.CommandID); err == nil {
					t.Fatal("stale ClaimPendingCommand() unexpectedly succeeded")
				}

				ledger = harness.Open(t)
				authority = harness.Authority(t, ledger)
				request.CommandID = "cmd_contract_stale_resolve_" + string(kind)
				if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_stale", authority, userCommandEvent(1), request); err != nil {
					t.Fatalf("prepare stale resolve: %v", err)
				}
				if _, err := ledger.ClaimPendingCommand(context.Background(), "ses_command_stale", authority, request.CommandID); err != nil {
					t.Fatalf("prepare resolve claim: %v", err)
				}
				harness.Invalidate(t, ledger, kind)
				if _, err := ledger.ResolvePendingCommand(context.Background(), "ses_command_stale", authority, request.CommandID, store.PendingCommandCompleted); err == nil {
					t.Fatal("stale ResolvePendingCommand() unexpectedly succeeded")
				}
			})
		}
	})

	t.Run("expired pending command cannot be claimed", func(t *testing.T) {
		ledger := harness.Open(t)
		authority := harness.Authority(t, ledger)
		request := store.PendingCommandRequest{CommandID: "cmd_contract_claim_expired", Type: "session.send", ExpiresAt: time.Now().Add(20 * time.Millisecond)}
		if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_expired", authority, userCommandEvent(1), request); err != nil {
			t.Fatalf("prepare expiry claim: %v", err)
		}
		time.Sleep(40 * time.Millisecond)
		if _, err := ledger.ClaimPendingCommand(context.Background(), "ses_command_expired", authority, request.CommandID); err == nil {
			t.Fatal("expired ClaimPendingCommand() unexpectedly succeeded")
		}
	})

	t.Run("reopen preserves queued and terminal ledger truth", func(t *testing.T) {
		ledger := harness.Open(t)
		authority := harness.Authority(t, ledger)
		request := store.PendingCommandRequest{CommandID: "cmd_contract_reopen", Type: "session.send", ExpiresAt: time.Now().Add(10 * time.Second)}
		committed, err := ledger.CommitPendingCommand(context.Background(), "ses_command_reopen", authority, userCommandEvent(1), request)
		if err != nil || committed.Duplicate {
			t.Fatalf("CommitPendingCommand() = %+v, %v; want new command", committed, err)
		}
		ledger = harness.Reopen(t, ledger)
		authority = harness.Authority(t, ledger)
		assertPendingCommandEvent(t, ledger, "ses_command_reopen", committed.Command.EventSeq)
		duplicate, err := ledger.CommitPendingCommand(context.Background(), "ses_command_reopen", authority, userCommandEvent(2), request)
		if err != nil || !duplicate.Duplicate {
			t.Fatalf("reopened duplicate = %+v, %v; want original duplicate", duplicate, err)
		}
		assertPendingCommand(t, duplicate.Command, "ses_command_reopen", request, committed.Command.EventSeq, store.PendingCommandPending)
		assertPendingCommandEvent(t, ledger, "ses_command_reopen", committed.Command.EventSeq)
		claim, err := ledger.ClaimPendingCommand(context.Background(), "ses_command_reopen", authority, request.CommandID)
		if err != nil || !claim.Claimed {
			t.Fatalf("reopened claim = %+v, %v; want claimed", claim, err)
		}
		if _, err := ledger.ResolvePendingCommand(context.Background(), "ses_command_reopen", authority, request.CommandID, store.PendingCommandCompleted); err != nil {
			t.Fatalf("reopened resolve: %v", err)
		}
		ledger = harness.Reopen(t, ledger)
		authority = harness.Authority(t, ledger)
		claim, err = ledger.ClaimPendingCommand(context.Background(), "ses_command_reopen", authority, request.CommandID)
		if err != nil || claim.Claimed || claim.Command.Status != store.PendingCommandCompleted {
			t.Fatalf("terminal reopened claim = %+v, %v; want completed non-claim", claim, err)
		}
		if _, err := ledger.ResolvePendingCommand(context.Background(), "ses_command_reopen", authority, request.CommandID, store.PendingCommandOutcomeUnknown); err == nil {
			t.Fatal("reopened terminal outcome was rewritten")
		}
	})

	t.Run("invalid references and expiry are rejected", func(t *testing.T) {
		ledger := harness.Open(t)
		authority := harness.Authority(t, ledger)
		invalid := []struct {
			name    string
			event   store.PendingEvent
			request store.PendingCommandRequest
		}{
			{"expired", userCommandEvent(1), store.PendingCommandRequest{CommandID: "cmd_contract_expired", Type: "session.send", ExpiresAt: time.Now().Add(-time.Second)}},
			{"unbounded expiry", userCommandEvent(1), store.PendingCommandRequest{CommandID: "cmd_contract_unbounded", Type: "session.send", ExpiresAt: time.Now().Add(31 * time.Second)}},
			{"wrong type", userCommandEvent(1), store.PendingCommandRequest{CommandID: "cmd_contract_type", Type: "session.interrupt", ExpiresAt: time.Now().Add(time.Second)}},
			{"wrong event", pending("session.state", 1), store.PendingCommandRequest{CommandID: "cmd_contract_event", Type: "session.send", ExpiresAt: time.Now().Add(time.Second)}},
			{"non-user message", nonUserCommandEvent(1), store.PendingCommandRequest{CommandID: "cmd_contract_agent", Type: "session.send", ExpiresAt: time.Now().Add(time.Second)}},
		}
		for _, test := range invalid {
			t.Run(test.name, func(t *testing.T) {
				if _, err := ledger.CommitPendingCommand(context.Background(), "ses_command_invalid", authority, test.event, test.request); err == nil {
					t.Fatal("CommitPendingCommand() unexpectedly succeeded")
				}
			})
		}
	})
}

func AttachmentContract(t *testing.T, harness AttachmentHarness) {
	t.Helper()
	if harness.Open == nil || harness.Reopen == nil {
		t.Fatal("attachment contract harness must provide open and reopen callbacks")
	}

	t.Run("creates stable identity and looks up by attachment or target", func(t *testing.T) {
		ledger := harness.Open(t)
		request := attachmentCreate("attach_contract_lookup", "ses_bootstrap_lookup", "ses_target_lookup")
		commit, err := ledger.CreateAttachment(context.Background(), request)
		if err != nil || commit.Noop {
			t.Fatalf("CreateAttachment() = %+v, %v; want new attachment", commit, err)
		}
		assertAttachment(t, commit.Attachment, request.Identity, store.AttachmentJoinPending, store.AttachmentDeliveryPending, 0, nil, &request.ExpiresAt, nil)
		assertAttachmentSummary(t, commit.Summary, commit.Attachment, nil)

		byID, err := ledger.Attachment(context.Background(), request.Identity.AttachID)
		if err != nil {
			t.Fatalf("Attachment() error = %v", err)
		}
		byTarget, err := ledger.AttachmentForTarget(context.Background(), request.Identity.TargetSessionID)
		if err != nil {
			t.Fatalf("AttachmentForTarget() error = %v", err)
		}
		assertSameAttachment(t, byID, commit.Attachment)
		assertSameAttachment(t, byTarget, commit.Attachment)
		for _, mutate := range []func(*store.AttachmentIdentity){
			func(identity *store.AttachmentIdentity) { identity.BootstrapSessionID = "ses_bootstrap_rewrite" },
			func(identity *store.AttachmentIdentity) { identity.TargetSessionID = "ses_target_rewrite" },
			func(identity *store.AttachmentIdentity) { identity.TargetCredentialLineageRef = "lineage_rewrite" },
		} {
			retry := request
			mutate(&retry.Identity)
			if _, err := ledger.CreateAttachment(context.Background(), retry); err == nil {
				t.Fatal("identity-rewriting attachment retry unexpectedly succeeded")
			}
		}
		afterConflict, err := ledger.Attachment(context.Background(), request.Identity.AttachID)
		if err != nil {
			t.Fatalf("Attachment() after immutable-identity conflicts: %v", err)
		}
		assertSameAttachment(t, afterConflict, commit.Attachment)
	})

	t.Run("versioned mutations return their committed blocker summary", func(t *testing.T) {
		ledger := harness.Open(t)
		request := attachmentCreate("attach_contract_mutation", "ses_bootstrap_mutation", "ses_target_mutation")
		_, err := ledger.CreateAttachment(context.Background(), request)
		if err != nil {
			t.Fatalf("CreateAttachment() error = %v", err)
		}
		reason := "capacity"
		blockerSessionID := "ses_blocker_mutation"
		queued := store.AttachmentUpdate{
			Status:            store.AttachmentQueued,
			DeliveryState:     store.AttachmentDeliveryPending,
			QueueReason:       &reason,
			ExpiresAt:         &request.ExpiresAt,
			BlockingSessionID: &blockerSessionID,
			Blocker: &store.AttachmentBlocker{
				Kind:              store.AttachmentBlockerQueued,
				Reason:            &reason,
				ExpiresAt:         &request.ExpiresAt,
				BlockingSessionID: &blockerSessionID,
			},
		}
		mutation, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, queued)
		if err != nil {
			t.Fatalf("queued UpdateAttachment() error = %v", err)
		}
		assertAttachment(t, mutation.Attachment, request.Identity, store.AttachmentQueued, store.AttachmentDeliveryPending, 1, &reason, &request.ExpiresAt, &blockerSessionID)
		assertAttachmentSummary(t, mutation.Summary, mutation.Attachment, queued.Blocker)
		if _, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, queued); err == nil {
			t.Fatal("stale UpdateAttachment() unexpectedly succeeded")
		}

		started := store.AttachmentUpdate{Status: store.AttachmentStartReceived, DeliveryState: store.AttachmentDeliveryReceived}
		mutation, err = ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 1, started)
		if err != nil {
			t.Fatalf("start-received UpdateAttachment() error = %v", err)
		}
		assertAttachment(t, mutation.Attachment, request.Identity, store.AttachmentStartReceived, store.AttachmentDeliveryReceived, 2, nil, nil, nil)
		assertAttachmentSummary(t, mutation.Summary, mutation.Attachment, nil)
	})
	t.Run("reconnect, canceled, and unknown outcomes remain bounded ledger truth", func(t *testing.T) {
		ledger := harness.Open(t)
		request := attachmentCreate("attach_contract_reconnect", "ses_bootstrap_reconnect", "ses_target_reconnect")
		if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
			t.Fatalf("CreateAttachment() error = %v", err)
		}
		reauthorize := store.AttachmentUpdate{
			Status:        store.AttachmentReauthorizationRequired,
			DeliveryState: store.AttachmentDeliveryPending,
			Blocker:       &store.AttachmentBlocker{Kind: store.AttachmentBlockerReauthorizationRequired},
		}
		mutation, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, reauthorize)
		if err != nil {
			t.Fatalf("reauthorization UpdateAttachment() error = %v", err)
		}
		assertAttachmentSummary(t, mutation.Summary, mutation.Attachment, reauthorize.Blocker)
		cancel := store.AttachmentUpdate{
			Status:        store.AttachmentCanceled,
			DeliveryState: store.AttachmentDeliveryPending,
			Blocker:       &store.AttachmentBlocker{Kind: store.AttachmentBlockerNewRunRequired},
		}
		mutation, err = ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 1, cancel)
		if err != nil {
			t.Fatalf("canceled UpdateAttachment() error = %v", err)
		}
		if mutation.Attachment.CanceledAt == nil {
			t.Fatal("canceled attachment omitted Store-clock canceled_at")
		}
		assertAttachmentSummary(t, mutation.Summary, mutation.Attachment, cancel.Blocker)
		if _, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 2, reauthorize); err == nil {
			t.Fatal("canceled attachment was resurrected")
		}
	})
	t.Run("healthy target attach retry is a no-op", func(t *testing.T) {
		ledger := harness.Open(t)
		request := attachmentCreate("attach_contract_healthy", "ses_bootstrap_healthy", "ses_target_healthy")
		if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
			t.Fatalf("CreateAttachment() error = %v", err)
		}
		started := store.AttachmentUpdate{Status: store.AttachmentStartReceived, DeliveryState: store.AttachmentDeliveryReceived}
		mutation, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, started)
		if err != nil {
			t.Fatalf("start-received UpdateAttachment() error = %v", err)
		}
		duplicate, err := ledger.CreateAttachment(context.Background(), request)
		if err != nil || !duplicate.Noop {
			t.Fatalf("healthy CreateAttachment() = %+v, %v; want no-op", duplicate, err)
		}
		assertSameAttachment(t, duplicate.Attachment, mutation.Attachment)
	})
	t.Run("outcome unknown is bounded and terminal status cannot rebind", func(t *testing.T) {
		ledger := harness.Open(t)
		request := attachmentCreate("attach_contract_outcome", "ses_bootstrap_outcome", "ses_target_outcome")
		if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
			t.Fatalf("CreateAttachment() error = %v", err)
		}
		operation := "start"
		unknown := store.AttachmentUpdate{
			Status:        store.AttachmentStartReceived,
			DeliveryState: store.AttachmentDeliveryOutcomeUnknown,
			Blocker:       &store.AttachmentBlocker{Kind: store.AttachmentBlockerOutcomeUnknown, Operation: &operation},
		}
		mutation, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, unknown)
		if err != nil {
			t.Fatalf("outcome unknown UpdateAttachment() error = %v", err)
		}
		assertAttachmentSummary(t, mutation.Summary, mutation.Attachment, unknown.Blocker)
		other := attachmentCreate("attach_contract_rebind", "ses_bootstrap_replacement", request.Identity.TargetSessionID)
		if _, err := ledger.CreateAttachment(context.Background(), other); err == nil {
			t.Fatal("replacement bootstrap rebound target attachment")
		}
		stored, err := ledger.AttachmentForTarget(context.Background(), request.Identity.TargetSessionID)
		if err != nil {
			t.Fatalf("AttachmentForTarget() after rebind rejection: %v", err)
		}
		if stored.Identity.BootstrapSessionID != request.Identity.BootstrapSessionID {
			t.Fatalf("bootstrap rebinding = %q, want %q", stored.Identity.BootstrapSessionID, request.Identity.BootstrapSessionID)
		}
	})
	t.Run("expiry is bounded and cannot be extended", func(t *testing.T) {
		ledger := harness.Open(t)
		expired := attachmentCreate("attach_contract_expired", "ses_bootstrap_expired", "ses_target_expired")
		expired.ExpiresAt = time.Now().Add(-time.Second)
		if _, err := ledger.CreateAttachment(context.Background(), expired); err == nil {
			t.Fatal("expired attachment create unexpectedly succeeded")
		}
		long := attachmentCreate("attach_contract_long", "ses_bootstrap_long", "ses_target_long")
		long.ExpiresAt = time.Now().Add(31 * time.Second)
		if _, err := ledger.CreateAttachment(context.Background(), long); err == nil {
			t.Fatal("unbounded attachment create unexpectedly succeeded")
		}
		request := attachmentCreate("attach_contract_expiry", "ses_bootstrap_expiry", "ses_target_expiry")
		if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
			t.Fatalf("CreateAttachment() error = %v", err)
		}
		reason := "capacity"
		blocker := "ses_blocker_expiry"
		extended := request.ExpiresAt.Add(time.Second)
		update := store.AttachmentUpdate{
			Status:            store.AttachmentQueued,
			DeliveryState:     store.AttachmentDeliveryPending,
			QueueReason:       &reason,
			ExpiresAt:         &extended,
			BlockingSessionID: &blocker,
			Blocker:           &store.AttachmentBlocker{Kind: store.AttachmentBlockerQueued, Reason: &reason, ExpiresAt: &extended, BlockingSessionID: &blocker},
		}
		if _, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, update); err == nil {
			t.Fatal("attachment expiry extension unexpectedly succeeded")
		}
		if _, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, store.AttachmentUpdate{Status: store.AttachmentStatus("starting"), DeliveryState: store.AttachmentDeliveryPending}); err == nil {
			t.Fatal("Hub-synthesized starting attachment state unexpectedly succeeded")
		}
	})
	t.Run("concurrent exact-version updates allow one winner", func(t *testing.T) {
		ledger := harness.Open(t)
		request := attachmentCreate("attach_contract_concurrent", "ses_bootstrap_concurrent", "ses_target_concurrent")
		if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
			t.Fatalf("CreateAttachment() error = %v", err)
		}
		update := store.AttachmentUpdate{
			Status:        store.AttachmentReauthorizationRequired,
			DeliveryState: store.AttachmentDeliveryPending,
			Blocker:       &store.AttachmentBlocker{Kind: store.AttachmentBlockerReauthorizationRequired},
		}
		start := make(chan struct{})
		results := make(chan error, 8)
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, update)
				results <- err
			}()
		}
		close(start)
		wg.Wait()
		close(results)
		var successes int
		for err := range results {
			if err == nil {
				successes++
			}
		}
		if successes != 1 {
			t.Fatalf("concurrent attachment updates = %d successes, want 1", successes)
		}
	})
	t.Run("reopen preserves versioned attachment truth", func(t *testing.T) {
		ledger := harness.Open(t)
		request := attachmentCreate("attach_contract_reopen", "ses_bootstrap_reopen", "ses_target_reopen")
		if _, err := ledger.CreateAttachment(context.Background(), request); err != nil {
			t.Fatalf("CreateAttachment() error = %v", err)
		}
		reauthorize := store.AttachmentUpdate{
			Status:        store.AttachmentReauthorizationRequired,
			DeliveryState: store.AttachmentDeliveryPending,
			Blocker:       &store.AttachmentBlocker{Kind: store.AttachmentBlockerReauthorizationRequired},
		}
		if _, err := ledger.UpdateAttachment(context.Background(), request.Identity.AttachID, 0, reauthorize); err != nil {
			t.Fatalf("UpdateAttachment() before reopen: %v", err)
		}
		ledger = harness.Reopen(t, ledger)
		stored, err := ledger.Attachment(context.Background(), request.Identity.AttachID)
		if err != nil {
			t.Fatalf("reopened Attachment() error = %v", err)
		}
		assertAttachment(t, stored, request.Identity, store.AttachmentReauthorizationRequired, store.AttachmentDeliveryPending, 1, nil, nil, nil)
	})
}
func attachmentCreate(attachID, bootstrapSessionID, targetSessionID string) store.AttachmentCreate {
	return store.AttachmentCreate{
		Identity: store.AttachmentIdentity{
			AttachID:                   attachID,
			BootstrapSessionID:         bootstrapSessionID,
			TargetSessionID:            targetSessionID,
			TargetCredentialLineageRef: "lineage_" + attachID,
		},
		ExpiresAt: time.Now().Add(20 * time.Second),
	}
}

func assertAttachment(t *testing.T, attachment store.Attachment, identity store.AttachmentIdentity, status store.AttachmentStatus, deliveryState store.AttachmentDeliveryState, version int64, reason *string, expiresAt *time.Time, blockingSessionID *string) {
	t.Helper()
	if attachment.Identity != identity || attachment.Status != status || attachment.DeliveryState != deliveryState || attachment.DeliveryVersion != version || !sameString(attachment.QueueReason, reason) || !sameTime(attachment.ExpiresAt, expiresAt) || !sameString(attachment.BlockingSessionID, blockingSessionID) {
		t.Fatalf("attachment = %+v, want identity=%+v status=%s delivery=%s version=%d reason=%v expiry=%v blocker=%v", attachment, identity, status, deliveryState, version, reason, expiresAt, blockingSessionID)
	}
}

func assertAttachmentSummary(t *testing.T, summary store.AttachmentSummary, attachment store.Attachment, blocker *store.AttachmentBlocker) {
	t.Helper()
	if summary.AttachID != attachment.Identity.AttachID || summary.TargetSessionID != attachment.Identity.TargetSessionID || summary.DeliveryVersion != attachment.DeliveryVersion || !sameTime(summary.ExpiresAt, attachment.ExpiresAt) || !sameBlocker(summary.Blocker, blocker) {
		t.Fatalf("attachment summary = %+v, want attachment=%+v blocker=%+v", summary, attachment, blocker)
	}
}

func assertSameAttachment(t *testing.T, got, want store.Attachment) {
	t.Helper()
	if got.Identity != want.Identity || got.Status != want.Status || got.DeliveryState != want.DeliveryState || got.DeliveryVersion != want.DeliveryVersion || !sameString(got.QueueReason, want.QueueReason) || !sameTime(got.ExpiresAt, want.ExpiresAt) || !sameTime(got.CanceledAt, want.CanceledAt) || !sameString(got.BlockingSessionID, want.BlockingSessionID) {
		t.Fatalf("attachment = %+v, want %+v", got, want)
	}
}

func sameBlocker(left, right *store.AttachmentBlocker) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && left.Kind == right.Kind && sameString(left.Reason, right.Reason) && sameTime(left.ExpiresAt, right.ExpiresAt) && sameString(left.BlockingSessionID, right.BlockingSessionID) && sameString(left.Operation, right.Operation))
}

func sameString(left, right *string) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}

func sameTime(left, right *time.Time) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && left.Equal(*right))
}

func userCommandEvent(n int) store.PendingEvent {
	event := pending("session.message", n)
	event.Payload = json.RawMessage(`{"role":"user"}`)
	return event
}

func nonUserCommandEvent(n int) store.PendingEvent {
	event := pending("session.message", n)
	event.Payload = json.RawMessage(`{"role":"agent"}`)
	return event
}

func assertPendingCommand(t *testing.T, command store.PendingCommand, sessionID string, request store.PendingCommandRequest, eventSeq int64, status store.PendingCommandStatus) {
	t.Helper()
	if command.SessionID != sessionID || command.CommandID != request.CommandID || command.Type != request.Type || command.EventSeq != eventSeq || command.Status != status || !command.ExpiresAt.Equal(request.ExpiresAt) {
		t.Fatalf("pending command = %+v, want session=%s command=%s type=%s event=%d status=%s expiry=%s", command, sessionID, request.CommandID, request.Type, eventSeq, status, request.ExpiresAt)
	}
}

func assertPendingCommandEvent(t *testing.T, ledger store.EventStore, sessionID string, eventSeq int64) {
	t.Helper()
	var events []store.Event
	if err := ledger.Replay(context.Background(), sessionID, 0, func(event store.Event) error {
		events = append(events, event)
		return nil
	}); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if len(events) != 1 || events[0].Seq != eventSeq || events[0].Type != "session.message" || !bytes.Contains(events[0].Payload, []byte(`"role":"user"`)) {
		t.Fatalf("reopened pending-command event reference = %+v", events)
	}
}

func appendHistoryEvents(t *testing.T, st store.EventStore, sessionID string, count int) {
	t.Helper()
	events := make([]store.PendingEvent, 0, count)
	for index := range count {
		events = append(events, pending("session.message", index))
	}
	if _, err := st.Append(context.Background(), sessionID, events); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
}

func historyPage(t *testing.T, st store.HistoryStore, sessionID string, beforeSeq *int64, limit int) store.HistoryPage {
	t.Helper()
	page, err := st.History(context.Background(), sessionID, beforeSeq, limit)
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	return page
}

func assertHistoryPage(t *testing.T, page store.HistoryPage, wantSeqs []int64, latestSeq int64, nextBeforeSeq *int64, retentionState string) {
	t.Helper()
	if page.LatestSeq != latestSeq {
		t.Fatalf("page latest seq = %d, want %d", page.LatestSeq, latestSeq)
	}
	if page.RetentionState != retentionState {
		t.Fatalf("page retention state = %q, want %q", page.RetentionState, retentionState)
	}
	if (page.NextBeforeSeq == nil) != (nextBeforeSeq == nil) || (page.NextBeforeSeq != nil && *page.NextBeforeSeq != *nextBeforeSeq) {
		t.Fatalf("page next before seq = %v, want %v", page.NextBeforeSeq, nextBeforeSeq)
	}
	if len(page.Events) != len(wantSeqs) {
		t.Fatalf("page event count = %d, want %d", len(page.Events), len(wantSeqs))
	}
	for index, event := range page.Events {
		if event.Seq != wantSeqs[index] {
			t.Fatalf("page event[%d] seq = %d, want %d", index, event.Seq, wantSeqs[index])
		}
		if index > 0 && page.Events[index-1].Seq >= event.Seq {
			t.Fatalf("page events are not strictly ascending: %d then %d", page.Events[index-1].Seq, event.Seq)
		}
	}
}

func int64Pointer(value int64) *int64 {
	return &value
}

func pending(eventType string, n int) store.PendingEvent {
	payload := json.RawMessage(fmt.Sprintf(`{"n":%d}`, n))
	return store.PendingEvent{
		Type:    eventType,
		Time:    time.UnixMilli(1764937200000 + int64(n)),
		Payload: payload,
	}
}

func replayAll(t *testing.T, st store.EventStore, sessionID string, afterSeq int64) []store.Event {
	t.Helper()

	var got []store.Event
	if err := st.Replay(context.Background(), sessionID, afterSeq, func(ev store.Event) error {
		got = append(got, ev)
		return nil
	}); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	return got
}

func assertSeqs(t *testing.T, got []store.Event, want []int64) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("got %d events, want %d", len(got), len(want))
	}
	for i, ev := range got {
		if ev.Seq != want[i] {
			t.Fatalf("event[%d].Seq = %d, want %d", i, ev.Seq, want[i])
		}
	}
}
