package hub

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
)

func TestCoreCoverageHelpersPreserveBoundaries(t *testing.T) {
	seq := int64(7)
	event := protocol.Event{Type: "session.message", SessionID: "ses_load", Time: 11, Payload: []byte(`{"content":"opaque"}`), Seq: &seq}
	clone := cloneProtocolEvent(event)
	clone.Payload[0] = '['
	if clone.Seq == event.Seq || *clone.Seq != seq || string(event.Payload) != `{"content":"opaque"}` {
		t.Fatalf("clone mutated source: original=%+v clone=%+v", event, clone)
	}

	if commandID(nil) != "" || commandID(&protocol.Command{CommandID: "cmd_1"}) != "cmd_1" {
		t.Fatal("commandID did not preserve nil and opaque ids")
	}
	if !subscribesTo([]protocol.Subscription{{SessionID: "ses_load"}}, "ses_load") || subscribesTo(nil, "ses_missing") {
		t.Fatal("subscription membership mismatch")
	}
	if !hasLiteralSessionControl(auth.Principal{Scopes: []auth.Scope{{Kind: auth.KindSession, ID: "ses_load", Access: auth.AccessControl}}}, "ses_load") {
		t.Fatal("session control scope was rejected")
	}
	if commandNeedsPersistence(protocol.CommandSessionSend) != commandCanBuffer(protocol.CommandSessionSend) ||
		commandNeedsPersistence(protocol.CommandSessionAttach) || commandCanBuffer(protocol.CommandSessionAttach) {
		t.Fatal("command persistence/buffering policy mismatch")
	}

	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{name: "hello", err: ErrInvalidHello, want: "invalid_hello"},
		{name: "token", err: auth.ErrInvalidToken, want: "unauthorized"},
		{name: "internal", err: errors.New("opaque"), want: "internal_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := protocolErrorCode(tc.err); got != tc.want {
				t.Fatalf("protocolErrorCode(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

func TestCoreCoverageCommandValidationAndSummary(t *testing.T) {
	if err := validateClientCommand(&protocol.Command{CommandID: "cmd_1", Type: protocol.CommandSessionSend, SessionID: "ses_load"}); err != nil {
		t.Fatalf("valid command rejected: %v", err)
	}
	if err := validateClientCommand(&protocol.Command{CommandID: "cmd_1", Type: protocol.CommandType("unknown"), SessionID: "ses_load"}); err == nil {
		t.Fatal("unknown command accepted")
	}
	if err := validateClientCommand(&protocol.Command{CommandID: "cmd_1", Type: protocol.CommandSettingsChange, SessionID: "ses_load", Payload: []byte("not-json")}); err == nil {
		t.Fatal("invalid settings payload accepted")
	}

	summary, err := (&Handshake{events: noopEventStore{}}).summary(context.Background(), "ses_load", 2, "ready", "provider")
	if err != nil || summary.LatestSeq != 0 || summary.ReplayFrom != 3 {
		t.Fatalf("summary = %+v, err=%v", summary, err)
	}
}

func TestCoreCoveragePublicationCancellationAndPendingExpiry(t *testing.T) {
	var gates sessionPublicationGates
	release, err := gates.acquire(context.Background(), "ses_load")
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := gates.acquire(waitCtx, "ses_load"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled acquire = %v", err)
	}
	release()

	handler := &webSocketHandler{
		pendingTargetJoins:        map[string]*pendingTargetJoin{},
		pendingTargetJoinByAttach: map[string]*pendingTargetJoin{},
	}
	entry := &pendingTargetJoin{attachID: "attach_load", finished: make(chan struct{})}
	handler.pendingTargetJoins["nonce"] = entry
	handler.pendingTargetJoinByAttach[entry.attachID] = entry
	handler.expirePendingTargetJoin(entry)
	select {
	case <-entry.finished:
	case <-time.After(time.Second):
		t.Fatal("pending target join did not finish")
	}
	if len(handler.pendingTargetJoins) != 0 || len(handler.pendingTargetJoinByAttach) != 0 {
		t.Fatalf("expired pending join retained: %+v %+v", handler.pendingTargetJoins, handler.pendingTargetJoinByAttach)
	}
}

func TestCoreCoverageBatcherRejectsAfterClose(t *testing.T) {
	batcher := newAdapterEventBatcher(adapterEventBatcherConfig{
		Store:     newBatcherTestStore(),
		SessionID: "ses_load",
		MaxEvents: 1,
	})
	batcher.cancel()
	<-batcher.done
	batcher.queue <- pendingAdapterEvent{}
	err := batcher.Enqueue(context.Background(), protocol.Event{Type: "session.message", SessionID: "ses_load"}, storePendingEventForCoverage())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("enqueue after close = %v, want context canceled", err)
	}
}

func storePendingEventForCoverage() store.PendingEvent {
	return store.PendingEvent{Type: "session.message", Time: time.Unix(1, 0), Payload: []byte(`{"content":"opaque"}`)}
}

func TestCoreCoveragePurePolicies(t *testing.T) {
	index := 1
	reason := "missing"
	outcome := protocol.FileReferenceOutcomePayload{MessageID: "msg", CommandID: "cmd", Outcome: "rejected", ReferenceIndex: &index, Reason: &reason}
	if !sameFileReferenceOutcome(outcome, outcome) || sameFileReferenceOutcome(outcome, protocol.FileReferenceOutcomePayload{MessageID: "other"}) {
		t.Fatal("file-reference outcome equality policy mismatch")
	}
	completion := "ready"
	runOutcome := protocol.RunControlOutcomePayload{CommandID: "cmd", Operation: "stop", Outcome: "completed", CompletionState: &completion, ReasonCode: &reason}
	if !sameRunControlOutcome(runOutcome, runOutcome) || sameRunControlOutcome(runOutcome, protocol.RunControlOutcomePayload{CommandID: "other"}) {
		t.Fatal("run-control outcome equality policy mismatch")
	}

	if !isRunControlCommand(protocol.CommandSessionInterrupt) || !isRunControlCommand(protocol.CommandSessionStop) || isRunControlCommand(protocol.CommandSessionSend) {
		t.Fatal("run-control command classification mismatch")
	}
	if runControlOperation(protocol.CommandSessionStop) != store.RunControlStop || runControlOperation(protocol.CommandSessionInterrupt) != store.RunControlInterrupt {
		t.Fatal("run-control operation mapping mismatch")
	}

	fileBytes, imageBytes := int64(16), int64(32)
	mediaType := "image/png"
	capability := protocol.FileReferenceCapabilityPayload{
		MaxReferences: 2, MaxTotalBytes: 64,
		File:  protocol.FileReferenceDispositionCapability{Mode: "allowed", MaxBytes: &fileBytes},
		Image: protocol.FileReferenceImageCapability{Mode: "allowed", MaxBytes: &imageBytes, MediaTypes: []string{mediaType}},
	}
	validRequest := protocol.FileReferenceSendPayload{ReferenceCount: 2, References: []protocol.FileReferencePart{{Disposition: "file", Bytes: 16}, {Disposition: "image", Bytes: 32, MediaType: &mediaType}}}
	if !fileReferenceRequestAllowed(validRequest, capability) {
		t.Fatal("valid file-reference request rejected")
	}
	tooLarge := validRequest
	tooLarge.References = append([]protocol.FileReferencePart(nil), validRequest.References...)
	tooLarge.References[0].Bytes = 17
	if fileReferenceRequestAllowed(tooLarge, capability) {
		t.Fatal("oversized file-reference request accepted")
	}

	command := &protocol.Command{CommandID: "cmd", Type: protocol.CommandSessionSend, SessionID: "ses", Payload: []byte(`{"text":"hello"}`)}
	eventType, payload, err := commandEventPayload(command)
	if err != nil || eventType != "session.message" {
		t.Fatalf("command event payload = %q %s, err=%v", eventType, payload, err)
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil || string(fields["message_id"]) != `"cmd"` || string(fields["role"]) != `"user"` {
		t.Fatalf("session message payload missing Hub fields: %s", payload)
	}
	permission := &protocol.Command{CommandID: "cmd", Type: protocol.CommandPermissionRespond, SessionID: "ses", Payload: []byte(`{"request_id":"req"}`)}
	if eventType, _, err := commandEventPayload(permission); err != nil || eventType != "permission.decision" {
		t.Fatalf("permission event payload = %q, err=%v", eventType, err)
	}
	if got := permissionDecisionKey(permission); got != "ses:req" || permissionDecisionKey(command) != "" {
		t.Fatalf("permission decision key = %q", got)
	}

	forwarded, err := fileReferenceForwardPayload(&protocol.Command{CommandID: "cmd", Payload: []byte(`{"references":[]}`)})
	if err != nil || len(forwarded) == 0 {
		t.Fatalf("file-reference forwarding failed: %v", err)
	}
	if _, err := fileReferenceForwardPayload(&protocol.Command{CommandID: "cmd", Payload: []byte(`{"message_id":"client"}`)}); err == nil {
		t.Fatal("client-supplied message id accepted")
	}

	cloned := cloneCommand(command)
	if cloned.CommandID != command.CommandID || &cloned.Payload[0] == &command.Payload[0] {
		t.Fatal("cloneCommand did not copy opaque payload")
	}
	if normalizedEventTime(0).IsZero() || !normalizedEventTime(11).Equal(time.UnixMilli(11)) {
		t.Fatal("event time normalization mismatch")
	}
	if !isEphemeralEvent("presence") || isEphemeralEvent("session.message") {
		t.Fatal("ephemeral event classification mismatch")
	}
	variants := copyEphemeralEventVariants(map[string]map[int]string{"presence": {2: "presence.v2"}, "": {1: "bad"}})
	if len(variants) != 1 || variants["presence"][2] != "presence.v2" {
		t.Fatalf("ephemeral variants copy = %+v", variants)
	}

	when := time.Unix(11, 0)
	left := auth.AttentionGrant{Subject: "sub", SessionIDs: []string{"ses"}, MaxSessions: 1, ExpiresAt: when}
	if !sameSessionIDs(left.SessionIDs, []string{"ses"}) || sameSessionIDs(left.SessionIDs, []string{"other"}) || !sameAttentionGrant(left, left) {
		t.Fatal("attention equality policy mismatch")
	}
	copy := copyAttentionGrant(left)
	copy.SessionIDs[0] = "changed"
	if left.SessionIDs[0] != "ses" {
		t.Fatal("attention grant copy aliased session ids")
	}
}

func TestCoreCoverageStateAndLineagePolicies(t *testing.T) {
	handler := &webSocketHandler{
		pendingCommands:  make(map[string][]queuedCommand),
		acceptedCommands: make(map[string]struct{}),
		decisions:        make(map[string]struct{}),
	}
	handler.markCommandAcceptedLocked("cmd")
	handler.markCommandAcceptedLocked("cmd")
	handler.markDecisionAcceptedLocked("decision")
	handler.markDecisionAcceptedLocked("decision")
	if len(handler.acceptedCommandOrder) != 1 || len(handler.decisionOrder) != 1 {
		t.Fatal("duplicate command or decision was accepted twice")
	}
	handler.pendingCommands["ses"] = []queuedCommand{
		{expiresAt: time.Now().Add(-time.Second)},
		{expiresAt: time.Now().Add(time.Minute)},
	}
	filtered := handler.prunePendingCommandsLocked("ses", time.Now())
	if len(filtered) != 1 || len(handler.pendingCommands["ses"]) != 1 {
		t.Fatalf("pending command pruning = %+v", filtered)
	}

	for _, tc := range []struct {
		lineage auth.SessionCredentialLineage
		valid   bool
	}{
		{auth.SessionCredentialLineage{Kind: auth.SessionCredentialBootstrapInitial}, true},
		{auth.SessionCredentialLineage{Kind: auth.SessionCredentialTargetAttach, AttachID: "attach"}, true},
		{auth.SessionCredentialLineage{Kind: auth.SessionCredentialTargetRotation, AttachID: "attach"}, true},
		{auth.SessionCredentialLineage{Kind: auth.SessionCredentialTargetAttach}, false},
		{auth.SessionCredentialLineage{Kind: auth.SessionCredentialTargetRotation}, false},
	} {
		if _, got := rotationLineage(tc.lineage); got != tc.valid {
			t.Fatalf("rotationLineage(%+v) = %v, want %v", tc.lineage, got, tc.valid)
		}
	}
	for _, tc := range []struct {
		lineage auth.SessionCredentialLineage
		valid   bool
	}{
		{auth.SessionCredentialLineage{Kind: auth.SessionCredentialBootstrapInitial}, true},
		{auth.SessionCredentialLineage{Kind: auth.SessionCredentialTargetAttach, AttachID: "a", JTI: "j"}, true},
		{auth.SessionCredentialLineage{Kind: auth.SessionCredentialTargetRotation, AttachID: "a"}, true},
		{auth.SessionCredentialLineage{Kind: auth.SessionCredentialTargetRotation, AttachID: "a", JTI: "j"}, false},
		{auth.SessionCredentialLineage{Kind: auth.SessionCredentialTargetAttach, AttachID: "a"}, false},
	} {
		if got := validAdapterCredentialLineage(tc.lineage); got != tc.valid {
			t.Fatalf("validAdapterCredentialLineage(%+v) = %v, want %v", tc.lineage, got, tc.valid)
		}
	}

	variants := copyEphemeralEventVariants(map[string]map[int]string{"presence": {protocol.ProtocolVersion: "presence.v2", 0: "old"}})
	types := publisherEphemeralTypes(variants)
	if _, ok := types["presence"]; !ok || !handlerWithVariants(variants).isPublisherEphemeralType("presence.v2") || handlerWithVariants(variants).selectEphemeralVariant(protocol.ProtocolVersion, "presence") != "presence.v2" {
		t.Fatalf("ephemeral publisher policy = %+v", types)
	}
	if !validOpaqueEventType("opaque") || validOpaqueEventType("") || validOpaqueEventType(string(make([]byte, 256))) {
		t.Fatal("opaque event type bounds mismatch")
	}
}

func handlerWithVariants(variants map[string]map[int]string) *webSocketHandler {
	return &webSocketHandler{ephemeralEventVariants: variants, publisherEphemeralTypes: publisherEphemeralTypes(variants)}
}

type pendingRunControlStore struct {
	store.RunControlStore
	pending []store.RunControlReservation
}

func (s pendingRunControlStore) PendingRunControls(context.Context, string) ([]store.RunControlReservation, error) {
	return s.pending, nil
}

type pendingSettingsStore struct {
	store.SettingsCommandStore
	pending []store.SettingsCommand
}

func (s pendingSettingsStore) PendingSettingsCommands(context.Context, string) ([]store.SettingsCommand, error) {
	return s.pending, nil
}

func TestCoreCoverageFailureReasonPolicies(t *testing.T) {
	ctx := context.Background()
	runStore := pendingRunControlStore{pending: []store.RunControlReservation{{CommandID: "cmd"}}}
	for _, tc := range []struct {
		err   error
		type_ protocol.CommandType
		want  string
	}{
		{errors.New("ID is reused"), protocol.CommandSessionStop, "cmd_id_reused"},
		{errors.New("unsupported"), protocol.CommandSessionStop, "stop_unsupported"},
		{errors.New("unsupported"), protocol.CommandSessionInterrupt, "interrupt_unsupported"},
		{errors.New("writer is fenced"), protocol.CommandSessionStop, "run_control_unavailable"},
		{errors.New("state invalid"), protocol.CommandSessionStop, "run_control_invalid_state"},
		{errors.New("other"), protocol.CommandSessionStop, "run_control_pending"},
		{nil, protocol.CommandSessionStop, "internal_error"},
	} {
		if got := runControlReserveFailureReason(ctx, runStore, "ses", tc.type_, tc.err); got != tc.want {
			t.Fatalf("runControlReserveFailureReason(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
	settingsStore := pendingSettingsStore{pending: []store.SettingsCommand{{CommandID: "cmd"}}}
	for _, tc := range []struct {
		err  error
		want string
	}{
		{errors.New("ID is reused"), "cmd_id_reused"},
		{errors.New("capability is stale"), "stale_capability"},
		{errors.New("other"), "settings_change_pending"},
		{nil, "internal_error"},
	} {
		if got := settingsReserveFailureReason(ctx, settingsStore, "ses", tc.err); got != tc.want {
			t.Fatalf("settingsReserveFailureReason(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
}

func TestCoreCoverageSettingsOutcomePolicy(t *testing.T) {
	command := store.SettingsCommand{RequestedModelID: stringPointer("model"), RequestedPermissionModeID: stringPointer("ask"), ReservedCapability: store.SettingsCapability{EffectiveModelID: "old", EffectivePermissionModeID: "ask"}}
	capability := store.SettingsCapability{EffectiveModelID: "model", EffectivePermissionModeID: "ask"}
	effective := protocol.SettingsEffectivePayload{Outcome: string(store.SettingsCommandApplied), EffectiveModelID: "model", EffectivePermissionModeID: "ask"}
	if got, reason := settingsFinalizationOutcome(command, capability, effective); got != store.SettingsCommandApplied || reason != nil {
		t.Fatalf("applied settings outcome = %v, %v", got, reason)
	}
	capability.EffectiveModelID = "other"
	effective.EffectiveModelID = "other"
	if got, reason := settingsFinalizationOutcome(command, capability, effective); got != store.SettingsCommandMismatched || reason == nil {
		t.Fatalf("mismatched settings outcome = %v, %v", got, reason)
	}
	command.RequestedModelID = nil
	command.RequestedPermissionModeID = nil
	capability.EffectiveModelID = "old"
	effective.EffectiveModelID = "old"
	effective.Outcome = string(store.SettingsCommandOutcomeUnknown)
	if got, reason := settingsFinalizationOutcome(command, capability, effective); got != store.SettingsCommandOutcomeUnknown || reason != nil {
		t.Fatalf("unknown settings outcome = %v, %v", got, reason)
	}
	effective.Outcome = string(store.SettingsCommandRejected)
	if got, reason := settingsFinalizationOutcome(command, capability, effective); got != store.SettingsCommandRejected || reason != nil {
		t.Fatalf("rejected settings outcome = %v, %v", got, reason)
	}
}

func TestCoreCoverageDurableReplayPolicies(t *testing.T) {
	completedPayload := []byte(`{"cmd_id":"cmd","operation":"stop","outcome":"completed","completion_state":"ended","reason_code":null}`)
	storeEvents := map[string][]store.Event{"ses": {
		{SessionID: "ses", Seq: 1, Type: "session.message", Payload: []byte(`{"message_id":"cmd","role":"user","text":"hello"}`)},
		{SessionID: "ses", Seq: 2, Type: "session.state", Payload: []byte(`{"state":"ready"}`)},
		{SessionID: "ses", Seq: 3, Type: "session.run.outcome", Payload: completedPayload},
	}}
	fake := &replayEventStore{events: storeEvents}
	handler := &webSocketHandler{events: fake}
	seq := int64(3)
	events, err := handler.runControlTerminalEvents(context.Background(), "ses", store.RunControlReservation{Outcome: store.RunControlCompleted, TerminalEventSeq: &seq})
	if err != nil || len(events) != 2 || events[0].Type != "session.state" || events[1].Type != "session.run.outcome" {
		t.Fatalf("completed terminal events = %+v, err=%v", events, err)
	}
	if _, err := handler.runControlTerminalEvents(context.Background(), "ses", store.RunControlReservation{Outcome: store.RunControlPending, TerminalEventSeq: &seq}); err == nil {
		t.Fatal("pending run-control reservation produced terminal events")
	}

	pending := store.PendingCommand{SessionID: "ses", CommandID: "cmd", EventSeq: 1, Status: store.PendingCommandReceived}
	command, err := handler.commandFromPendingEvent(context.Background(), pending)
	if err != nil || command.CommandID != "cmd" || command.Type != protocol.CommandSessionSend {
		t.Fatalf("replayed command = %+v, err=%v", command, err)
	}
	fake.events["ses"][0].Type = "unsupported.event"
	if _, err := handler.commandFromPendingEvent(context.Background(), pending); err == nil {
		t.Fatal("unsupported durable command event accepted")
	}
	seq = 2
	if _, err := handler.runControlTerminalEvents(context.Background(), "ses", store.RunControlReservation{Outcome: store.RunControlRejected, TerminalEventSeq: &seq}); err == nil {
		t.Fatal("non-outcome event accepted as run-control terminal")
	}
}

type replayEventStore struct {
	events map[string][]store.Event
}

func (s *replayEventStore) Append(_ context.Context, sessionID string, pending []store.PendingEvent) (int64, error) {
	first := int64(len(s.events[sessionID]) + 1)
	for index, item := range pending {
		s.events[sessionID] = append(s.events[sessionID], store.Event{SessionID: sessionID, Seq: first + int64(index), Type: item.Type, Time: item.Time, Payload: item.Payload})
	}
	return first, nil
}

func (s *replayEventStore) Replay(_ context.Context, sessionID string, afterSeq int64, fn func(store.Event) error) error {
	for _, event := range s.events[sessionID] {
		if event.Seq > afterSeq {
			if err := fn(event); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *replayEventStore) LatestSeq(_ context.Context, sessionID string) (int64, error) {
	events := s.events[sessionID]
	if len(events) == 0 {
		return 0, nil
	}
	return events[len(events)-1].Seq, nil
}

func TestCoreCoverageDispatcherAndPendingJoinPolicies(t *testing.T) {
	failure := errors.New("dispatcher failed")
	handler := &webSocketHandler{activityDispatcherErr: failure, pendingTargetJoins: map[string]*pendingTargetJoin{}, pendingTargetJoinByAttach: map[string]*pendingTargetJoin{}}
	if err := handler.RunActivityDispatcher(context.Background()); !errors.Is(err, failure) || !errors.Is(handler.RequestActivityRefresh(context.Background()), failure) {
		t.Fatal("dispatcher error was not preserved")
	}
	handler.activityDispatcherErr = nil
	if err := handler.RunActivityDispatcher(context.Background()); err != nil {
		t.Fatalf("nil dispatcher run = %v", err)
	}
	if err := handler.RequestActivityRefresh(context.Background()); err == nil {
		t.Fatal("nil dispatcher refresh was accepted")
	}
	valid := &pendingTargetJoin{attachID: "valid", expiresAt: time.Now().Add(time.Minute), finished: make(chan struct{})}
	expired := &pendingTargetJoin{attachID: "expired", expiresAt: time.Now().Add(-time.Minute), finished: make(chan struct{})}
	handler.pendingTargetJoins["valid"] = valid
	handler.pendingTargetJoins["expired"] = expired
	handler.pendingTargetJoinByAttach[valid.attachID] = valid
	handler.pendingTargetJoinByAttach[expired.attachID] = expired
	handler.pendingTargetJoinMu.Lock()
	handler.prunePendingTargetJoinsLocked(time.Now())
	handler.pendingTargetJoinMu.Unlock()
	if len(handler.pendingTargetJoins) != 1 || len(handler.pendingTargetJoinByAttach) != 1 {
		t.Fatalf("pending join pruning = %+v %+v", handler.pendingTargetJoins, handler.pendingTargetJoinByAttach)
	}
}

func TestCoreCoverageHistoryValidationPolicies(t *testing.T) {
	handler := &webSocketHandler{publisherEphemeralTypes: map[string]struct{}{}}
	request := &protocol.HistoryPageRequest{RequestID: "req", SessionID: "ses", Limit: 3}
	page := store.HistoryPage{LatestSeq: 3, RetentionState: store.RetentionComplete, Events: []store.Event{
		{SessionID: "ses", Seq: 1, Type: "session.message", Payload: []byte(`{"message":"one"}`)},
		{SessionID: "ses", Seq: 2, Type: "session.message", Payload: []byte(`{"message":"two"}`)},
		{SessionID: "ses", Seq: 3, Type: "session.message", Payload: []byte(`{"message":"three"}`)},
	}}
	if !handler.validHistoryPage(page, request) {
		t.Fatal("valid complete history page rejected")
	}
	if handler.validHistoryPage(store.HistoryPage{LatestSeq: 3, RetentionState: "invalid", Events: page.Events}, request) {
		t.Fatal("invalid retention state accepted")
	}
	badOrder := page
	badOrder.Events = append([]store.Event(nil), page.Events...)
	badOrder.Events[1].Seq = 1
	if handler.validHistoryPage(badOrder, request) {
		t.Fatal("non-monotonic history page accepted")
	}
	before := int64(2)
	request.BeforeSeq = &before
	if handler.validHistoryPage(page, request) {
		t.Fatal("history event at or after before_seq accepted")
	}
}

type attentionAuthorizerStub struct {
	grant auth.AttentionGrant
	err   error
}

func (s attentionAuthorizerStub) AuthorizeAttention(context.Context, auth.Principal) (auth.AttentionGrant, error) {
	return s.grant, s.err
}

type sessionAdmissionStub struct {
	auth.Authenticator
	claim auth.SessionAdmissionClaim
	err   error
}

func (s sessionAdmissionStub) SessionAdmissionClaim(context.Context, auth.Principal, string) (auth.SessionAdmissionClaim, error) {
	return s.claim, s.err
}

type truthEventStore struct {
	*replayEventStore
	truth store.SessionAdmissionTruth
}

func (s truthEventStore) SessionAdmissionTruth(context.Context, string) (store.SessionAdmissionTruth, error) {
	return s.truth, nil
}

func TestCoreCoverageHandshakeAdmissionPolicies(t *testing.T) {
	principal := auth.Principal{Subject: "sub", Scopes: []auth.Scope{{Kind: auth.KindAttention, ID: "att", Access: auth.AccessAttention}}}
	if !attentionPrincipal(principal) || attentionPrincipal(auth.Principal{}) {
		t.Fatal("attention principal classification mismatch")
	}
	grant := auth.AttentionGrant{Subject: "sub", SessionIDs: []string{"ses"}, MaxSessions: 1, ExpiresAt: time.Now().Add(time.Minute)}
	if got, err := attentionGrant(context.Background(), attentionAuthorizerStub{grant: grant}, principal); err != nil || got.Subject != "sub" {
		t.Fatalf("attention grant = %+v, err=%v", got, err)
	}
	if _, err := attentionGrant(context.Background(), attentionAuthorizerStub{err: errors.New("denied")}, principal); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("attention authorizer error = %v", err)
	}

	claim := auth.SessionAdmissionClaim{SessionID: "ses", Provider: "provider", ExpiresAt: time.Now().Add(time.Minute)}
	handshake := &Handshake{authenticator: sessionAdmissionStub{claim: claim}, events: noopEventStore{}}
	gotClaim, err := handshake.sessionAdmissionClaim(context.Background(), auth.Principal{}, "ses")
	if err != nil || gotClaim != claim {
		t.Fatalf("session admission claim = %+v, err=%v", gotClaim, err)
	}
	if _, err := (&Handshake{}).sessionAdmissionClaim(context.Background(), auth.Principal{}, "ses"); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("missing session admission authenticator = %v", err)
	}
	truthStore := truthEventStore{replayEventStore: &replayEventStore{}, truth: store.SessionAdmissionTruth{SessionID: "ses", Exists: true, Complete: true, Live: true}}
	handshake.events = truthStore
	truth, err := handshake.sessionAdmissionTruth(context.Background(), "ses")
	if err != nil || !truth.Exists {
		t.Fatalf("session admission truth = %+v, err=%v", truth, err)
	}
}
