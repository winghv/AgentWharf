package hub

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"sync"
	"time"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/protocol"
	"github.com/winghv/agentwharf/store"
	"nhooyr.io/websocket"
)

const warmAttachCredentialHandoffTimeout = time.Second
const maxPendingTargetJoins = 64

var ErrWarmAttachCredentialNotAccepted = errors.New("warm attach credential was not accepted")

// WarmAttachCredentialDelivery is committed, non-secret target identity. It is
// deliberately independent from the pending-join wire transport owned by T18G.
type WarmAttachCredentialDelivery struct {
	AttachID                   string
	TargetSessionID            string
	TargetCredentialLineageRef string
	Generation                 int64
	ExpiresAt                  time.Time
}

// WarmAttachCredentialHandoff receives one already-committed target bearer.
// Its bounded internal transfer runs while Store owns both final tuples; T18G
// alone owns any later pending-join protocol/socket transport and rechecks it
// at that delivery boundary.
type WarmAttachCredentialHandoff interface {
	DeliverCommittedTargetCredential(context.Context, WarmAttachCredentialDelivery, auth.PreparedSessionCredential) error
}

type pendingTargetJoin struct {
	attachID                   string
	targetSessionID            string
	targetCredentialLineageRef string
	generation                 int64
	expiresAt                  time.Time
	bootstrap                  store.AdapterConnectionAdmission

	conn             *managedConn
	joined           chan struct{}
	readerReady      chan struct{}
	finished         chan struct{}
	claimed          bool
	inputObserved    bool
	deliveryClaimed  bool
	deliveryNonce    string
	deliveryFrame    *protocol.TargetJoinCredential
	deliveryResult   chan error
	deliveryDeadline time.Time
	observeHook      func()
	finishedOnce     sync.Once
	timer            *time.Timer
}

func (h *webSocketHandler) beginPendingTargetJoin(ctx context.Context, authorization auth.AttachAuthorization, activation store.WarmAttachTargetActivation) error {
	nonceBytes := make([]byte, 32)
	if _, err := rand.Read(nonceBytes); err != nil {
		return errors.New("generate pending target join nonce")
	}
	nonce := base64.RawURLEncoding.EncodeToString(nonceBytes)
	expiresAt := activation.ExpiresAt.UTC()
	if deadline := time.Now().Add(30 * time.Second); expiresAt.After(deadline) {
		expiresAt = deadline
	}
	if !expiresAt.After(time.Now()) {
		return errors.New("pending target join is expired")
	}
	entry := &pendingTargetJoin{
		attachID: authorization.Grant.AttachID, targetSessionID: authorization.Grant.TargetSessionID,
		targetCredentialLineageRef: authorization.Grant.Commit.TargetCredentialLineageRef,
		generation:                 activation.Generation, expiresAt: expiresAt,
		bootstrap:   store.AdapterConnectionAdmission{CredentialGeneration: authorization.Bootstrap.CredentialGeneration, ConnectionEpoch: authorization.Bootstrap.ConnectionEpoch, AcceptedFence: authorization.Bootstrap.AcceptedFence, GrantFence: authorization.Grant.GrantFence},
		joined:      make(chan struct{}),
		readerReady: make(chan struct{}),
		finished:    make(chan struct{}),
	}
	h.pendingTargetJoinMu.Lock()
	h.prunePendingTargetJoinsLocked(time.Now())
	if len(h.pendingTargetJoins) >= maxPendingTargetJoins || h.pendingTargetJoinByAttach[entry.attachID] != nil {
		h.pendingTargetJoinMu.Unlock()
		return errors.New("pending target join is unavailable")
	}
	h.startPendingTargetJoinTimer(entry)
	h.pendingTargetJoins[nonce] = entry
	h.pendingTargetJoinByAttach[entry.attachID] = entry
	h.pendingTargetJoinMu.Unlock()

	h.mu.Lock()
	bootstrap := h.adapters[authorization.Grant.BootstrapSessionID]
	h.mu.Unlock()
	if bootstrap == nil || bootstrap.protocolVersion != protocol.ProtocolVersionV2 || bootstrap.provider != authorization.Grant.Provider ||
		bootstrap.admission.CredentialGeneration != entry.bootstrap.CredentialGeneration ||
		bootstrap.admission.ConnectionEpoch != entry.bootstrap.ConnectionEpoch ||
		bootstrap.admission.AcceptedFence != entry.bootstrap.AcceptedFence ||
		h.writeAdapterFrame(ctx, bootstrap, &protocol.TargetJoinChallenge{TargetSessionID: entry.targetSessionID, JoinNonce: nonce, ExpiresAt: entry.expiresAt.UnixMilli()}) != nil {
		h.cancelPendingTargetJoin(entry.attachID)
		return errors.New("deliver pending target join challenge")
	}
	return nil
}

func (h *webSocketHandler) startPendingTargetJoinTimer(entry *pendingTargetJoin) {
	afterFunc := h.pendingTargetJoinTimer
	if afterFunc == nil {
		afterFunc = time.AfterFunc
	}
	entry.timer = afterFunc(time.Until(entry.expiresAt), func() { h.expirePendingTargetJoin(entry) })
}

func (h *webSocketHandler) expirePendingTargetJoin(entry *pendingTargetJoin) {
	h.pendingTargetJoinMu.Lock()
	defer h.pendingTargetJoinMu.Unlock()
	if entry == nil || h.pendingTargetJoinByAttach[entry.attachID] != entry {
		return
	}
	h.forgetPendingTargetJoinLocked(entry)
	if entry.conn != nil {
		entry.finish()
		entry.conn.CloseNow()
	}
}

func (h *webSocketHandler) servePendingTargetJoin(ctx context.Context, conn *managedConn, join *protocol.TargetJoin) {
	entry, err := h.claimPendingTargetJoin(join, conn)
	if err != nil {
		_ = conn.Close(websocket.StatusPolicyViolation, "target join rejected")
		return
	}
	defer h.forgetPendingTargetJoin(entry)

	h.startPendingTargetJoinReader(ctx, entry)
	select {
	case <-entry.finished:
	case <-ctx.Done():
	}
}

func (h *webSocketHandler) startPendingTargetJoinReader(ctx context.Context, entry *pendingTargetJoin) {
	h.pendingTargetJoinMu.Lock()
	if entry == nil || h.pendingTargetJoinByAttach[entry.attachID] != entry || entry.conn == nil {
		h.pendingTargetJoinMu.Unlock()
		return
	}
	close(entry.readerReady)
	conn := entry.conn
	h.pendingTargetJoinMu.Unlock()
	go func() {
		conn.setPongHandler(func(nonce string) error {
			return h.completePendingTargetJoinDelivery(entry, nonce)
		})
		_, _ = readProtocolFrame(ctx, conn)
		h.observePendingTargetJoinInput(entry)
	}()
}

func (h *webSocketHandler) observePendingTargetJoinInput(entry *pendingTargetJoin) {
	if entry != nil && entry.observeHook != nil {
		entry.observeHook()
	}
	h.pendingTargetJoinMu.Lock()
	defer h.pendingTargetJoinMu.Unlock()
	if entry == nil || h.pendingTargetJoinByAttach[entry.attachID] != entry || entry.deliveryClaimed {
		return
	}
	entry.inputObserved = true
	if entry.deliveryResult != nil {
		entry.deliveryResult <- ErrWarmAttachCredentialNotAccepted
	}
	h.forgetPendingTargetJoinLocked(entry)
	if entry.conn != nil {
		entry.conn.CloseNow()
	}
}

func (h *webSocketHandler) claimPendingTargetJoin(join *protocol.TargetJoin, conn *managedConn) (*pendingTargetJoin, error) {
	if join == nil || join.ProtocolVersion != protocol.ProtocolVersionV2 || len(join.JoinNonce) < protocol.MinTargetJoinNonceBytes || conn == nil {
		return nil, ErrWarmAttachCredentialNotAccepted
	}
	h.pendingTargetJoinMu.Lock()
	defer h.pendingTargetJoinMu.Unlock()
	h.prunePendingTargetJoinsLocked(time.Now())
	entry := h.pendingTargetJoins[join.JoinNonce]
	if entry == nil || entry.claimed || entry.inputObserved || entry.deliveryClaimed || !entry.expiresAt.After(time.Now()) {
		return nil, ErrWarmAttachCredentialNotAccepted
	}
	entry.claimed, entry.conn = true, conn
	close(entry.joined)
	return entry, nil
}

func (h *webSocketHandler) DeliverCommittedTargetCredential(ctx context.Context, delivery WarmAttachCredentialDelivery, prepared auth.PreparedSessionCredential) error {
	entry, err := h.awaitPendingTargetJoin(ctx, delivery)
	if err != nil {
		return err
	}
	frame := &protocol.TargetJoinCredential{
		Credential: prepared.Bearer, TargetSessionID: delivery.TargetSessionID, TargetCredentialLineageRef: delivery.TargetCredentialLineageRef,
		Generation: delivery.Generation, ExpiresAt: delivery.ExpiresAt.UnixMilli(),
	}
	if err := h.claimPendingTargetJoinDelivery(ctx, entry, frame); err != nil {
		return err
	}
	return nil
}

// claimPendingTargetJoinDelivery establishes a reader-owned WebSocket control
// barrier before a bearer write. The sole reader stops at every data frame; a
// returned Ping proves no earlier data frame was decoded, even if its observer
// has not yet acquired pendingTargetJoinMu.
func (h *webSocketHandler) claimPendingTargetJoinDelivery(ctx context.Context, entry *pendingTargetJoin, frame *protocol.TargetJoinCredential) error {
	nonceBytes := make([]byte, 24)
	if _, err := rand.Read(nonceBytes); err != nil {
		return ErrWarmAttachCredentialNotAccepted
	}
	nonce := base64.RawURLEncoding.EncodeToString(nonceBytes)
	h.pendingTargetJoinMu.Lock()
	if entry == nil || h.pendingTargetJoinByAttach[entry.attachID] != entry || entry.inputObserved || entry.deliveryClaimed || entry.conn == nil {
		h.pendingTargetJoinMu.Unlock()
		return ErrWarmAttachCredentialNotAccepted
	}
	conn := entry.conn
	deadline := entry.expiresAt
	if candidate, ok := ctx.Deadline(); ok && candidate.Before(deadline) {
		deadline = candidate
	}
	entry.deliveryNonce, entry.deliveryFrame, entry.deliveryResult, entry.deliveryDeadline = nonce, frame, make(chan error, 1), deadline
	result := entry.deliveryResult
	h.pendingTargetJoinMu.Unlock()
	if err := conn.writePing(ctx, nonce); err != nil {
		return h.rejectPendingTargetJoinDelivery(entry)
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return h.rejectPendingTargetJoinDelivery(entry)
	}
}

// completePendingTargetJoinDelivery runs in the only connection reader before
// it can advance to another data frame.
func (h *webSocketHandler) completePendingTargetJoinDelivery(entry *pendingTargetJoin, nonce string) error {
	h.pendingTargetJoinMu.Lock()
	defer h.pendingTargetJoinMu.Unlock()
	if entry == nil || nonce != entry.deliveryNonce || entry.deliveryResult == nil || entry.deliveryFrame == nil ||
		h.pendingTargetJoinByAttach[entry.attachID] != entry || entry.inputObserved || entry.deliveryClaimed || entry.conn == nil || !entry.expiresAt.After(time.Now()) {
		return nil
	}
	entry.deliveryClaimed = true
	ctx, cancel := context.WithDeadline(context.Background(), entry.deliveryDeadline)
	err := writeProtocolFrame(ctx, entry.conn, entry.deliveryFrame)
	cancel()
	if err != nil {
		entry.deliveryResult <- ErrWarmAttachCredentialNotAccepted
		h.forgetPendingTargetJoinLocked(entry)
		entry.finish()
		return nil
	}
	entry.deliveryResult <- nil
	h.forgetPendingTargetJoinLocked(entry)
	entry.finish()
	_ = entry.conn.CloseNow()
	return nil
}

func (h *webSocketHandler) rejectPendingTargetJoinDelivery(entry *pendingTargetJoin) error {
	h.pendingTargetJoinMu.Lock()
	defer h.pendingTargetJoinMu.Unlock()
	if entry != nil && h.pendingTargetJoinByAttach[entry.attachID] == entry {
		h.forgetPendingTargetJoinLocked(entry)
		if entry.conn != nil {
			entry.conn.CloseNow()
		}
	}
	return ErrWarmAttachCredentialNotAccepted
}

func (h *webSocketHandler) awaitPendingTargetJoin(ctx context.Context, delivery WarmAttachCredentialDelivery) (*pendingTargetJoin, error) {
	h.pendingTargetJoinMu.Lock()
	entry := h.pendingTargetJoinByAttach[delivery.AttachID]
	h.pendingTargetJoinMu.Unlock()
	if entry == nil || entry.targetSessionID != delivery.TargetSessionID || entry.targetCredentialLineageRef != delivery.TargetCredentialLineageRef ||
		entry.generation != delivery.Generation || !entry.expiresAt.After(time.Now()) {
		return nil, ErrWarmAttachCredentialNotAccepted
	}
	select {
	case <-entry.joined:
	case <-ctx.Done():
		return nil, ErrWarmAttachCredentialNotAccepted
	}
	select {
	case <-entry.readerReady:
	case <-ctx.Done():
		return nil, ErrWarmAttachCredentialNotAccepted
	}
	h.pendingTargetJoinMu.Lock()
	valid := h.pendingTargetJoinByAttach[delivery.AttachID] == entry && !entry.inputObserved && !entry.deliveryClaimed && entry.conn != nil
	h.pendingTargetJoinMu.Unlock()
	if !valid {
		return nil, ErrWarmAttachCredentialNotAccepted
	}
	return entry, nil
}

func (h *webSocketHandler) cancelPendingTargetJoin(attachID string) {
	h.pendingTargetJoinMu.Lock()
	defer h.pendingTargetJoinMu.Unlock()
	if entry := h.pendingTargetJoinByAttach[attachID]; entry != nil {
		h.forgetPendingTargetJoinLocked(entry)
		if entry.conn != nil {
			entry.conn.CloseNow()
		}
	}
}

func (h *webSocketHandler) forgetPendingTargetJoin(entry *pendingTargetJoin) {
	h.pendingTargetJoinMu.Lock()
	defer h.pendingTargetJoinMu.Unlock()
	h.forgetPendingTargetJoinLocked(entry)
}

func (h *webSocketHandler) forgetPendingTargetJoinLocked(entry *pendingTargetJoin) {
	if entry == nil {
		return
	}
	entry.finish()
	if entry.timer != nil {
		entry.timer.Stop()
	}
	for nonce, current := range h.pendingTargetJoins {
		if current == entry {
			delete(h.pendingTargetJoins, nonce)
		}
	}
	if h.pendingTargetJoinByAttach[entry.attachID] == entry {
		delete(h.pendingTargetJoinByAttach, entry.attachID)
	}
}

func (entry *pendingTargetJoin) finish() {
	entry.finishedOnce.Do(func() { close(entry.finished) })
}

func (h *webSocketHandler) prunePendingTargetJoinsLocked(now time.Time) {
	for _, entry := range h.pendingTargetJoins {
		if !entry.expiresAt.After(now) {
			h.forgetPendingTargetJoinLocked(entry)
			if entry.conn != nil {
				entry.conn.CloseNow()
			}
		}
	}
}

type warmAttachCredentialStore interface {
	store.WarmAttachStore
	store.AdapterConnectionTransactor
	store.WarmAttachTargetActivationStore
	store.AttachmentStore
}

func (h *webSocketHandler) prepareWarmAttachCredential(ctx context.Context, authorization auth.AttachAuthorization) (store.WarmAttachTargetActivation, auth.PreparedSessionCredential, error) {
	if h.sessionCredentialIssuer == nil || h.sessionCredentialLifecycle == nil {
		return store.WarmAttachTargetActivation{}, auth.PreparedSessionCredential{}, errors.New("warm attach credential delivery is not configured")
	}
	expiresAt := authorization.Grant.DeliveryDeadline.UTC().Truncate(time.Millisecond)
	activation := store.WarmAttachTargetActivation{Generation: 1, ExpiresAt: expiresAt}
	prepared, err := h.sessionCredentialIssuer.PrepareSessionCredential(ctx, auth.SessionCredentialRequest{
		SessionID: authorization.Grant.TargetSessionID,
		Lineage: auth.SessionCredentialLineage{
			Kind: auth.SessionCredentialTargetAttach, AttachID: authorization.Grant.AttachID, JTI: authorization.Grant.JTI,
		},
		Generation: activation.Generation, RotationID: authorization.Grant.Commit.TargetCredentialLineageRef,
		RevocationID: authorization.Grant.Commit.TargetCredentialLineageRef, ExpiresAt: activation.ExpiresAt,
	})
	if err != nil || !validPreparedWarmAttachCredential(prepared, authorization, activation) {
		return store.WarmAttachTargetActivation{}, auth.PreparedSessionCredential{}, errors.New("prepare warm attach credential")
	}
	return activation, prepared, nil
}

func validPreparedWarmAttachCredential(prepared auth.PreparedSessionCredential, authorization auth.AttachAuthorization, activation store.WarmAttachTargetActivation) bool {
	return prepared.Bearer != "" && prepared.SessionID == authorization.Grant.TargetSessionID &&
		prepared.Lineage == (auth.SessionCredentialLineage{Kind: auth.SessionCredentialTargetAttach, AttachID: authorization.Grant.AttachID, JTI: authorization.Grant.JTI}) &&
		prepared.Generation == activation.Generation && prepared.RotationID == authorization.Grant.Commit.TargetCredentialLineageRef &&
		prepared.RevocationID == authorization.Grant.Commit.TargetCredentialLineageRef && prepared.ExpiresAt.Equal(activation.ExpiresAt) &&
		prepared.Scope == auth.SessionAdapter(authorization.Grant.TargetSessionID)
}

func (h *webSocketHandler) handoffCommittedWarmAttachCredential(ctx context.Context, credentialStore warmAttachCredentialStore, authorization auth.AttachAuthorization, admission store.AdapterConnectionAdmission, commit store.WarmAttachCommit, prepared auth.PreparedSessionCredential) error {
	if h.warmAttachCredentialHandoff == nil || !validPreparedWarmAttachCredential(prepared, authorization, commit.TargetActivation) {
		return errors.New("warm attach credential handoff is not configured")
	}
	if commit.Duplicate && commit.Attachment.DeliveryState == store.AttachmentDeliveryCompleted {
		h.cancelPendingTargetJoin(commit.Attachment.Identity.AttachID)
		return nil
	}
	if commit.Duplicate && commit.Attachment.DeliveryState != store.AttachmentDeliveryPending {
		return errors.New("warm attach credential handoff outcome is unknown")
	}
	delivery := WarmAttachCredentialDelivery{
		AttachID: authorization.Grant.AttachID, TargetSessionID: authorization.Grant.TargetSessionID,
		TargetCredentialLineageRef: authorization.Grant.Commit.TargetCredentialLineageRef,
		Generation:                 commit.TargetActivation.Generation, ExpiresAt: commit.TargetActivation.ExpiresAt,
	}
	// Waiting for the target socket is not a Store operation. Keep it outside
	// the final revalidation transaction so a missing worker cannot stall the
	// bootstrap's liveness/fencing path.
	if _, err := h.awaitPendingTargetJoin(ctx, delivery); err != nil {
		return h.releaseWarmAttachCredentialHandoff(ctx, commit.Attachment, credentialStore)
	}
	claimed, err := credentialStore.UpdateAttachment(ctx, commit.Attachment.Identity.AttachID, commit.Attachment.DeliveryVersion, store.AttachmentUpdate{
		Status: store.AttachmentJoinPending, DeliveryState: store.AttachmentDeliveryReceived,
		ExpiresAt: commit.Attachment.ExpiresAt,
	})
	if err != nil {
		return errors.New("claim warm attach credential handoff")
	}
	commit.Attachment = claimed.Attachment
	handoffCtx, cancel := context.WithTimeout(ctx, warmAttachCredentialHandoffTimeout)
	defer cancel()
	err = credentialStore.WithAdapterConnectionTransaction(handoffCtx, func(tx store.AdapterConnectionStore) error {
		activationStore, ok := tx.(store.WarmAttachTargetActivationStore)
		if !ok {
			return errors.New("warm attach activation transaction is unavailable")
		}
		if _, err := tx.ValidateAdapterAdmission(handoffCtx, authorization.Grant.BootstrapSessionID, admission); err != nil {
			return errors.New("warm attach bootstrap authority lost")
		}
		if err := activationStore.ValidateWarmAttachTargetActivation(handoffCtx, authorization.Grant.TargetSessionID, commit.TargetActivation); err != nil {
			return errors.New("warm attach target authority lost")
		}
		return h.warmAttachCredentialHandoff.DeliverCommittedTargetCredential(handoffCtx, delivery, prepared)
	})
	if err != nil {
		if errors.Is(err, ErrWarmAttachCredentialNotAccepted) {
			return h.releaseWarmAttachCredentialHandoff(ctx, commit.Attachment, credentialStore)
		}
		return h.failClosedWarmAttachCredentialHandoff(commit.Attachment, credentialStore)
	}
	if _, err := credentialStore.UpdateAttachment(ctx, commit.Attachment.Identity.AttachID, commit.Attachment.DeliveryVersion, store.AttachmentUpdate{
		Status: store.AttachmentJoinPending, DeliveryState: store.AttachmentDeliveryCompleted,
		ExpiresAt: commit.Attachment.ExpiresAt,
	}); err != nil {
		return h.failClosedWarmAttachCredentialHandoff(commit.Attachment, credentialStore)
	}
	return nil
}

func (h *webSocketHandler) activateCommittedWarmAttachCredential(ctx context.Context, prepared auth.PreparedSessionCredential) error {
	if h.sessionCredentialLifecycle == nil || h.sessionCredentialLifecycle.ActivateSessionCredential(ctx, prepared) != nil {
		return errors.New("warm attach credential activation is not configured")
	}
	return nil
}

func (h *webSocketHandler) discardWarmAttachCredential(ctx context.Context, prepared auth.PreparedSessionCredential) {
	if h.sessionCredentialLifecycle != nil {
		h.sessionCredentialLifecycle.DiscardSessionCredential(ctx, prepared)
	}
}

func (h *webSocketHandler) releaseWarmAttachCredentialHandoff(ctx context.Context, attachment store.Attachment, credentialStore store.AttachmentStore) error {
	_, err := credentialStore.UpdateAttachment(ctx, attachment.Identity.AttachID, attachment.DeliveryVersion, store.AttachmentUpdate{Status: store.AttachmentJoinPending, DeliveryState: store.AttachmentDeliveryPending, ExpiresAt: attachment.ExpiresAt})
	if err != nil {
		return h.failClosedWarmAttachCredentialHandoff(attachment, credentialStore)
	}
	return ErrWarmAttachCredentialNotAccepted
}

func (h *webSocketHandler) failClosedWarmAttachCredentialHandoff(attachment store.Attachment, credentialStore store.AttachmentStore) error {
	ctx, cancel := context.WithTimeout(context.Background(), warmAttachCredentialHandoffTimeout)
	defer cancel()
	_, _ = credentialStore.UpdateAttachment(ctx, attachment.Identity.AttachID, attachment.DeliveryVersion, store.AttachmentUpdate{
		Status: store.AttachmentReauthorizationRequired, DeliveryState: store.AttachmentDeliveryOutcomeUnknown,
		Blocker: &store.AttachmentBlocker{Kind: store.AttachmentBlockerOutcomeUnknown, Operation: stringPointer("credential_handoff")},
	})
	return errors.New("warm attach credential handoff outcome is unknown")
}

func stringPointer(value string) *string { return &value }
