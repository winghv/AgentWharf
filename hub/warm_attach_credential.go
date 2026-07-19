package hub

import (
	"context"
	"errors"
	"time"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/store"
)

const warmAttachCredentialHandoffTimeout = time.Second

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

type warmAttachCredentialStore interface {
	store.WarmAttachStore
	store.AdapterConnectionTransactor
	store.WarmAttachTargetActivationStore
	store.AttachmentStore
}

func (h *webSocketHandler) prepareWarmAttachCredential(ctx context.Context, authorization auth.AttachAuthorization) (store.WarmAttachTargetActivation, auth.PreparedSessionCredential, error) {
	if h.sessionCredentialIssuer == nil || h.warmAttachCredentialHandoff == nil {
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
		return nil
	}
	if commit.Duplicate && commit.Attachment.DeliveryState != store.AttachmentDeliveryPending {
		return errors.New("warm attach credential handoff outcome is unknown")
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
		return h.warmAttachCredentialHandoff.DeliverCommittedTargetCredential(handoffCtx, WarmAttachCredentialDelivery{
			AttachID: authorization.Grant.AttachID, TargetSessionID: authorization.Grant.TargetSessionID,
			TargetCredentialLineageRef: authorization.Grant.Commit.TargetCredentialLineageRef,
			Generation:                 commit.TargetActivation.Generation, ExpiresAt: commit.TargetActivation.ExpiresAt,
		}, prepared)
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
