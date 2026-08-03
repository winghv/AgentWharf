package postgres

import (
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
)

func TestPostgresTimestampNormalizesIdempotencyInputs(t *testing.T) {
	value := time.Date(2026, time.July, 28, 13, 40, 0, 123456789, time.FixedZone("offset", 8*60*60))
	want := time.Date(2026, time.July, 28, 5, 40, 0, 123456000, time.UTC)

	if got := postgresTimestamp(value); !got.Equal(want) || got.Location() != time.UTC {
		t.Fatalf("postgresTimestamp() = %v; want %v UTC", got, want)
	}

	attachmentExpiry := value
	blockerExpiry := value
	updated := normalizeAttachmentUpdate(store.AttachmentUpdate{
		ExpiresAt: &attachmentExpiry,
		Blocker:   &store.AttachmentBlocker{ExpiresAt: &blockerExpiry},
	})
	if !updated.ExpiresAt.Equal(want) || !updated.Blocker.ExpiresAt.Equal(want) {
		t.Fatalf("normalizeAttachmentUpdate() = %+v; want microsecond expiries", updated)
	}
	if !attachmentExpiry.Equal(value) || !blockerExpiry.Equal(value) {
		t.Fatal("normalizeAttachmentUpdate() mutated caller-owned timestamps")
	}

	warm := normalizeWarmAttachRequest(store.WarmAttachRequest{
		Attempt:          store.AttachAttemptRequest{ExpiresAt: value},
		Attachment:       store.AttachmentCreate{ExpiresAt: value},
		TargetActivation: store.WarmAttachTargetActivation{ExpiresAt: value},
		FirstDelivery:    store.WarmAttachFirstDelivery{ExpiresAt: value},
	})
	if !warm.Attempt.ExpiresAt.Equal(want) || !warm.Attachment.ExpiresAt.Equal(want) || !warm.TargetActivation.ExpiresAt.Equal(want) || !warm.FirstDelivery.ExpiresAt.Equal(want) {
		t.Fatalf("normalizeWarmAttachRequest() = %+v; want microsecond expiries", warm)
	}

	childExpiry := value
	lease := normalizeWorkspaceLeaseReserve(store.WorkspaceLeaseReserve{
		ExpiresAt: value,
		ChildScope: &store.WorkspaceLeaseChildScope{
			ExpiresAt: childExpiry,
		},
	})
	if !lease.ExpiresAt.Equal(want) || !lease.ChildScope.ExpiresAt.Equal(want) {
		t.Fatalf("normalizeWorkspaceLeaseReserve() = %+v; want microsecond expiries", lease)
	}
	if !childExpiry.Equal(value) {
		t.Fatal("normalizeWorkspaceLeaseReserve() mutated caller-owned child scope")
	}
}
