package hub

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/winghv/agentwharf/auth"
	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/sqlite"
)

func TestAdapterAdmissionBindsOpaqueSettingsWriterLease(t *testing.T) {
	ctx := context.Background()
	ledger, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	authority := &adapterDispatchAuthority{
		store: ledger,
		adapterCredential: func(context.Context, string, auth.Principal, string) (int64, int64, bool, error) {
			return 1, time.Now().Add(time.Hour).UnixNano(), true, nil
		},
	}
	first, err := authority.admit(ctx, "ses_lease", 1, time.Now().Add(time.Hour), true)
	if err != nil || first.writer.LeaseID == "" {
		t.Fatalf("first admit = %+v, %v", first, err)
	}
	if _, err := ledger.Append(ctx, "ses_lease", []store.PendingEvent{{Type: "session.settings.capabilities", Time: time.Now(), Payload: json.RawMessage(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.PublishSettingsCapability(ctx, "ses_lease", store.SettingsCapabilityUpdate{
		EventSeq: 1, Fingerprint: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		EffectiveModelID: "balanced", EffectivePermissionModeID: "ask", Writer: first.writer,
	}); err != nil {
		t.Fatalf("publish with admitted writer = %v", err)
	}
	second, err := authority.admit(ctx, "ses_lease", 1, time.Now().Add(time.Hour), false)
	if err != nil || second.writer.LeaseID == "" || second.writer.LeaseID == first.writer.LeaseID {
		t.Fatalf("replacement admit = %+v, %v", second, err)
	}
	if _, err := ledger.Append(ctx, "ses_lease", []store.PendingEvent{{Type: "session.settings.capabilities", Time: time.Now(), Payload: json.RawMessage(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.PublishSettingsCapability(ctx, "ses_lease", store.SettingsCapabilityUpdate{
		EventSeq: 2, Fingerprint: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		EffectiveModelID: "balanced", EffectivePermissionModeID: "ask", Writer: first.writer,
	}); err == nil {
		t.Fatal("replaced writer lease unexpectedly published capability")
	}
}
