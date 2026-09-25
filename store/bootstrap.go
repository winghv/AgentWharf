package store

import "context"

// BootstrapStore restores the latest retained control-state events at the
// handshake watermark. Implementations use indexed seeks, never a full replay.
// Payloads remain opaque, including required-mode encrypted carriers.
type BootstrapStore interface {
	HistoryStore
	Bootstrap(ctx context.Context, sessionID string, throughSeq int64) ([]Event, error)
}

// BootstrapEventTypes is deliberately finite. Transcript events are loaded
// separately with History; control state must survive falling outside that page.
func BootstrapEventTypes() []string {
	return []string{
		"session.state",
		"session.settings.capabilities",
		"session.settings.effective",
		"session.run.capabilities",
		"session.run.outcome",
		"session.file_references.capabilities",
		"permission.request",
		"permission.decision",
	}
}
