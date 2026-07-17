package static

import (
	"context"
	"time"

	"github.com/winghv/agentwharf/auth"
)

func (a *Authenticator) AdapterCredential(ctx context.Context, _ string, principal auth.Principal, sessionID string) (int64, int64, bool, error) {
	return 1, time.Now().AddDate(10, 0, 0).UnixNano(), true, a.Authorize(ctx, principal, auth.SessionAdapter(sessionID))
}
