//go:build !linux && !darwin

package e2ee

import (
	"context"
	"database/sql"
)

// No unprotected fallback for platforms without an approved local store.
func OpenLocalDatabase(context.Context, string) (*sql.DB, error) {
	return nil, ErrJournal
}
