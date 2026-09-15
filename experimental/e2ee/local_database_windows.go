//go:build windows

package e2ee

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/windows"
	_ "modernc.org/sqlite"
)

// OpenLocalDatabase requires a private directory beneath a trusted local parent,
// protected on Windows by an explicit owner-only DACL. Like local identity
// storage, it does not defend against host administrators or same-UID malicious
// code. DELETE journaling avoids long-lived WAL sidecars containing keys.
func OpenLocalDatabase(ctx context.Context, directory string) (*sql.DB, error) {
	if !filepath.IsAbs(directory) {
		return nil, ErrInvalid
	}
	sid, err := windowsUserSID()
	if err != nil {
		return nil, err
	}
	directoryHandle, err := openEndpointDirectory(directory, sid)
	if err != nil {
		return nil, err
	}
	defer windows.CloseHandle(directoryHandle)
	databasePath := filepath.Join(directory, "endpoint.db")
	file, err := openEndpointFile(databasePath, windows.GENERIC_READ|windows.GENERIC_WRITE, windows.OPEN_ALWAYS, sid)
	if err != nil {
		return nil, ErrJournal
	}
	_ = file.Close()
	// Reject pre-existing alternate journal modes and redirected rollback files.
	for _, name := range []string{"endpoint.db-journal", "endpoint.db-wal", "endpoint.db-shm"} {
		side, err := openEndpointFile(filepath.Join(directory, name), windows.GENERIC_READ, windows.OPEN_EXISTING, sid)
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, windows.ERROR_FILE_NOT_FOUND) || errors.Is(err, windows.ERROR_PATH_NOT_FOUND) {
			continue
		}
		if err != nil {
			return nil, ErrJournal
		}
		_ = side.Close()
	}
	// A drive-letter path is passed to sqlite3_open_v2 as a plain filename rather
	// than a file: URI, so no URI parser can re-interpret "C:" as an authority or
	// scheme. modernc still reads the query parameters from the same string.
	query := url.Values{"_pragma": {"busy_timeout(3000)", "journal_mode(DELETE)", "synchronous(FULL)", "foreign_keys(ON)"}}.Encode()
	database, err := sql.Open("sqlite", databasePath+"?"+query)
	if err != nil {
		return nil, ErrJournal
	}
	database.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := database.PingContext(ctx); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("open endpoint database: %w", ErrJournal)
	}
	return database, nil
}
