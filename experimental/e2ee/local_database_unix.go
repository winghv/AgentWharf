//go:build linux || darwin

package e2ee

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

// OpenLocalDatabase requires a private directory beneath a trusted local parent.
// Like local identity storage, it does not defend against host root or same-UID
// malicious code. DELETE journaling avoids long-lived WAL sidecars containing keys.
func OpenLocalDatabase(ctx context.Context, directory string) (*sql.DB, error) {
	if !filepath.IsAbs(directory) {
		return nil, ErrInvalid
	}
	fd, err := unix.Open(directory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, ErrJournal
	}
	defer unix.Close(fd)
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || stat.Uid != uint32(os.Geteuid()) || stat.Mode&0777 != 0700 {
		return nil, ErrJournal
	}
	fileFD, err := unix.Openat(fd, "endpoint.db", unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err == unix.EEXIST {
		fileFD, err = unix.Openat(fd, "endpoint.db", unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	}
	if err != nil {
		return nil, ErrJournal
	}
	valid := privateRegular(fileFD)
	_ = unix.Close(fileFD)
	if !valid {
		return nil, ErrJournal
	}
	// Reject pre-existing alternate journal modes and redirected rollback files.
	for _, name := range []string{"endpoint.db-journal", "endpoint.db-wal", "endpoint.db-shm"} {
		var side unix.Stat_t
		err := unix.Fstatat(fd, name, &side, unix.AT_SYMLINK_NOFOLLOW)
		if err == unix.ENOENT {
			continue
		}
		if err != nil || side.Uid != uint32(os.Geteuid()) || side.Mode&unix.S_IFMT != unix.S_IFREG || side.Mode&0077 != 0 || side.Nlink != 1 {
			return nil, ErrJournal
		}
	}
	uri := url.URL{Scheme: "file", Path: filepath.Join(directory, "endpoint.db")}
	query := url.Values{"mode": {"rw"}, "_pragma": {"busy_timeout(3000)", "journal_mode(DELETE)", "synchronous(FULL)", "foreign_keys(ON)"}}
	uri.RawQuery = query.Encode()
	database, err := sql.Open("sqlite", uri.String())
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
