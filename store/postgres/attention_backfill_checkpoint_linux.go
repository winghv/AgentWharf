//go:build linux

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

const maxAttentionCheckpointBytes = 4096

func (s FileAttentionBackfillCheckpointStore) checkpointDir() (*os.File, string, error) {
	if !filepath.IsAbs(s.Path) || filepath.Base(s.Path) == "." || filepath.Base(s.Path) == ".." {
		return nil, "", errors.New("attention backfill checkpoint path must be absolute")
	}
	fd, err := unix.Open(filepath.Dir(s.Path), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, "", err
	}
	dir := os.NewFile(uintptr(fd), filepath.Dir(s.Path))
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil || stat.Uid != uint32(os.Geteuid()) || stat.Mode&(unix.S_IWGRP|unix.S_IWOTH) != 0 {
		dir.Close()
		return nil, "", errors.New("attention backfill checkpoint directory is not trusted")
	}
	return dir, filepath.Base(s.Path), nil
}

func (s FileAttentionBackfillCheckpointStore) Load(context.Context) (AttentionBackfillCheckpoint, error) {
	dir, name, err := s.checkpointDir()
	if err != nil {
		return AttentionBackfillCheckpoint{}, err
	}
	defer dir.Close()
	fd, err := unix.Openat(int(dir.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, os.ErrNotExist) {
		return AttentionBackfillCheckpoint{}, nil
	}
	if err != nil {
		return AttentionBackfillCheckpoint{}, err
	}
	file := os.NewFile(uintptr(fd), name)
	defer file.Close()
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0o077 != 0 || stat.Uid != uint32(os.Geteuid()) {
		return AttentionBackfillCheckpoint{}, errors.New("attention backfill checkpoint is not trusted")
	}
	b, err := io.ReadAll(io.LimitReader(file, maxAttentionCheckpointBytes+1))
	if err != nil || len(b) > maxAttentionCheckpointBytes {
		return AttentionBackfillCheckpoint{}, errors.New("attention backfill checkpoint is invalid")
	}
	var c AttentionBackfillCheckpoint
	if err := json.Unmarshal(b, &c); err != nil {
		return c, err
	}
	if !validAttentionCheckpoint(c) {
		return AttentionBackfillCheckpoint{}, errors.New("attention backfill checkpoint cursor is invalid")
	}
	return c, nil
}

func (s FileAttentionBackfillCheckpointStore) Save(_ context.Context, c AttentionBackfillCheckpoint) error {
	if !validAttentionCheckpoint(c) {
		return errors.New("attention backfill checkpoint cursor is invalid")
	}
	dir, _, err := s.checkpointDir()
	if err != nil {
		return err
	}
	defer dir.Close()
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	tmpName := fmt.Sprintf(".attention-backfill-%d", time.Now().UnixNano())
	tmpFD, err := unix.Openat(int(dir.Fd()), tmpName, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return err
	}
	tmp := os.NewFile(uintptr(tmpFD), tmpName)
	defer func() { _ = unix.Unlinkat(int(dir.Fd()), tmpName, 0) }()
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := unix.Renameat(int(dir.Fd()), tmpName, int(dir.Fd()), filepath.Base(s.Path)); err != nil {
		return err
	}
	return dir.Sync()
}
