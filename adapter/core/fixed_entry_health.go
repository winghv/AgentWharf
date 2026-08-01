package core

import (
	"context"
	"fmt"
	"os"
	"time"
)

const fixedEntryHealthInterval = 30 * time.Second

func StartFixedEntryHealth(ctx context.Context, marker string) (func(), error) {
	if marker == "" {
		return func() {}, nil
	}
	info, err := os.Lstat(marker)
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("fixed-entry health marker is unavailable")
	}
	touch := func() error { return os.Chtimes(marker, time.Now(), time.Now()) }
	if err := touch(); err != nil {
		return nil, fmt.Errorf("touch fixed-entry health marker: %w", err)
	}
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(fixedEntryHealthInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				_ = touch()
			}
		}
	}()
	return func() { close(stop) }, nil
}
