package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/sqlite"
	"github.com/winghv/agentwharf/store/storetest"
)

func TestEventStoreContract(t *testing.T) {
	storetest.Contract(t, func(t *testing.T) store.EventStore {
		t.Helper()
		st, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "events.db"))
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		t.Cleanup(func() {
			if err := st.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
		})
		return st
	})
}
