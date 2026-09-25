package sqlite_test

import (
	"path/filepath"
	"testing"

	"github.com/winghv/agentwharf/store/storetest"
)

func TestBootstrapStoreContract(t *testing.T) {
	storetest.BootstrapContract(t, openStore(t, filepath.Join(t.TempDir(), "bootstrap.db")))
}
