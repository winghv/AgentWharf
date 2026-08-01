package storetest

import (
	"context"
	"testing"

	"github.com/winghv/agentwharf/store"
)

type workspaceLeaseSurface interface {
	ReserveWorkspaceLease(context.Context, store.WorkspaceLeaseReserve) (store.WorkspaceLease, error)
	WorkspaceLease(context.Context, store.WorkspaceLeaseKey) (store.WorkspaceLease, error)
	RecordWorkspaceStartReceived(context.Context, store.WorkspaceLeaseKey, int64, store.WorkspaceLeaseOwner) (store.WorkspaceLease, error)
	QuarantineWorkspaceLease(context.Context, store.WorkspaceLeaseKey, int64) (store.WorkspaceLease, error)
	ReleaseWorkspaceLeaseAfterQuiescence(context.Context, store.WorkspaceLeaseKey, int64, store.WorkspaceLeaseOwner) (store.WorkspaceLease, error)
}

func TestWorkspaceLeaseContractSurface(t *testing.T) {
	var _ workspaceLeaseSurface = (store.WorkspaceLeaseStore)(nil)
}
