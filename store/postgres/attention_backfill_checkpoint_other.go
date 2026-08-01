//go:build !linux

package postgres

import (
	"context"
	"errors"
)

var errAttentionBackfillCheckpointUnsupported = errors.New("attention backfill file checkpoints require linux")

func (FileAttentionBackfillCheckpointStore) Load(context.Context) (AttentionBackfillCheckpoint, error) {
	return AttentionBackfillCheckpoint{}, errAttentionBackfillCheckpointUnsupported
}

func (FileAttentionBackfillCheckpointStore) Save(context.Context, AttentionBackfillCheckpoint) error {
	return errAttentionBackfillCheckpointUnsupported
}
