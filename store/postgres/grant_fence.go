package postgres

import (
	"context"
	"errors"
	"fmt"
)

const allocateAdapterGrantFenceSQL = `SELECT nextval('session_adapter_connection_accepted_fence_seq')::bigint`

func (s *Store) AllocateAdapterGrantFence(ctx context.Context) (int64, error) {
	if s == nil {
		return 0, errors.New("postgres grant fence store is nil")
	}
	var fence int64
	if s.connectionTx != nil {
		if err := s.connectionTx.QueryRow(ctx, allocateAdapterGrantFenceSQL).Scan(&fence); err != nil {
			return 0, fmt.Errorf("allocate adapter grant fence: %w", err)
		}
	} else {
		if s.pool == nil {
			return 0, errors.New("postgres grant fence pool is nil")
		}
		if err := s.pool.QueryRow(ctx, allocateAdapterGrantFenceSQL).Scan(&fence); err != nil {
			return 0, fmt.Errorf("allocate adapter grant fence: %w", err)
		}
	}
	if fence < 1 {
		return 0, errors.New("postgres allocated invalid grant fence")
	}
	return fence, nil
}
