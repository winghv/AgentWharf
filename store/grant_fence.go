package store

import "context"

// AdapterGrantFenceStore allocates opaque, globally monotonic fences for
// bootstrap-to-target attach grants. Callers must not derive or compare fences
// from wall-clock time; AdapterConnectionStore owns admission comparisons.
type AdapterGrantFenceStore interface {
	AllocateAdapterGrantFence(ctx context.Context) (int64, error)
}
