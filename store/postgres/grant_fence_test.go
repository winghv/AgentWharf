package postgres_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
	"github.com/winghv/agentwharf/store/postgres"
)

func TestGrantFenceContract(t *testing.T) {
	ctx := context.Background()
	harness := newPostgresConnectionHarness(t)
	allocator := postgres.New(harness.pool)
	first, err := allocator.AllocateAdapterGrantFence(ctx)
	if err != nil || first < 1 {
		t.Fatalf("first fence = %d, err = %v", first, err)
	}
	second, err := allocator.AllocateAdapterGrantFence(ctx)
	if err != nil || second <= first {
		t.Fatalf("second fence = %d, first = %d, err = %v", second, first, err)
	}

	const workers = 8
	fences := make(chan int64, workers)
	errs := make(chan error, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			fence, err := allocator.AllocateAdapterGrantFence(ctx)
			if err != nil {
				errs <- err
				return
			}
			fences <- fence
		}()
	}
	wait.Wait()
	close(fences)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent allocation: %v", err)
	}
	seen := map[int64]bool{first: true, second: true}
	for fence := range fences {
		if fence <= second || seen[fence] {
			t.Fatalf("concurrent fence = %d, second = %d, duplicate = %t", fence, second, seen[fence])
		}
		seen[fence] = true
	}

	if _, err := harness.InitializeAdapterConnection(ctx, store.AdapterConnectionInitialize{
		SessionID: "ses_connection", ActiveCredentialGeneration: 1, ActiveCredentialExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("initialize adapter connection: %v", err)
	}
	connection, err := harness.AcceptAdapterHello(ctx, "ses_connection", store.AdapterHello{CredentialGeneration: 1})
	if err != nil {
		t.Fatalf("accept adapter hello: %v", err)
	}
	grant, err := allocator.AllocateAdapterGrantFence(ctx)
	if err != nil || grant <= connection.AcceptedFence {
		t.Fatalf("grant fence = %d, accepted fence = %d, err = %v", grant, connection.AcceptedFence, err)
	}
}
