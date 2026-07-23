package hub

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAdapterHelloAuthorityBudgetReleasesAdmissionForReplacement(t *testing.T) {
	handler := &webSocketHandler{adapterAdmissionLocks: make(map[string]chan struct{})}
	writeStarted := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, unlock := handler.lockAdapterAdmission("ses_1")
		defer unlock()
		firstDone <- withAdapterAuthorityBudget(context.Background(), func(ctx context.Context) error {
			close(writeStarted)
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	select {
	case <-writeStarted:
	case <-time.After(time.Second):
		t.Fatal("blocked hello.ack write did not start")
	}

	replacementDone := make(chan struct{})
	go func() {
		_, unlock := handler.lockAdapterAdmission("ses_1")
		unlock()
		close(replacementDone)
	}()
	select {
	case <-replacementDone:
	case <-time.After(2 * adapterAuthorityPollInterval):
		t.Fatal("blocked hello.ack write indefinitely held replacement admission")
	}
	if err := <-firstDone; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded hello.ack write error = %v, want deadline exceeded", err)
	}
}
