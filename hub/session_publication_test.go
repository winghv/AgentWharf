package hub

import (
	"context"
	"testing"
	"time"
)

func TestSessionPublicationCancelledWaiterRelaysFIFO(t *testing.T) {
	var gates sessionPublicationGates
	firstRelease, err := gates.acquire(context.Background(), "ses_1")
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		_, err := gates.acquire(cancelled, "ses_1")
		secondDone <- err
	}()
	cancel()
	select {
	case err := <-secondDone:
		if err == nil {
			t.Fatal("cancelled waiter acquired publication gate")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled waiter did not return")
	}

	thirdAcquired := make(chan func(), 1)
	go func() {
		release, err := gates.acquire(context.Background(), "ses_1")
		if err != nil {
			t.Errorf("acquire third: %v", err)
			return
		}
		thirdAcquired <- release
	}()
	select {
	case <-thirdAcquired:
		t.Fatal("third waiter bypassed first publication")
	case <-time.After(30 * time.Millisecond):
	}

	firstRelease()
	select {
	case release := <-thirdAcquired:
		release()
	case <-time.After(time.Second):
		t.Fatal("cancelled waiter did not relay publication gate")
	}
}
