package e2ee

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"testing"
	"time"
)

func TestPairingReceiptRequiresCommittedEnrollment(t *testing.T) {
	now := time.Now()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, wrapping, err := GenerateWrappingKey()
	if err != nil {
		t.Fatal(err)
	}
	identity := PairingIdentity{"device", base64.RawURLEncoding.EncodeToString(public), base64.RawURLEncoding.EncodeToString(wrapping)}
	for _, failed := range []bool{false, true} {
		invitation, offer, err := NewPairingInvitation("machine", now)
		if err != nil {
			t.Fatal(err)
		}
		request, err := EncryptPairingIdentity(offer, identity, private, now)
		if err != nil {
			t.Fatal(err)
		}
		called := 0
		receipt, err := invitation.AcceptCommitted(request, now, func(got PairingIdentity) error {
			called++
			if got != identity {
				t.Fatal("wrong identity committed")
			}
			if failed {
				return errors.New("synthetic commit failure")
			}
			return nil
		})
		if called != 1 {
			t.Fatal("commit count", called)
		}
		if failed {
			if !errors.Is(err, ErrJournal) || receipt.Confirmation != "" {
				t.Fatal("receipt escaped failed commit", err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decode(receipt.Confirmation, 32, 32); err != nil {
				t.Fatal("invalid receipt")
			}
		}
		retried, retryErr := invitation.AcceptCommitted(request, now, func(PairingIdentity) error { called++; return nil })
		if called != 1 {
			t.Fatal("consumed invitation committed twice")
		}
		if failed {
			if retryErr == nil {
				t.Fatal("ambiguous commit was retried")
			}
		} else if retryErr != nil || retried != receipt {
			t.Fatal("lost response could not recover exact receipt", retryErr)
		}
		changed, err := EncryptPairingIdentity(offer, identity, private, now)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := invitation.AcceptCommitted(changed, now, func(PairingIdentity) error { t.Fatal("changed request executed"); return nil }); err == nil {
			t.Fatal("accepted a changed request on consumed invitation")
		}
		if _, err := invitation.AcceptCommitted(request, now.Add(5*time.Minute), func(PairingIdentity) error { t.Fatal("expired retry executed"); return nil }); err == nil {
			t.Fatal("replayed expired receipt")
		}
	}
}
