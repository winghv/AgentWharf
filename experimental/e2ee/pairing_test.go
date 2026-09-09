package e2ee

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPairingInvitationAuthenticationAndOneTimeUse(t *testing.T) {
	now := time.Now()
	invitation, offer, err := NewPairingInvitation("machine", now)
	if err != nil {
		t.Fatal(err)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, wrapPublic, err := GenerateWrappingKey()
	if err != nil {
		t.Fatal(err)
	}
	identity := PairingIdentity{"device", base64.RawURLEncoding.EncodeToString(public), base64.RawURLEncoding.EncodeToString(wrapPublic)}
	wrong := offer
	wrong.Secret = base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	attack, err := EncryptPairingIdentity(wrong, identity, private, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := invitation.Accept(attack, now); err == nil {
		t.Fatal("accepted platform without local secret")
	}
	wrong = offer
	wrong.Machine = "other"
	attack, err = EncryptPairingIdentity(wrong, identity, private, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := invitation.Accept(attack, now); err == nil {
		t.Fatal("accepted wrong machine binding")
	}
	request, err := EncryptPairingIdentity(offer, identity, private, now)
	if err != nil {
		t.Fatal(err)
	}
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := invitation.Accept(request, now)
			if err == nil {
				if got != identity {
					t.Error("identity mismatch")
				}
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatal("one-time admission", accepted.Load())
	}
	expired, oldOffer, err := NewPairingInvitation("machine", now)
	if err != nil {
		t.Fatal(err)
	}
	oldRequest, err := EncryptPairingIdentity(oldOffer, identity, private, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := expired.Accept(oldRequest, now.Add(5*time.Minute)); err == nil {
		t.Fatal("accepted expired offer")
	}
}
