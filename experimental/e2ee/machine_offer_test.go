package e2ee

import (
	"testing"
	"time"
)

func TestMachineOfferBindsLongTermIdentity(t *testing.T) {
	now := time.Now()
	identity, err := NewLocalIdentity()
	if err != nil {
		t.Fatal(err)
	}
	_, offer, err := NewMachineOffer("machine", identity, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyMachineOffer(offer, now); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*MachineOffer){
		func(o *MachineOffer) { o.Identity.Device = "other" },
		func(o *MachineOffer) { o.Offer.Machine = "other" },
		func(o *MachineOffer) { o.Offer.ExpiresAt++ },
		func(o *MachineOffer) { o.Identity.WrappingKey = o.Offer.PublicKey },
		func(o *MachineOffer) { o.Proof += "=" },
	} {
		altered := offer
		change(&altered)
		if VerifyMachineOffer(altered, now) == nil {
			t.Fatal("accepted substituted local offer")
		}
	}
	if VerifyMachineOffer(offer, now.Add(6*time.Minute)) == nil {
		t.Fatal("accepted expired offer")
	}
}
