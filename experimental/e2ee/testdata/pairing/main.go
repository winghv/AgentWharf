// Synthetic interoperability harness. The offer is deliberately printed only
// in this test executable, never through a platform logger or API.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
)

func run() error {
	invitation, offer, err := e2ee.NewPairingInvitation("machine_fixture", time.Now())
	if err != nil {
		return err
	}
	var output any = offer
	if os.Getenv("E2EE_TEST_MACHINE_OFFER") == "1" {
		// Public deterministic fixture identity allows native trust-pin recovery
		// across verifier runs. Never use this harness as a real endpoint.
		wrapping := make([]byte, 32)
		wrapping[31] = 3
		identity := e2ee.LocalIdentity{Version: 1, Device: "machine_native_fixture", SigningSeed: make([]byte, 32), WrappingPrivate: wrapping}
		var local e2ee.MachineOffer
		invitation, local, err = e2ee.NewMachineOffer("machine_fixture", identity, time.Now())
		if err != nil {
			return err
		}
		output = local
	}
	if err := json.NewEncoder(os.Stdout).Encode(output); err != nil {
		return err
	}
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 4096)
	if !scanner.Scan() {
		return e2ee.ErrInvalid
	}
	var request e2ee.WrappedKey
	if json.Unmarshal(scanner.Bytes(), &request) != nil {
		return e2ee.ErrInvalid
	}
	var identity e2ee.PairingIdentity
	receipt, err := invitation.AcceptCommitted(request, time.Now(), func(value e2ee.PairingIdentity) error { identity = value; return nil })
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Identity e2ee.PairingIdentity `json:"identity"`
		Receipt  e2ee.PairingReceipt  `json:"receipt"`
	}{identity, receipt})
}
func main() {
	if run() != nil {
		fmt.Fprintln(os.Stderr, "pairing interop rejected")
		os.Exit(1)
	}
}
