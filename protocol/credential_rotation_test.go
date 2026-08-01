package protocol

import "testing"

func TestCredentialRotationFramesRoundTrip(t *testing.T) {
	t.Parallel()

	request := &CredentialRotationRequest{RotationID: "rot_01"}
	credential := &CredentialRotationCredential{
		SessionID: "ses_01", RotationID: "rot_01", Generation: 2,
		Credential: "test-only-bearer", ExpiresAt: 1764937800000,
	}
	possession := &CredentialRotationPossession{
		SessionID: "ses_01", RotationID: "rot_01", Generation: 2, AcceptedEpoch: 7,
	}
	activation := &CredentialRotationActivation{
		RotationID: "rot_01", Generation: 2, ConnectionEpoch: 8, AcceptedFence: 11,
	}

	for _, frame := range []Frame{request, credential, possession, activation} {
		encoded, err := Encode(frame)
		if err != nil {
			t.Fatalf("Encode(%T): %v", frame, err)
		}
		decoded, err := Decode(encoded)
		if err != nil {
			t.Fatalf("Decode(%T): %v", frame, err)
		}
		if decoded.FrameName() != frame.FrameName() {
			t.Fatalf("frame name = %q, want %q", decoded.FrameName(), frame.FrameName())
		}
	}
}
