package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
)

func TestEncryptedACPSettingsLocalExecution(t *testing.T) {
	for _, failPublication := range []bool{false, true} {
		name := "completed"
		if failPublication {
			name = "publication_failure"
		}
		t.Run(name, func(t *testing.T) { testEncryptedACPSettingsLocalExecution(t, failPublication) })
	}
}

func testEncryptedACPSettingsLocalExecution(t *testing.T, failPublication bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runtime, err := openMachineE2EERuntime(ctx, filepath.Join(t.TempDir(), "endpoint"), "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.database.Close()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.executor.ReplaceGrants(ctx, "session", "key", 0, []e2ee.DeviceGrant{{DeviceID: "client", VerifyKey: public, Control: true}}); err != nil {
		t.Fatal(err)
	}
	key, err := runtime.vault.Load(ctx, "session", "key")
	if err != nil {
		t.Fatal(err)
	}
	defer clear(key)
	tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask")})
	state, ok := tracker.Current()
	if !ok {
		t.Fatal("missing settings")
	}
	payload, err := json.Marshal(map[string]any{"capability_fingerprint": state.Capability.Fingerprint, "model_id": "reasoning"})
	if err != nil {
		t.Fatal(err)
	}
	packet, err := e2ee.SealPacket(e2ee.Context{Scope: "command", Session: "session", KeyID: "key", Sender: "client", MessageID: "settings", Type: string(protocol.CommandSettingsChange)}, key, private, e2ee.PublicMetadata{}, payload)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(map[string]any{"version": 1, "scope": "command", "key_id": "key", "sender": "client", "message_id": "settings", "type": protocol.CommandSettingsChange, "packet": packet})
	if err != nil {
		t.Fatal(err)
	}
	command := &protocol.Command{SessionID: "session", CommandID: "settings", Type: protocol.CommandSettingsChange, Payload: wire}
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	responses := newACPResponseRouter()
	done := make(chan error, 1)
	go func() {
		request := readACPSettingsTestRequest(bufio.NewScanner(reader))
		var localState string
		if err := runtime.database.QueryRowContext(ctx, `SELECT state FROM e2ee_local_commands WHERE message='settings'`).Scan(&localState); err != nil {
			done <- err
			return
		}
		if localState != "claimed" {
			done <- io.ErrUnexpectedEOF
			return
		}
		responses.Deliver(testACPSettingsResponse(request["id"], "reasoning", "ask"), 1)
		done <- nil
	}()
	nextID := int64(1)
	var mu sync.Mutex
	var acks []protocol.AckStatus
	events := 0
	write := func(frame protocol.Frame) error {
		switch typed := frame.(type) {
		case *protocol.Event:
			events++
			if failPublication {
				return io.ErrClosedPipe
			}
		case *protocol.CommandAck:
			var localState string
			if err := runtime.database.QueryRowContext(ctx, `SELECT state FROM e2ee_local_commands WHERE message='settings'`).Scan(&localState); err != nil {
				return err
			}
			if localState != "completed" {
				return io.ErrUnexpectedEOF
			}
			acks = append(acks, typed.Status)
		}
		return nil
	}
	for i := 0; i < 2; i++ {
		err := deliverEncryptedACPSettings(ctx, wrapConfig{SessionID: "session", e2eeRuntime: runtime}, command, writer, "provider", &nextID, responses, tracker, &mu, write)
		if (err != nil) != failPublication {
			t.Fatalf("delivery error: %v", err)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if failPublication {
		var localState string
		if err := runtime.database.QueryRowContext(ctx, `SELECT state FROM e2ee_local_commands WHERE message='settings'`).Scan(&localState); err != nil || localState != "outcome_unknown" {
			t.Fatalf("ambiguous state: %s %v", localState, err)
		}
		if nextID != 2 || events != 1 || len(acks) != 0 {
			t.Fatalf("ambiguous delivery repeated or acknowledged: %d %d %v", nextID, events, acks)
		}
		return
	}
	if nextID != 2 || events != 2 || len(acks) != 2 || acks[0] != protocol.AckAccepted || acks[1] != protocol.AckDuplicate {
		t.Fatalf("delivery: requests=%d events=%d acks=%v", nextID, events, acks)
	}
}
