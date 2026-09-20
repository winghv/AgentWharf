package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
)

func TestEncryptedFilePreflightRejectsRetiredEpochWithoutDisconnect(t *testing.T) {
	ctx := context.Background()
	runtime, err := openMachineE2EERuntime(ctx, filepath.Join(t.TempDir(), "endpoint"), "machine", "account")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.database.Close()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	grants := []e2ee.DeviceGrant{{DeviceID: "client", VerifyKey: public, Control: true}}
	if err := runtime.executor.ReplaceGrants(ctx, "session", "old-key", 0, grants); err != nil {
		t.Fatal(err)
	}
	oldKey, err := runtime.vault.Load(ctx, "session", "old-key")
	if err != nil {
		t.Fatal(err)
	}
	defer clear(oldKey)
	if err := runtime.executor.ReplaceGrants(ctx, "session", "new-key", 1, grants); err != nil {
		t.Fatal(err)
	}
	cfg := wrapConfig{SessionID: "session", WorkingDirectory: t.TempDir(), ContentMode: protocol.ContentModeRequired, e2eeRuntime: runtime}
	for _, kind := range []protocol.CommandType{protocol.CommandFileRead, protocol.CommandFileList} {
		t.Run(string(kind), func(t *testing.T) {
			id := "retired-" + string(kind)
			packet, err := e2ee.SealPacket(e2ee.Context{Scope: "command", Session: "session", Sender: "client", KeyID: "old-key", MessageID: id, Type: string(kind)}, oldKey, private, e2ee.PublicMetadata{}, json.RawMessage(`{"path":"."}`))
			if err != nil {
				t.Fatal(err)
			}
			wire, err := json.Marshal(e2ee.CommandWire{Version: 1, Scope: "command", KeyID: "old-key", Sender: "client", MessageID: id, Type: string(kind), Packet: packet})
			if err != nil {
				t.Fatal(err)
			}
			command := &protocol.Command{SessionID: "session", CommandID: id, Type: kind, Payload: wire}
			var frames []protocol.Frame
			write := func(frame protocol.Frame) error { frames = append(frames, frame); return nil }
			if kind == protocol.CommandFileRead {
				err = deliverEncryptedFileRead(ctx, cfg, command, write)
			} else {
				err = deliverEncryptedFileList(ctx, cfg, command, write)
			}
			if err != nil || len(frames) != 1 {
				t.Fatalf("retired epoch disconnected or produced content: frames=%d err=%v", len(frames), err)
			}
			ack, ok := frames[0].(*protocol.CommandAck)
			if !ok || ack.Status != protocol.AckRejected || ack.Reason != "epoch_stale" {
				t.Fatalf("unexpected acknowledgement: %#v", frames[0])
			}
			var count int
			if err := runtime.database.QueryRow(`SELECT count(*) FROM e2ee_local_commands WHERE session=? AND message=?`, "session", id).Scan(&count); err != nil || count != 0 {
				t.Fatalf("retired epoch was admitted: %d %v", count, err)
			}
		})
	}
}
