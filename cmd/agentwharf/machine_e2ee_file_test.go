package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
)

func TestEncryptedFileReadClaimsReadsAndSealsResult(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "secret.txt"), []byte("private-file-canary"), 0600); err != nil {
		t.Fatal(err)
	}
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
	seal := func(id string, commandType protocol.CommandType, payload []byte) []byte {
		packet, err := e2ee.SealPacket(e2ee.Context{Scope: "command", Session: "session", Sender: "client", KeyID: "key", MessageID: id, Type: string(commandType)}, key, private, e2ee.PublicMetadata{}, payload)
		if err != nil {
			t.Fatal(err)
		}
		wire, _ := json.Marshal(map[string]any{"version": 1, "scope": "command", "key_id": "key", "sender": "client", "message_id": id, "type": commandType, "packet": packet})
		return wire
	}
	command := &protocol.Command{SessionID: "session", CommandID: "file-read", Type: protocol.CommandFileRead, Payload: seal("file-read", protocol.CommandFileRead, []byte(`{"path":"secret.txt"}`))}
	cfg := wrapConfig{SessionID: "session", WorkingDirectory: root, ProtocolVersion: protocol.ProtocolVersionV2, ContentMode: protocol.ContentModeRequired, e2eeRuntime: runtime}
	frames := make([]protocol.Frame, 0)
	connection := newHubConnection(cfg, nil, nil)
	write := func(frame protocol.Frame) error {
		if event, ok := frame.(*protocol.Event); ok {
			prepared, err := connection.prepareEvent(ctx, event)
			if err != nil {
				return err
			}
			frame = prepared
		}
		frames = append(frames, frame)
		return nil
	}
	if err := deliverEncryptedFileRead(ctx, cfg, command, write); err != nil {
		t.Fatal(err)
	}
	if len(frames) != 2 {
		t.Fatalf("frames=%d", len(frames))
	}
	event, ok := frames[0].(*protocol.Event)
	if !ok || event.Type != "session.file.result" || strings.Contains(string(event.Payload), "private-file-canary") {
		t.Fatal("file result leaked or missing")
	}
	carrier, err := protocol.DecodeEncryptedPacketCarrier(event.Payload, "event", "session.file.result", "")
	if err != nil {
		t.Fatal(err)
	}
	signing, err := base64.RawURLEncoding.DecodeString(runtime.public.SigningKey)
	if err != nil {
		t.Fatal(err)
	}
	packetJSON, _ := json.Marshal(carrier.Packet)
	var endpointPacket e2ee.ContentPacket
	if err := json.Unmarshal(packetJSON, &endpointPacket); err != nil {
		t.Fatal(err)
	}
	decoded, err := e2ee.OpenPacket(e2ee.Context{Scope: "event", Session: "session", Sender: runtime.public.Device, KeyID: carrier.KeyID, MessageID: carrier.MessageID, Type: "session.file.result"}, key, ed25519.PublicKey(signing), endpointPacket)
	if err != nil || !strings.Contains(string(decoded), "private-file-canary") {
		t.Fatalf("decrypt result: %v", err)
	}
	clear(decoded)
	var state string
	if err := runtime.database.QueryRowContext(ctx, `SELECT state FROM e2ee_local_commands WHERE session='session' AND message='file-read'`).Scan(&state); err != nil || state != "completed" {
		t.Fatalf("journal state=%s err=%v", state, err)
	}
	t.Run("bounded directory listing", func(t *testing.T) {
		for _, name := range []string{"empty", "large"} {
			if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i <= maxEncryptedFileListEntries; i++ {
			if err := os.WriteFile(filepath.Join(root, "large", fmt.Sprintf("file-%03d", i)), nil, 0600); err != nil {
				t.Fatal(err)
			}
		}
		for _, tc := range []struct {
			path       string
			reasonCode string
		}{{".", ""}, {"empty", ""}, {"large", "file_unavailable"}, {"secret.txt", "file_unavailable"}, {"../", "invalid_file_request"}} {
			frames = nil
			id := fmt.Sprintf("list-%d", len(tc.path)) + strings.ReplaceAll(tc.path, "/", "_")
			payload, _ := json.Marshal(map[string]string{"path": tc.path})
			command := &protocol.Command{SessionID: "session", CommandID: id, Type: protocol.CommandFileList, Payload: seal(id, protocol.CommandFileList, payload)}
			err := deliverEncryptedFileList(ctx, cfg, command, write)
			if tc.reasonCode != "" {
				if err != nil || len(frames) != 1 {
					t.Fatalf("directory %q rejection: frames=%d err=%v", tc.path, len(frames), err)
				}
				ack, ok := frames[0].(*protocol.CommandAck)
				if !ok || ack.Status != protocol.AckRejected || ack.Reason != tc.reasonCode {
					t.Fatalf("directory %q rejection = %#v", tc.path, frames[0])
				}
				var count int
				if err := runtime.database.QueryRowContext(ctx, `SELECT count(*) FROM e2ee_local_commands WHERE session='session' AND message=?`, id).Scan(&count); err != nil || count != 0 {
					t.Fatalf("rejected directory command was journaled: count=%d err=%v", count, err)
				}
				continue
			}
			if err != nil || len(frames) != 2 {
				t.Fatalf("directory %q: frames=%d err=%v", tc.path, len(frames), err)
			}
			event := frames[0].(*protocol.Event)
			carrier, err := protocol.DecodeEncryptedPacketCarrier(event.Payload, "event", "session.file.result", "")
			if err != nil {
				t.Fatal(err)
			}
			packetJSON, _ := json.Marshal(carrier.Packet)
			var packet e2ee.ContentPacket
			if err := json.Unmarshal(packetJSON, &packet); err != nil {
				t.Fatal(err)
			}
			decoded, err := e2ee.OpenPacket(e2ee.Context{Scope: "event", Session: "session", Sender: runtime.public.Device, KeyID: carrier.KeyID, MessageID: carrier.MessageID, Type: "session.file.result"}, key, ed25519.PublicKey(signing), packet)
			if err != nil {
				t.Fatal(err)
			}
			var result struct {
				Path  string
				Nodes []json.RawMessage
			}
			if err := json.Unmarshal(decoded, &result); err != nil || result.Path != tc.path {
				t.Fatal("invalid directory result", err)
			}
			if tc.path == "empty" && len(result.Nodes) != 0 {
				t.Fatal("nonempty directory result")
			}
			if tc.path == "." && (len(result.Nodes) != 3 || strings.Contains(string(event.Payload), "secret.txt")) {
				t.Fatal("listing missing or leaked")
			}
			frames = nil
			if err := deliverEncryptedFileList(ctx, cfg, command, write); err != nil || len(frames) != 1 || frames[0].(*protocol.CommandAck).Status != protocol.AckDuplicate {
				t.Fatal("listing redelivery executed", err)
			}
		}
	})
	if err := os.Symlink("secret.txt", filepath.Join(root, "secret-link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("empty", filepath.Join(root, "directory-link")); err != nil {
		t.Fatal(err)
	}
	frames = nil
	linkListID := "list-directory-link"
	linkListPayload := seal(linkListID, protocol.CommandFileList, []byte(`{"path":"directory-link"}`))
	if err := deliverEncryptedFileList(ctx, cfg, &protocol.Command{SessionID: "session", CommandID: linkListID, Type: protocol.CommandFileList, Payload: linkListPayload}, write); err != nil || len(frames) != 1 || frames[0].(*protocol.CommandAck).Status != protocol.AckRejected || frames[0].(*protocol.CommandAck).Reason != "file_unavailable" {
		t.Fatal("directory symlink rejection failed", err)
	}
	for _, path := range []string{"../secret.txt", "dir/../secret.txt", "name..txt", "/etc/passwd", "secret-link", "missing.txt"} {
		frames = frames[:0]
		bad := &protocol.Command{SessionID: "session", CommandID: "bad-" + strings.ReplaceAll(path, "/", "_"), Type: protocol.CommandFileRead, Payload: seal("bad-"+strings.ReplaceAll(path, "/", "_"), protocol.CommandFileRead, []byte(`{"path":"`+path+`"}`))}
		if err := deliverEncryptedFileRead(ctx, cfg, bad, write); err != nil {
			t.Fatalf("file rejection terminated delivery: %s: %v", path, err)
		}
		if len(frames) != 1 {
			t.Fatal("rejected file produced unexpected output")
		}
		ack := frames[0].(*protocol.CommandAck)
		wantReason := "file_unavailable"
		if path == "../secret.txt" || path == "dir/../secret.txt" || path == "/etc/passwd" {
			wantReason = "invalid_file_request"
		}
		if ack.Status != protocol.AckRejected || ack.Reason != wantReason {
			t.Fatalf("file %q rejection = %#v", path, ack)
		}
	}
	frames = nil
	followUp := &protocol.Command{SessionID: "session", CommandID: "file-read-after-rejection", Type: protocol.CommandFileRead, Payload: seal("file-read-after-rejection", protocol.CommandFileRead, []byte(`{"path":"secret.txt"}`))}
	if err := deliverEncryptedFileRead(ctx, cfg, followUp, write); err != nil || len(frames) != 2 || frames[1].(*protocol.CommandAck).Status != protocol.AckAccepted {
		t.Fatalf("valid file command after rejection failed: frames=%d err=%v", len(frames), err)
	}
}
