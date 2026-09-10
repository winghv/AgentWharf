// Isolated synthetic interoperability harness; never use real keys or content.
package main

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/winghv/agentwharf/experimental/e2ee"
	"github.com/winghv/agentwharf/protocol"
	_ "modernc.org/sqlite"
)

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir, err := os.MkdirTemp("", "e2ee-wire-interop-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	identity, err := e2ee.NewLocalIdentity()
	if err != nil {
		return err
	}
	open := func() (*sql.DB, *e2ee.CommandExecutor, *e2ee.SessionKeyVault, error) {
		db, err := sql.Open("sqlite", filepath.Join(dir, "endpoint.db"))
		if err != nil {
			return nil, nil, nil, err
		}
		db.SetMaxOpenConns(1)
		if _, err = db.ExecContext(ctx, `PRAGMA synchronous=FULL; PRAGMA busy_timeout=2000;`); err != nil {
			db.Close()
			return nil, nil, nil, err
		}
		journal, err := e2ee.NewCommandJournal(ctx, db)
		if err != nil {
			db.Close()
			return nil, nil, nil, err
		}
		vault, err := e2ee.NewSessionKeyVault(ctx, db, identity)
		if err != nil {
			db.Close()
			return nil, nil, nil, err
		}
		executor, err := e2ee.NewCommandExecutor(journal, vault)
		if err != nil {
			db.Close()
			return nil, nil, nil, err
		}
		return db, executor, vault, nil
	}
	db, executor, vault, err := open()
	if err != nil {
		return err
	}
	defer func() {
		if db != nil {
			db.Close()
		}
	}()
	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i + 32)
	}
	public := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	if err = executor.ReplaceGrants(ctx, "session", "key", 0, []e2ee.DeviceGrant{{DeviceID: "client", VerifyKey: public, Control: true}}); err != nil {
		return err
	}
	key, err := vault.Load(ctx, "session", "key")
	if err != nil {
		return err
	}
	defer clear(key)
	// This ephemeral key is used only by this isolated synthetic client.
	if err = json.NewEncoder(os.Stdout).Encode(map[string]string{"key": base64.RawURLEncoding.EncodeToString(key)}); err != nil {
		return err
	}
	var request struct{ Wire json.RawMessage }
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 64*1024))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&request); err != nil {
		return err
	}
	if _, err = protocol.DecodeEncryptedPacketCarrier(request.Wire, "command", "session.send", "command"); err != nil {
		return err
	}
	calls := 0
	deliver := func(_ context.Context, payload json.RawMessage) error {
		calls++
		var got struct {
			Content []struct {
				Kind string `json:"kind"`
				Text string `json:"text"`
			} `json:"content"`
		}
		if json.Unmarshal(payload, &got) != nil || len(got.Content) != 1 || got.Content[0].Text != "synthetic wire instruction" {
			return e2ee.ErrInvalid
		}
		return nil
	}
	for _, routing := range [][2]string{{"other-session", "command"}, {"session", "other-command"}} {
		if _, err = executor.ExecuteWire(ctx, routing[0], routing[1], "session.send", request.Wire, deliver); err == nil || calls != 0 {
			return e2ee.ErrInvalid
		}
	}
	admission, err := executor.ExecuteWire(ctx, "session", "command", "session.send", request.Wire, deliver)
	if err != nil || !admission.Execute || admission.State != "completed" || calls != 1 {
		return e2ee.ErrInvalid
	}
	if err = db.Close(); err != nil {
		return err
	}
	db, executor, _, err = open()
	if err != nil {
		return err
	}
	admission, err = executor.ExecuteWire(ctx, "session", "command", "session.send", request.Wire, deliver)
	if err != nil || admission.Execute || calls != 1 {
		return e2ee.ErrInvalid
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"delivered": calls, "restart_duplicate": true, "routing_substitution_rejected": true})
}
func main() {
	if run() != nil {
		fmt.Fprintln(os.Stderr, "command wire interoperability failed")
		os.Exit(1)
	}
}
