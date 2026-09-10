//go:build linux || darwin

package e2ee

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestIdentityPersistsAndSerializesConcurrentCreation(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "keys")
	var wg sync.WaitGroup
	identities := make(chan LocalIdentity, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			identity, err := LoadOrCreateIdentity(context.Background(), directory)
			if err != nil {
				t.Error(err)
				return
			}
			identities <- identity
		}()
	}
	wg.Wait()
	close(identities)
	var first LocalIdentity
	count := 0
	for identity := range identities {
		if count == 0 {
			first = identity
		} else if !reflect.DeepEqual(first, identity) {
			t.Error("concurrent creation changed identity")
		}
		count++
	}
	if count != 12 {
		t.Fatal("missing identity results")
	}
	reloaded, err := LoadOrCreateIdentity(context.Background(), directory)
	if err != nil || !reflect.DeepEqual(first, reloaded) {
		t.Fatal("identity not persistent", err)
	}
	public, err := first.Public()
	if err != nil || public.Device != first.Device {
		t.Fatal("public identity invalid", err)
	}
	if err := os.WriteFile(filepath.Join(directory, "identity.json"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreateIdentity(context.Background(), directory); err == nil {
		t.Fatal("corrupt identity silently regenerated")
	}
}

func TestIdentityProcessHelper(t *testing.T) {
	if os.Getenv("WHARF_TEST_IDENTITY_HELPER") != "1" {
		return
	}
	identity, err := LoadOrCreateIdentity(context.Background(), os.Getenv("WHARF_TEST_IDENTITY_DIRECTORY"))
	if err != nil {
		os.Exit(2)
	}
	public, err := identity.Public()
	if err != nil {
		os.Exit(2)
	}
	if json.NewEncoder(os.Stdout).Encode(public) != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

func TestIdentitySerializesSeparateProcesses(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "keys")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const count = 4
	commands := make([]*exec.Cmd, count)
	outputs := make([]bytes.Buffer, count)
	for i := range commands {
		commands[i] = exec.CommandContext(ctx, os.Args[0], "-test.run=^TestIdentityProcessHelper$")
		commands[i].Env = append(os.Environ(), "WHARF_TEST_IDENTITY_HELPER=1", "WHARF_TEST_IDENTITY_DIRECTORY="+directory)
		commands[i].Stdout = &outputs[i]
		if err := commands[i].Start(); err != nil {
			t.Fatal(err)
		}
	}
	var expected PairingIdentity
	for i, command := range commands {
		if err := command.Wait(); err != nil {
			t.Fatal("identity process failed", err)
		}
		var public PairingIdentity
		if json.Unmarshal(outputs[i].Bytes(), &public) != nil {
			t.Fatal("invalid public identity response")
		}
		if i == 0 {
			expected = public
		} else if public != expected {
			t.Fatal("separate processes generated different identities")
		}
	}
}

func TestIdentityRefusesUnsafeFilesystemObjects(t *testing.T) {
	for _, kind := range []string{"directory_symlink", "file_symlink", "hardlink", "permissions", "pending", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			directory := filepath.Join(root, "keys")
			if err := os.Mkdir(directory, 0700); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "directory_symlink":
				link := filepath.Join(root, "linked")
				if err := os.Symlink(directory, link); err != nil {
					t.Fatal(err)
				}
				directory = link
			case "file_symlink", "hardlink":
				target := filepath.Join(root, "target")
				if err := os.WriteFile(target, []byte("synthetic"), 0600); err != nil {
					t.Fatal(err)
				}
				var err error
				if kind == "file_symlink" {
					err = os.Symlink(target, filepath.Join(directory, "identity.json"))
				} else {
					err = os.Link(target, filepath.Join(directory, "identity.json"))
				}
				if err != nil {
					t.Fatal(err)
				}
			case "permissions":
				if err := os.Chmod(directory, 0755); err != nil {
					t.Fatal(err)
				}
			case "pending":
				if err := os.WriteFile(filepath.Join(directory, "identity.pending"), []byte("interrupted"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			if kind == "cancelled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if _, err := LoadOrCreateIdentity(ctx, directory); err == nil {
				t.Fatal("accepted unsafe state")
			}
		})
	}
}
