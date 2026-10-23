package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/winghv/agentwharf/protocol"
)

func writeDiscoveryFile(t *testing.T, root, relative, content string) {
	t.Helper()
	path := filepath.Join(root, relative)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoverProviderCommandsReadsProjectCommandsAndSkills(t *testing.T) {
	root := t.TempDir()
	writeDiscoveryFile(t, root, ".claude/commands/review.md", "---\ndescription: Review the current changes\nargument-hint: <path>\n---\n")
	writeDiscoveryFile(t, root, ".claude/commands/frontend/component.md", "Create a component.\n")
	writeDiscoveryFile(t, root, ".claude/skills/deploy/SKILL.md", "---\ndescription: Deploy the app\n---\n")
	commands := discoverProviderCommands("claude", root)
	byName := map[string]discoveredCommand{}
	for _, command := range commands {
		byName[command.Name] = command
	}
	if got := byName["review"]; got.Description != "Review the current changes" || got.ArgumentHint != "<path>" || got.Source != "project" {
		t.Fatalf("review command = %+v", got)
	}
	if got := byName["frontend:component"]; got.Description != "Create a component." {
		t.Fatalf("nested command = %+v", got)
	}
	if got := byName["deploy"]; got.Description != "Deploy the app" || got.Source != "skill" {
		t.Fatalf("skill command = %+v", got)
	}
}

func TestDiscoverProviderCommandsSkipsSymlinksAndOversizedFiles(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	writeDiscoveryFile(t, outside, "secret.md", "Do not expose this.\n")
	if err := os.MkdirAll(filepath.Join(root, ".claude", "commands"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret.md"), filepath.Join(root, ".claude", "commands", "leak.md")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	writeDiscoveryFile(t, root, ".claude/commands/big.md", strings.Repeat("x", maxDiscoveredCommandBytes+1))
	commands := discoverProviderCommands("claude", root)
	if len(commands) != 0 {
		t.Fatalf("commands = %+v, want none", commands)
	}
}

func TestPublishDiscoveredCommandsSendsOnlyNamesAndDescriptions(t *testing.T) {
	root := t.TempDir()
	writeDiscoveryFile(t, root, ".claude/commands/review.md", "---\ndescription: Review changes\n---\n")
	var sent []protocol.Frame
	cfg := wrapConfig{SessionID: "session_1", Provider: "claude", WorkingDirectory: root}
	err := publishDiscoveredCommands(context.Background(), cfg, nil, func(_ context.Context, frame protocol.Frame) error {
		sent = append(sent, frame)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 {
		t.Fatalf("sent = %d frames, want one", len(sent))
	}
	event, ok := sent[0].(*protocol.Event)
	if !ok || event.Type != "session.commands" {
		t.Fatalf("frame = %#v, want session.commands", sent[0])
	}
	if strings.Contains(string(event.Payload), root) || strings.Contains(string(event.Payload), ".claude") {
		t.Fatalf("payload leaked a host path: %s", event.Payload)
	}
	var payload struct {
		Commands []struct {
			Name        string `json:"name"`
			Description string `json:"description"`
		} `json:"commands"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Commands) != 1 || payload.Commands[0].Name != "review" || payload.Commands[0].Description != "Review changes" {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestDiscoverProviderCommandsRejectsSymlinkedDiscoveryRoot(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	writeDiscoveryFile(t, outside, ".claude/commands/secret.md", "Do not expose this.\n")
	if err := os.Symlink(filepath.Join(outside, ".claude"), filepath.Join(root, ".claude")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	commands := discoverProviderCommands("claude", root)
	if len(commands) != 0 {
		t.Fatalf("commands = %+v, want none from a symlinked root", commands)
	}
}
