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

func TestProviderSettingsProbeUsesShortACPSessionWithoutPrompt(t *testing.T) {
	dir := t.TempDir()
	trace := filepath.Join(dir, "trace")
	bridge := filepath.Join(dir, "claude-agent-acp")
	script := `#!/bin/sh
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$PROBE_TRACE"
  case "$line" in
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{}}' ;;
    *'"id":2'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"probe-session","configOptions":[{"id":"model","type":"select","category":"model","currentValue":"model-a","options":[{"value":"model-a","name":"Model A"},{"value":"model-b","name":"Model B"}]},{"id":"mode","type":"select","category":"mode","currentValue":"ask","options":[{"value":"ask","name":"Ask"},{"value":"auto","name":"Auto"}]},{"id":"reasoning_effort","type":"select","category":"thought_level","currentValue":"high","options":[{"value":"high","name":"High"}]}]}}' ;;
    *'"id":3'*) printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'; exit 0 ;;
  esac
done
`
	if err := os.WriteFile(bridge, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("PROBE_TRACE", trace)
	var output strings.Builder
	if err := runProviderSettingsProbe(context.Background(), []string{"--provider", "claude-code"}, &output); err != nil {
		t.Fatal(err)
	}
	var capability protocol.SettingsCapabilityPayload
	if err := json.Unmarshal([]byte(output.String()), &capability); err != nil {
		t.Fatalf("decode probe output: %v; output=%q", err, output.String())
	}
	if capability.EffectiveModelID != "model-a" || len(capability.Models) != 2 {
		t.Fatalf("unexpected capability: %+v", capability)
	}
	frames, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	text := string(frames)
	for _, method := range []string{`"method":"initialize"`, `"method":"session/new"`, `"method":"session/close"`} {
		if !strings.Contains(text, method) {
			t.Errorf("probe did not send %s: %s", method, text)
		}
	}
	if strings.Contains(text, `"method":"session/prompt"`) || strings.Contains(text, `"prompt"`) {
		t.Fatalf("probe sent user instruction data: %s", text)
	}
}

func TestProviderSettingsProbeRejectsUnknownProvider(t *testing.T) {
	if err := runProviderSettingsProbe(context.Background(), []string{"--provider", "unknown"}, &strings.Builder{}); err == nil {
		t.Fatal("unknown provider accepted")
	}
}
