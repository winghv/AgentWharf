package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestPiSettingsPolicyLoadsEnabledModelPatterns(t *testing.T) {
	dir := t.TempDir()
	settingsPath := filepath.Join(dir, "settings.json")
	data := []byte(`{"apiKeys":{"openai":"test-value"},"enabledModels":["anthropic/claude-sonnet-4","openai/gpt-5"]}`)
	if err := os.WriteFile(settingsPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(piAgentDirEnv, dir)

	policy := acpSettingsPolicyForProviderWithModelScope("pi")
	want := []string{"anthropic/claude-sonnet-4", "openai/gpt-5"}
	if !reflect.DeepEqual(policy.modelPatterns, want) {
		t.Fatalf("Pi model patterns = %v, want %v", policy.modelPatterns, want)
	}
	if got := acpSettingsPolicyForProvider("pi").modelPatterns; len(got) != 0 {
		t.Fatalf("base Pi settings policy unexpectedly loaded model scope: %v", got)
	}
}

func TestReadPiEnabledModelPatternsIgnoresInvalidOrEmptyEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	if err := os.WriteFile(path, []byte("\xef\xbb\xbf{\"enabledModels\":[\" openai/gpt-5 \",\"\",\"openai/gpt-5\",7]}"), 0o600); err != nil {
		t.Fatal(err)
	}
	got := readPiEnabledModelPatterns(path)
	if !reflect.DeepEqual(got, []string{"openai/gpt-5"}) {
		t.Fatalf("Pi model patterns = %v, want [openai/gpt-5]", got)
	}
}

func TestReadPiEnabledModelPatternsDoesNotFilterOnInvalidSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := readPiEnabledModelPatterns(path); len(got) != 0 {
		t.Fatalf("invalid settings produced model scope: %v", got)
	}
}
