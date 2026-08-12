package buildinfo

import "testing"

func TestModuleName(t *testing.T) {
	if ModuleName != "agentwharf" {
		t.Fatalf("ModuleName = %q, want %q", ModuleName, "agentwharf")
	}
}

func TestDevelopmentVersion(t *testing.T) {
	if Version == "" {
		t.Fatal("Version must not be empty")
	}
}
