package buildinfo

import "testing"

func TestModuleName(t *testing.T) {
	if ModuleName != "agentwharf" {
		t.Fatalf("ModuleName = %q, want %q", ModuleName, "agentwharf")
	}
}
