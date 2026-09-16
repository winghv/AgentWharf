package core

import (
	"bytes"
	"os"
	"testing"
)

func TestExactEnvironmentChild(t *testing.T) {
	if os.Getenv("LOCAL_ENV_PROBE") != "1" {
		return
	}
	if os.Getenv("CUSTOM_API_KEY") != "synthetic-local-key" || os.Getenv("AGENTWHARF_PARENT_SECRET") != "" {
		os.Exit(7)
	}
	os.Exit(0)
}

func TestExactEnvironmentDoesNotRefilterOrReinherit(t *testing.T) {
	t.Setenv("AGENTWHARF_PARENT_SECRET", "synthetic-internal-secret")
	var output bytes.Buffer
	handle, err := (execProcessRunner{}).Start(ProcessCommand{Path: os.Args[0], Args: []string{"-test.run=^TestExactEnvironmentChild$"}, Env: []string{"LOCAL_ENV_PROBE=1", "CUSTOM_API_KEY=synthetic-local-key"}, ExactEnv: true, Stdout: &output, Stderr: &output})
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Wait(); err != nil {
		t.Fatalf("child environment boundary: %v", err)
	}
}
