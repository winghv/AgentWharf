//go:build linux

package core

import (
	"os/exec"
	"testing"
)

func TestApplyProcessCredentialEnforcesNonRootIdentity(t *testing.T) {
	command := exec.Command("true")
	if err := applyProcessCredential(command, nil); err != nil || command.SysProcAttr != nil {
		t.Fatalf("nil credential: error = %v, attr = %+v", err, command.SysProcAttr)
	}
	if err := applyProcessCredential(command, &ProcessCredential{UID: 0, GID: 10}); err == nil {
		t.Fatal("root UID credential was accepted")
	}
	if err := applyProcessCredential(command, &ProcessCredential{UID: 10, GID: 0}); err == nil {
		t.Fatal("root GID credential was accepted")
	}
	if err := applyProcessCredential(command, &ProcessCredential{UID: 1000, GID: 1000}); err != nil {
		t.Fatalf("non-root credential error = %v", err)
	}
	if command.SysProcAttr == nil || command.SysProcAttr.Credential == nil ||
		command.SysProcAttr.Credential.Uid != 1000 || command.SysProcAttr.Credential.Gid != 1000 {
		t.Fatalf("applied credential attr = %+v", command.SysProcAttr)
	}
}
