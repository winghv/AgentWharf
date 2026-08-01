//go:build linux

package core

import (
	"fmt"
	"os/exec"
	"syscall"
)

func applyProcessCredential(cmd *exec.Cmd, credential *ProcessCredential) error {
	if credential == nil {
		return nil
	}
	if credential.UID == 0 || credential.GID == 0 {
		return fmt.Errorf("provider credential must be non-root")
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: credential.UID, Gid: credential.GID}}
	return nil
}
