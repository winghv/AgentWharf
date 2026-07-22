//go:build !linux

package core

import (
	"fmt"
	"os/exec"
)

func applyProcessCredential(_ *exec.Cmd, credential *ProcessCredential) error {
	if credential != nil {
		return fmt.Errorf("provider credential drop requires linux")
	}
	return nil
}
