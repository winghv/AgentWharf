//go:build !darwin && !linux

package core

import "os"

func pathOwnerUID(os.FileInfo) (uint32, bool) { return 0, false }
