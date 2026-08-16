//go:build windows

package main

import "os"

func watchTerminalResize(ptmx *os.File) func() {
	return func() {}
}
