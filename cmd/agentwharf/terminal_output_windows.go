//go:build windows

package main

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

// enableOfficialTerminalOutput makes the parent console interpret the VT stream
// emitted by ConPTY. Redirected stdout is not a console and needs no mode change.
func enableOfficialTerminalOutput() (func(), error) {
	handle := windows.Handle(os.Stdout.Fd())
	var original uint32
	if err := windows.GetConsoleMode(handle, &original); err != nil {
		return func() {}, nil
	}
	if err := windows.SetConsoleMode(handle, original|windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING); err != nil {
		return nil, fmt.Errorf("enable virtual terminal output: %w", err)
	}
	return func() { _ = windows.SetConsoleMode(handle, original) }, nil
}
