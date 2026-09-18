//go:build windows

package main

import (
	"os"
	"sync"
	"time"

	ptylib "github.com/aymanbagabas/go-pty"
	"golang.org/x/term"
)

// Windows' GetConsoleScreenBufferInfo requires an output console handle;
// stdin is an input handle and cannot be used to query the viewport size.
func officialTerminalSize() (int, int, error) {
	return term.GetSize(officialTerminalSizeFD(os.Stdout.Fd()))
}

func officialTerminalSizeFD(outputFD uintptr) int {
	return int(outputFD)
}

// Windows does not deliver SIGWINCH. Poll the console viewport so ConPTY keeps
// the official provider TUI aligned with Windows Terminal, PowerShell, or cmd.
func watchTerminalResize(ptmx ptylib.Pty) func() {
	done := make(chan struct{})
	var once sync.Once
	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		lastWidth, lastHeight := 0, 0
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				width, height, err := officialTerminalSize()
				if err == nil && width > 0 && height > 0 && (width != lastWidth || height != lastHeight) {
					_ = ptmx.Resize(width, height)
					lastWidth, lastHeight = width, height
				}
			}
		}
	}()
	return func() { once.Do(func() { close(done) }) }
}
