//go:build unix

package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/creack/pty"
	"golang.org/x/term"
)

func watchTerminalResize(ptmx *os.File) func() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGWINCH)
	go func() {
		for range ch {
			if width, height, err := term.GetSize(int(os.Stdin.Fd())); err == nil {
				_ = pty.Setsize(ptmx, &pty.Winsize{Rows: uint16(height), Cols: uint16(width)})
			}
		}
	}()
	return func() { signal.Stop(ch) }
}
