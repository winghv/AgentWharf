//go:build unix

package main

import (
	"os"
	"os/signal"
	"sync"
	"syscall"

	ptylib "github.com/aymanbagabas/go-pty"
	"golang.org/x/term"
)

func watchTerminalResize(ptmx ptylib.Pty) func() {
	resized := make(chan os.Signal, 1)
	done := make(chan struct{})
	var once sync.Once
	signal.Notify(resized, syscall.SIGWINCH)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-resized:
				if width, height, err := term.GetSize(int(os.Stdin.Fd())); err == nil {
					_ = ptmx.Resize(width, height)
				}
			}
		}
	}()
	return func() {
		once.Do(func() {
			signal.Stop(resized)
			close(done)
		})
	}
}
