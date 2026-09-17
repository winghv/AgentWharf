//go:build !windows

package main

import ptylib "github.com/aymanbagabas/go-pty"

func applyOfficialProviderShellShim(*ptylib.Cmd) error { return nil }
