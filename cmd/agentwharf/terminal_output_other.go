//go:build !windows

package main

func enableOfficialTerminalOutput() (func(), error) {
	return func() {}, nil
}
