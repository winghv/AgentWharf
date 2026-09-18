//go:build windows

package main

import "testing"

func TestOfficialTerminalSizeUsesOutputHandle(t *testing.T) {
	const outputFD = uintptr(22)
	if got := officialTerminalSizeFD(outputFD); got != int(outputFD) {
		t.Fatalf("officialTerminalSizeFD() = %d, want output handle %d", got, outputFD)
	}
}
