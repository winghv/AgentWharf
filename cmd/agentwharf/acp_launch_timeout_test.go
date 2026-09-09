package main

import (
	"bufio"
	"context"
	"io"
	"testing"
	"time"
)

func TestRequiredLaunchCancellationInterruptsSilentProvider(t *testing.T) {
	tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask")})
	inputReader, inputWriter := io.Pipe()
	outputReader, outputWriter := io.Pipe()
	defer inputReader.Close()
	defer inputWriter.Close()
	defer outputReader.Close()
	defer outputWriter.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sent := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(inputReader)
		if scanner.Scan() {
			close(sent)
		}
	}()
	done := make(chan error, 1)
	go func() {
		done <- applyRequiredACPLaunchSettingsWithPipes(ctx, tracker, "session", inputWriter, outputReader, bufio.NewScanner(outputReader), wrapLaunchSettings{ModelID: "reasoning"})
	}()
	select {
	case <-sent:
	case <-time.After(2 * time.Second):
		t.Fatal("provider never received setting")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("silent provider accepted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not unblock provider read")
	}
}
