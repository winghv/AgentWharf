package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"testing"
	"time"
)

func TestRequiredLaunchLeavesPipesUsableAfterSuccess(t *testing.T) {
	tracker := newACPSettingsTracker(map[string]any{"configOptions": testACPConfigOptions("balanced", "ask")})
	inputReader, inputWriter := io.Pipe()
	outputReader, outputWriter := io.Pipe()
	defer inputReader.Close()
	defer inputWriter.Close()
	defer outputReader.Close()
	defer outputWriter.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	providerDone := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(inputReader)
		request := readACPSettingsTestRequest(scanner)
		if _, err := fmt.Fprintln(outputWriter, string(testACPSettingsResponse(request["id"], "reasoning", "ask"))); err != nil {
			providerDone <- err
			return
		}
		if !scanner.Scan() || scanner.Text() != "next-command" {
			providerDone <- io.ErrUnexpectedEOF
			return
		}
		_, err := fmt.Fprintln(outputWriter, "next-output")
		providerDone <- err
	}()
	scanner := bufio.NewScanner(outputReader)
	if err := applyRequiredACPLaunchSettingsWithPipes(ctx, tracker, "session", inputWriter, outputReader, scanner, wrapLaunchSettings{ModelID: "reasoning"}); err != nil {
		t.Fatal(err)
	}
	// Cancel the operation's parent after successful application. Its removed
	// callback must not destroy pipes now owned by the ongoing session.
	cancel()
	done := make(chan error, 1)
	go func() {
		if _, err := fmt.Fprintln(inputWriter, "next-command"); err != nil {
			done <- err
			return
		}
		if !scanner.Scan() || scanner.Text() != "next-output" {
			done <- io.ErrUnexpectedEOF
			return
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("successful launch closed session pipes", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pipes blocked after successful launch")
	}
	if err := <-providerDone; err != nil {
		t.Fatal(err)
	}
}
