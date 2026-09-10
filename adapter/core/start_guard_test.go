package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProcessSupervisorGuardsEveryActualChildStart(t *testing.T) {
	runner := newFakeProcessRunner()
	checks := 0
	supervisor, err := newProcessSupervisor(ProcessConfig{Command: ProcessCommand{Path: "provider"}, MaxRestarts: 2, Backoff: time.Millisecond, GracePeriod: 20 * time.Millisecond, StartGuard: func(_ context.Context, start func() error) error {
		checks++
		if checks == 2 {
			return errors.New("local grant revoked")
		}
		if runner.startCount() != 0 {
			t.Fatal("child existed before start guard")
		}
		return start()
	}}, runner)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(context.Background()) }()
	waitEvent(t, supervisor.Events(), ProcessEventStarted)
	runner.handle(0).finish(errors.New("crashed"))
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("revoked child restart succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("guard rejection did not stop supervisor")
	}
	if checks != 2 || runner.startCount() != 1 {
		t.Fatal("actual process creation bypassed guard")
	}
}
