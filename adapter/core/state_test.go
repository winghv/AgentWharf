package core_test

import (
	"errors"
	"testing"

	"github.com/winghv/agentwharf/adapter/core"
)

func TestSessionStateMachineAllowsSpecTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		from core.SessionState
		to   core.SessionState
	}{
		{name: "starting to ready", from: core.StateStarting, to: core.StateReady},
		{name: "ready to busy", from: core.StateReady, to: core.StateBusy},
		{name: "busy to ready", from: core.StateBusy, to: core.StateReady},
		{name: "busy to waiting permission", from: core.StateBusy, to: core.StateWaitingPermission},
		{name: "waiting permission to busy", from: core.StateWaitingPermission, to: core.StateBusy},
		{name: "waiting permission to ready", from: core.StateWaitingPermission, to: core.StateReady},
		{name: "ready to recovering", from: core.StateReady, to: core.StateRecovering},
		{name: "busy to recovering", from: core.StateBusy, to: core.StateRecovering},
		{name: "recovering to ready", from: core.StateRecovering, to: core.StateReady},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sm, err := core.NewStateMachine(tt.from)
			if err != nil {
				t.Fatalf("NewStateMachine() error = %v", err)
			}
			transition, err := sm.Transition(tt.to, "")
			if err != nil {
				t.Fatalf("Transition(%s -> %s) error = %v", tt.from, tt.to, err)
			}
			if transition.From != tt.from || transition.To != tt.to || transition.Terminal() {
				t.Fatalf("transition = %+v", transition)
			}
			if sm.State() != tt.to {
				t.Fatalf("state = %q, want %q", sm.State(), tt.to)
			}
		})
	}
}

func TestSessionStateMachineAllowsAnyKnownStateToTerminal(t *testing.T) {
	t.Parallel()

	states := []core.SessionState{
		core.StateStarting,
		core.StateReady,
		core.StateBusy,
		core.StateWaitingPermission,
		core.StateRecovering,
	}
	for _, from := range states {
		from := from
		t.Run(string(from)+" to ended", func(t *testing.T) {
			t.Parallel()

			sm, err := core.NewStateMachine(from)
			if err != nil {
				t.Fatalf("NewStateMachine() error = %v", err)
			}
			transition, err := sm.End(core.EndReasonProviderExit)
			if err != nil {
				t.Fatalf("End() error = %v", err)
			}
			if transition.From != from || transition.To != core.StateEnded ||
				transition.Reason != string(core.EndReasonProviderExit) || !transition.Terminal() {
				t.Fatalf("transition = %+v", transition)
			}
			if sm.State() != core.StateEnded {
				t.Fatalf("state = %q, want ended", sm.State())
			}
		})

		t.Run(string(from)+" to error", func(t *testing.T) {
			t.Parallel()

			sm, err := core.NewStateMachine(from)
			if err != nil {
				t.Fatalf("NewStateMachine() error = %v", err)
			}
			transition, err := sm.Error("adapter_lost")
			if err != nil {
				t.Fatalf("Error() error = %v", err)
			}
			if transition.From != from || transition.To != core.StateError ||
				transition.Reason != "adapter_lost" || !transition.Terminal() {
				t.Fatalf("transition = %+v", transition)
			}
			if sm.State() != core.StateError {
				t.Fatalf("state = %q, want error", sm.State())
			}
		})
	}
}

func TestSessionStateMachineRejectsIllegalTransitionsAndKeepsState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		from core.SessionState
		to   core.SessionState
	}{
		{name: "starting cannot skip to busy", from: core.StateStarting, to: core.StateBusy},
		{name: "ready cannot wait permission", from: core.StateReady, to: core.StateWaitingPermission},
		{name: "recovering cannot go busy", from: core.StateRecovering, to: core.StateBusy},
		{name: "busy cannot go starting", from: core.StateBusy, to: core.StateStarting},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sm, err := core.NewStateMachine(tt.from)
			if err != nil {
				t.Fatalf("NewStateMachine() error = %v", err)
			}
			_, err = sm.Transition(tt.to, "")
			if !errors.Is(err, core.ErrInvalidTransition) {
				t.Fatalf("Transition() error = %v, want ErrInvalidTransition", err)
			}
			if sm.State() != tt.from {
				t.Fatalf("state changed to %q, want %q", sm.State(), tt.from)
			}
		})
	}
}

func TestSessionStateMachineRejectsUnknownStatesAndTerminalTransitions(t *testing.T) {
	t.Parallel()

	if _, err := core.NewStateMachine(core.SessionState("paused")); !errors.Is(err, core.ErrInvalidState) {
		t.Fatalf("NewStateMachine(paused) error = %v, want ErrInvalidState", err)
	}

	sm, err := core.NewStateMachine(core.StateReady)
	if err != nil {
		t.Fatalf("NewStateMachine() error = %v", err)
	}
	if _, err := sm.Transition(core.SessionState("paused"), ""); !errors.Is(err, core.ErrInvalidState) {
		t.Fatalf("Transition(paused) error = %v, want ErrInvalidState", err)
	}
	if _, err := sm.End(core.EndReasonUserStop); err != nil {
		t.Fatalf("End() error = %v", err)
	}
	if _, err := sm.Transition(core.StateReady, ""); !errors.Is(err, core.ErrTerminalState) {
		t.Fatalf("Transition(from terminal) error = %v, want ErrTerminalState", err)
	}
}

func TestEndRejectsUnknownReasonAndErrorRequiresReason(t *testing.T) {
	t.Parallel()

	sm, err := core.NewStateMachine(core.StateReady)
	if err != nil {
		t.Fatalf("NewStateMachine() error = %v", err)
	}
	if _, err := sm.End(core.EndReason("manual")); !errors.Is(err, core.ErrInvalidReason) {
		t.Fatalf("End(manual) error = %v, want ErrInvalidReason", err)
	}
	if _, err := sm.Error(""); !errors.Is(err, core.ErrInvalidReason) {
		t.Fatalf("Error(empty) error = %v, want ErrInvalidReason", err)
	}
	if sm.State() != core.StateReady {
		t.Fatalf("state = %q, want ready", sm.State())
	}
}
