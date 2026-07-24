package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidProcessConfig = errors.New("invalid process supervisor config")
	ErrRestartLimitExceeded = errors.New("process restart limit exceeded")
)

const (
	defaultMaxRestarts = 3
	defaultBackoff     = 100 * time.Millisecond
	defaultGracePeriod = 15 * time.Second
)

type ProcessCommand struct {
	Path       string
	Args       []string
	Env        []string
	Dir        string
	Stdin      io.Reader
	Stdout     io.Writer
	Stderr     io.Writer
	Credential *ProcessCredential
}

// ProcessCredential is the fixed-entry-only identity for a Provider child.
type ProcessCredential struct{ UID, GID uint32 }

type ProcessConfig struct {
	Command        ProcessCommand
	MaxRestarts    int
	Backoff        time.Duration
	GracePeriod    time.Duration
	StartAdmission ProcessStartAdmission
}

// ProcessStartAdmission binds every individual Provider child start to a
// trusted lifecycle. Prepare runs before exec and confirmation runs as soon
// as a child exists. It deliberately receives no Provider configuration.
type ProcessStartAdmission interface {
	PrepareProcessStart(context.Context, int) error
	ConfirmProcessStarted(context.Context, int) error
}

// RecoveryStartHandle is an opaque reference returned by an admitted child
// start. It carries no authority and has no accessor; T42B owns any later
// GroupSupervisor recovery consumption.
type RecoveryStartHandle struct{ value string }

func NewRecoveryStartHandle(value string) (RecoveryStartHandle, error) {
	if len(value) < 32 || len(value) > 128 {
		return RecoveryStartHandle{}, errors.New("invalid recovery start handle")
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') {
			return RecoveryStartHandle{}, errors.New("invalid recovery start handle")
		}
	}
	return RecoveryStartHandle{value: value}, nil
}

type ProcessEventType string

const (
	ProcessEventStarted    ProcessEventType = "started"
	ProcessEventExited     ProcessEventType = "exited"
	ProcessEventRestarting ProcessEventType = "restarting"
	ProcessEventStopped    ProcessEventType = "stopped"
)

type ProcessEvent struct {
	Type    ProcessEventType
	Attempt int
	PID     int
	Err     error
	Killed  bool
}

type ProcessSupervisor struct {
	cfg    ProcessConfig
	runner processRunner
	events chan ProcessEvent

	mu       sync.Mutex
	current  *runningProcess
	stopping bool
}

type runningProcess struct {
	handle  processHandle
	attempt int

	done chan struct{}
	mu   sync.Mutex
	err  error
}

type processRunner interface {
	Start(ProcessCommand) (processHandle, error)
}

type processHandle interface {
	PID() int
	Wait() error
	Interrupt() error
	Kill() error
}

type execProcessRunner struct{}

type execProcessHandle struct {
	cmd *exec.Cmd
}

func NewProcessSupervisor(cfg ProcessConfig) (*ProcessSupervisor, error) {
	return newProcessSupervisor(cfg, execProcessRunner{})
}

func newProcessSupervisor(cfg ProcessConfig, runner processRunner) (*ProcessSupervisor, error) {
	if runner == nil {
		return nil, fmt.Errorf("%w: process runner is required", ErrInvalidProcessConfig)
	}
	normalized, err := normalizeProcessConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &ProcessSupervisor{
		cfg:    normalized,
		runner: runner,
		events: make(chan ProcessEvent, 128),
	}, nil
}

func (s *ProcessSupervisor) Events() <-chan ProcessEvent {
	return s.events
}

func (s *ProcessSupervisor) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	restarts := 0
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		process, err := s.start(ctx, attempt)
		if err != nil {
			return err
		}

		select {
		case <-process.done:
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.GracePeriod+time.Second)
			_ = s.Stop(shutdownCtx)
			cancel()
			return ctx.Err()
		}

		err = process.waitErr()
		s.clearCurrent(process)
		if s.isStopping() {
			return nil
		}

		s.emit(ProcessEvent{Type: ProcessEventExited, Attempt: process.attempt, PID: process.pid(), Err: err})
		if err == nil {
			return nil
		}
		if restarts >= s.cfg.MaxRestarts {
			return fmt.Errorf("%w: %w", ErrRestartLimitExceeded, err)
		}
		restarts++
		s.emit(ProcessEvent{Type: ProcessEventRestarting, Attempt: attempt + 1, Err: err})
		if err := sleepContext(ctx, s.cfg.Backoff); err != nil {
			return err
		}
	}
}

func (s *ProcessSupervisor) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	process := s.markStopping()
	if process == nil {
		return nil
	}

	killed := false
	if err := process.handle.Interrupt(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("interrupt provider process: %w", err)
	}

	timer := time.NewTimer(s.cfg.GracePeriod)
	defer timer.Stop()

	select {
	case <-process.done:
	case <-timer.C:
		killed = true
		if err := process.handle.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return fmt.Errorf("kill provider process: %w", err)
		}
		select {
		case <-process.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	s.emit(ProcessEvent{
		Type:    ProcessEventStopped,
		Attempt: process.attempt,
		PID:     process.pid(),
		Err:     process.waitErr(),
		Killed:  killed,
	})
	return nil
}

// Interrupt cancels the current Provider turn without changing supervisor
// lifecycle state. The process remains available for subsequent work and no
// stopped event or lease cleanup is emitted.
func (s *ProcessSupervisor) Interrupt(ctx context.Context) error {
	if s == nil {
		return ErrInvalidProcessConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.mu.Lock()
	process := s.current
	s.mu.Unlock()
	if process == nil {
		return errors.New("provider process is not running")
	}
	if err := process.handle.Interrupt(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("interrupt provider turn: %w", err)
	}
	return nil
}

func (s *ProcessSupervisor) start(ctx context.Context, attempt int) (*runningProcess, error) {
	if s.cfg.StartAdmission != nil {
		if err := s.cfg.StartAdmission.PrepareProcessStart(ctx, attempt); err != nil {
			return nil, fmt.Errorf("prepare provider start admission: %w", err)
		}
	}
	handle, err := s.runner.Start(s.cfg.Command)
	if err != nil {
		return nil, fmt.Errorf("start provider process: %w", err)
	}

	process := &runningProcess{
		handle:  handle,
		attempt: attempt,
		done:    make(chan struct{}),
	}
	s.setCurrent(process)
	go func() {
		err := handle.Wait()
		process.mu.Lock()
		process.err = err
		process.mu.Unlock()
		close(process.done)
	}()
	if s.cfg.StartAdmission != nil {
		if err := s.cfg.StartAdmission.ConfirmProcessStarted(ctx, attempt); err != nil {
			stopCtx, cancel := context.WithTimeout(context.Background(), s.cfg.GracePeriod+time.Second)
			stopErr := s.Stop(stopCtx)
			cancel()
			if stopErr != nil {
				return nil, fmt.Errorf("confirm provider start admission: %w (stop rejected child: %v)", err, stopErr)
			}
			return nil, fmt.Errorf("confirm provider start admission: %w", err)
		}
	}
	s.emit(ProcessEvent{Type: ProcessEventStarted, Attempt: attempt, PID: process.pid()})
	return process, nil
}

func (s *ProcessSupervisor) setCurrent(process *runningProcess) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.current = process
	s.stopping = false
}

func (s *ProcessSupervisor) clearCurrent(process *runningProcess) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == process {
		s.current = nil
	}
}

func (s *ProcessSupervisor) markStopping() *runningProcess {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopping = true
	return s.current
}

func (s *ProcessSupervisor) isStopping() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopping
}

func (s *ProcessSupervisor) emit(event ProcessEvent) {
	select {
	case s.events <- event:
	default:
	}
}

func (p *runningProcess) waitErr() error {
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *runningProcess) pid() int {
	return p.handle.PID()
}

func normalizeProcessConfig(cfg ProcessConfig) (ProcessConfig, error) {
	if cfg.Command.Path == "" {
		return ProcessConfig{}, fmt.Errorf("%w: command path is required", ErrInvalidProcessConfig)
	}
	if cfg.MaxRestarts < 0 {
		return ProcessConfig{}, fmt.Errorf("%w: max restarts must not be negative", ErrInvalidProcessConfig)
	}
	if cfg.MaxRestarts == 0 {
		cfg.MaxRestarts = defaultMaxRestarts
	}
	if cfg.Backoff <= 0 {
		cfg.Backoff = defaultBackoff
	}
	if cfg.GracePeriod <= 0 {
		cfg.GracePeriod = defaultGracePeriod
	}
	return cfg, nil
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (execProcessRunner) Start(command ProcessCommand) (processHandle, error) {
	cmd := exec.Command(command.Path, command.Args...)
	cmd.Dir = command.Dir
	cmd.Env = providerEnvironment(command.Path, command.Env)
	cmd.ExtraFiles = nil
	cmd.Stdin = command.Stdin
	cmd.Stdout = command.Stdout
	cmd.Stderr = command.Stderr
	if err := applyProcessCredential(cmd, command.Credential); err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &execProcessHandle{cmd: cmd}, nil
}

func providerEnvironment(path string, explicit []string) []string {
	values := make([]string, 0, len(explicit)+8)
	seen := make(map[string]struct{})
	testHelper := path == os.Args[0]
	add := func(item string) {
		name, _, ok := strings.Cut(item, "=")
		if !ok || !safeProviderEnvName(name) {
			return
		}
		if _, exists := seen[name]; exists {
			return
		}
		seen[name] = struct{}{}
		values = append(values, item)
	}
	for _, item := range os.Environ() {
		name, _, ok := strings.Cut(item, "=")
		if ok && inheritedProviderEnvName(name, testHelper) {
			add(item)
		}
	}
	for _, item := range explicit {
		add(item)
	}
	return values
}

func inheritedProviderEnvName(name string, testHelper bool) bool {
	if strings.HasPrefix(name, "LC_") || strings.HasPrefix(name, "XDG_") {
		return true
	}
	switch name {
	case "PATH", "HOME", "LANG", "TERM", "TMPDIR", "TZ", "USER", "LOGNAME", "SHELL", "PWD":
		return true
	case "AGENTWHARF_WRAP_HELPER", "AGENTWHARF_ACP_HELPER", "AGENTWHARF_ACP_IDLE_HELPER", "AGENTWHARF_ACP_PERMISSION_HELPER", "AGENTWHARF_RESTART_CRASH_HELPER", "AGENTWHARF_START_BLOCK_HELPER", "AGENTWHARF_START_BLOCK_MARKER":
		return testHelper
	default:
		return false
	}
}

func safeProviderEnvName(name string) bool {
	if name == "" {
		return false
	}
	upper := strings.ToUpper(name)
	for _, marker := range []string{"TOKEN", "PASSWORD", "SECRET", "CREDENTIAL", "DSN", "SESSION", "HUB", "CLOUD", "MACHINE", "SIGNER", "CONTROL", "PROXY"} {
		if strings.Contains(upper, marker) {
			return strings.HasSuffix(upper, "_HELPER")
		}
	}
	return true
}

func (h *execProcessHandle) PID() int {
	if h.cmd.Process == nil {
		return 0
	}
	return h.cmd.Process.Pid
}

func (h *execProcessHandle) Wait() error {
	return h.cmd.Wait()
}

func (h *execProcessHandle) Interrupt() error {
	if h.cmd.Process == nil {
		return nil
	}
	return h.cmd.Process.Signal(os.Interrupt)
}

func (h *execProcessHandle) Kill() error {
	if h.cmd.Process == nil {
		return nil
	}
	return h.cmd.Process.Kill()
}
