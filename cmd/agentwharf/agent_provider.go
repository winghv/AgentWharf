package main

import (
	"context"
	"time"

	"github.com/winghv/agentwharf/protocol"
)

// launchSettings is the provider-neutral model/permission/reasoning tuple parsed
// from a launch's passthrough arguments. Empty means "provider default".
type launchSettings struct {
	model      string
	permission string
	reasoning  string
}

// agentProvider is the provider-specific half of the official-CLI launch path.
// The shared plumbing (PTY, resize, raw mode, Hub command injection, prompt
// dedup, state/settings publishing, transcript tailing) lives in
// official_provider.go and talks to an agent only through this interface, so a
// new agent can be added as a small module without touching the shared path.
type agentProvider interface {
	// command returns the native CLI binary name.
	command() string
	// sessionArgs returns provider-specific launch args that identify the
	// session (e.g. --session-id for Claude). It returns nil when the provider
	// discovers its session file after launch (Codex rollout files).
	sessionArgs(sessionID string) []string
	// transcriptPath returns the transcript file for the launched session,
	// bounded by launchTime. Providers that cannot compute it upfront may poll
	// the filesystem until a matching file appears.
	transcriptPath(ctx context.Context, cfg wrapConfig, sessionID string, launchTime time.Time) (string, error)
	// translateLine maps one transcript line to zero or more hub events.
	translateLine(sessionID string, line []byte) ([]protocol.Event, error)
	// launchSettings parses model/permission/reasoning from passthrough args.
	launchSettings(args []string) launchSettings
	// transcriptUserText returns the plain-text user message in a line ("" when
	// the line is not a plain-text user message), used to dedup Hub-injected
	// prompts in the transcript mirror.
	transcriptUserText(line []byte) string
}

// genericProvider launches an unrecognized agent's own binary but mirrors it
// with Claude's transcript semantics, preserving the historical behavior for
// agents other than claude and codex (which have no dedicated transcript module
// yet). Embedding claudeProvider inherits every method except command.
type genericProvider struct {
	name string
	claudeProvider
}

func (p genericProvider) command() string { return p.name }

// officialProviderForAgent selects the agent module for the official-CLI path.
func officialProviderForAgent(agent string) agentProvider {
	switch agent {
	case "codex":
		return codexProvider{}
	case "claude", "claude-code":
		return claudeProvider{}
	default:
		return genericProvider{name: agent}
	}
}
