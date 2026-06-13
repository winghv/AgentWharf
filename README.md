# AgentWharf

Durable session gateway for AI agents: durable per-session event log with hub-issued `seq`, multi-client fanout, reconnect replay, permission sync, and an ACP-first provider bridge.

> Status: pre-release. Protocol spec and implementation are under active development; public release follows internal validation.

## What It Is

- **AgentWharf Hub**: the single authority for a session's event stream — assigns `seq`, persists durable events (SQLite by default, pluggable `EventStore`), fans out to any number of clients, replays gaps on reconnect.
- **Adapter**: bridges a coding agent (Claude Code, Codex, Gemini, ...) into the AgentWharf Hub protocol. ACP (Agent Client Protocol) first; PTY/stdio fallback for providers without ACP support.
- **Protocol**: versioned WebSocket spec (`spec/v1.md`) — frames, durable/ephemeral events, commands with idempotency, scopes, replay semantics.

## Planned layout

```text
spec/        # protocol spec (authoritative)
protocol/    # frame & event types, codecs, version negotiation
hub/         # hub library: connections, seq, fanout, replay
store/       # EventStore implementations (sqlite, postgres)
auth/        # Authenticator implementations (static)
masking/     # streaming secret masking
adapter/     # core, acp bridge, fallback runners
client-ts/   # TypeScript client SDK
examples/    # minimal web UI
cmd/agentwharf/  # single binary: serve / wrap
```

## Self-Host Goal

```console
$ agentwharf serve
$ agentwharf wrap --agent claude --acp
# open the local URL from browser/phone: observe and control your local agent remotely
```

## License

Apache-2.0
