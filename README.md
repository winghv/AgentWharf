# AgentWharf

[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Status: pre-release](https://img.shields.io/badge/status-pre--release-orange.svg)](#status)
[![Website](https://img.shields.io/badge/website-cloud.superwhv.me-black.svg)](https://cloud.superwhv.me/agentwharf)
[![Console](https://img.shields.io/badge/console-cloud.superwhv.me-0f766e.svg)](https://cloud.superwhv.me/app/machines)
[![Protocol](https://img.shields.io/badge/protocol-v1-2563eb.svg)](spec/v1.md)

Open-source session gateway for coding agents. AgentWharf lets you run Claude,
Codex, Gemini, or another ACP-compatible agent on your own machine, then control
the session from SuperWHV Console with durable replay, multi-client fanout, and
permission sync.

Links: [Website](https://cloud.superwhv.me/agentwharf) |
[SuperWHV Console](https://cloud.superwhv.me/app/machines) |
[Protocol spec](spec/v1.md) | [TypeScript client](client-ts/)

## Quickstart: Connect Your Own Machine

Prerequisites:

- Access to [SuperWHV Console](https://cloud.superwhv.me/app/machines).
- The agent you want to run is installed and authenticated on this machine.
- `npm` is available so the installer can install the Claude and Codex ACP
  bridge wrappers used by `wharf claude` and `wharf codex`.

Install Wharf:

```console
$ curl -fsSL https://github.com/winghv/agentwharf/releases/latest/download/install.sh | sh
```

The script downloads the matching prebuilt binary from GitHub Releases,
installs the `wharf` command, and installs the `claude-agent-acp` /
`codex-acp` provider bridge wrappers.

Run the same install command again to upgrade. When `wharf` already exists on
`PATH`, the installer upgrades that existing directory in place so the active
command is replaced instead of installing a second copy behind an older binary.
It also removes the legacy `agentwharf` command from that directory. Set
`AGENTWHARF_INSTALL_DIR` only when you explicitly want to override the target
directory.

Start the agent you want to use:

```console
$ wharf claude
# or:
$ wharf codex
```

The CLI prints a pairing prompt:

```text
Pair this machine at https://cloud.superwhv.me/app/machines
device_code: dev_xxxxx
user_code: ABCD-EFGH
```

Then open [Console Machines](https://cloud.superwhv.me/app/machines), paste the
`device_code` and `user_code`, give the machine a name, and confirm. The session
appears in Console and can be reopened from the browser or another client.

## Why AgentWharf

- **Connect your own machine**: keep your local provider login, quota, and secrets.
- **Durable sessions**: Hub-issued `seq` lets clients reconnect and replay missed
  events in order.
- **Multi-client control**: the same agent session can be viewed and controlled
  from CLI, browser, editor, or phone.
- **Permission sync**: approval requests are normalized and broadcast through the
  same session protocol.
- **ACP first**: providers should connect through Agent Client Protocol; stdio
  and structured-stream fallbacks are available for advanced adapters.

## How It Works

```text
wharf claude / wharf codex
  -> creates a device pairing code
  -> waits for Console confirmation
  -> exchanges the machine token for a session-bound adapter token
  -> starts the provider adapter
  -> connects to the AgentWharf Hub
```

Tokens are kept in memory. They are not printed by the CLI and are not written
to disk.

Core pieces:

- **AgentWharf Hub**: the single authority for a session event stream. It assigns
  `seq`, persists durable events, fans out live events, and replays gaps.
- **Adapter**: bridges Claude, Codex, Gemini, or another provider into the
  AgentWharf session protocol.
- **Protocol**: versioned WebSocket frames, durable and ephemeral events,
  commands with idempotency, scopes, and replay semantics.

## Advanced: Local Self-Host

Use this path when you want to run a local Hub without SuperWHV Console pairing:

```console
$ wharf serve
$ wharf wrap --agent claude --acp
# open the local URL from a browser or phone to observe and control the session
```

Advanced and test harnesses can still use the explicit managed pairing form:

```console
$ wharf wrap --agent claude --acp --pair --cloud https://cloud.superwhv.me/v1
```

Most users should start with `wharf claude` or `wharf codex`.

## Repository Layout

```text
spec/             # protocol spec (authoritative)
protocol/         # frame and event types, codecs, version negotiation
hub/              # hub library: connections, seq, fanout, replay
store/            # EventStore implementations (SQLite, Postgres)
auth/             # Authenticator implementations
masking/          # streaming secret masking
adapter/          # core adapter, ACP bridge, fallback runners
client-ts/        # TypeScript client SDK
examples/         # minimal web UI
cmd/agentwharf/   # CLI: serve / wrap / claude / codex / gemini
```

## Status

Pre-release. The protocol spec and implementation are under active development;
public release follows internal validation. The project is Apache-2.0 licensed.

## License

Apache-2.0
