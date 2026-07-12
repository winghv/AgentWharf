# AgentWharf Protocol Specifications

| Version | Status | Idle-warning rule |
| --- | --- | --- |
| v1 | Frozen legacy | Emits only deprecated `session.idle_warning` through the approved window. |
| v2 | Current | Emits only opaque ephemeral `x.vm.idle_warning`; capability-gates `history.page`. |

The compatibility window ends at `2027-01-08T00:00:00Z`. One negotiated
connection receives one name only. The generic Hub does not interpret platform
payloads. T1B, T15K, and T52B remain time-gated until T69 and the separately
approved removal contract.
