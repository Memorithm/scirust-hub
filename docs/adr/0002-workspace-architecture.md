# ADR-0002: Workspace architecture

Status: accepted
Date: 2026-08-25

## Context

The Hub needs domain logic, a stable wire protocol, process execution, an HTTP
API and two binaries. More crates mean more ceremony; fewer crates risk mixing
concerns the mission explicitly separates (domain / protocol / storage /
execution / transport).

## Decision

Four library crates and two application binaries:

```text
crates/
├── hub-core       domain model + ports (sync, no IO runtimes)
│                  ids, digests, capabilities, components, runs,
│                  DAG, validation, in-memory repositories
├── hub-protocol   versioned wire DTOs (serde), API error envelope
├── hub-executor   ProcessExecutor (std::process, capped outputs,
│                  timeout, cancellation) + deterministic MockExecutor
└── hub-api        axum router mapping HTTP <-> domain services
apps/
├── scirust-hub    CLI client for a running daemon
└── scirust-hubd   daemon binary
```

Rules enforced by dependency direction:

- `hub-core` depends only on `serde`, `thiserror`, `uuid`, `sha2`. No tokio,
  no axum, no process spawning.
- `hub-executor` implements the `Executor` port defined in `hub-core`.
- `hub-api` depends on core/protocol/executor; handlers stay thin.
- Binaries compose everything; they contain no domain rules.

Persistence starts in-memory behind repository traits inside `hub-core`
(ADR-0005). A future `hub-store-sqlite` would be a new crate implementing the
same traits; nothing else changes.

## Consequences

- The domain stays testable without network or hardware.
- The wire format can evolve without touching domain semantics.
- Adding a SQLite backend later is additive.

## Alternatives considered

- Single crate with modules: fastest to start, but lets transport types leak
  into the domain and makes the "protocol is more stable than internals" rule
  unverifiable at compile time.
- Six crates (separate store/registry crates from day one): the registry is
  domain logic over repository ports and has no independent compile boundary
  today; splitting it now adds ceremony without substitution value.
