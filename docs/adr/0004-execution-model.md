# ADR-0004: Execution model

Status: accepted
Date: 2026-08-25

## Context

The Hub must separate WHAT to run (RunSpec, validated against the registry)
from HOW to run it (executor backends). Future backends include remote
runtimes, containers and SciCapsule execution; none of them exist today in a
form the Hub can contract against. SciRust's `scirust-digest` shows the
digest discipline the ecosystem uses; `scirust-provenance` signs emitted code
but stores no run records.

## Decision

### Ports (in `hub-core`, synchronous)

```rust
pub trait Executor {
    fn backend_id(&self) -> &str;
    fn execute(&self, request: &ExecutionRequest, cancel: &CancelToken)
        -> Result<ExecutionOutcome, ExecutorError>;
}
```

- The port is **blocking/synchronous**. Subprocess supervision with
  deadlines is naturally expressed synchronously (`try_wait` polling); the
  async HTTP layer offloads calls via `spawn_blocking`. This keeps
  `hub-core` free of runtimes and makes executors deterministic under test.
- `CancelToken` is an `Arc<AtomicBool>` checked between polls; cooperative,
  explicit, observable.
- `ExecutionOutcome` records exit status, captured stdout/stderr **as digests
  plus capped bytes**, truncation flags, duration, timeout flag.

### Backends (in `hub-executor`)

1. `ProcessExecutor`: structured requests only — `program`, `args[]`,
   working directory, constructed environment. Never a shell string; no
   `sh -c`. Hard caps: per-stream output bytes, wall-clock timeout, argument
   count/length. Environment is built from scratch (PATH only by default);
   nothing from the parent process leaks unless explicitly listed.
2. `MockExecutor`: scripted deterministic outcomes for tests; never used in
   the demo path or daemon defaults.

This is **not** a sandbox. A subprocess runs with the Hub's OS privileges;
the caps limit resource damage, not access. Documented as such everywhere the
executor appears.

### Provenance

Every completed run persists a `RunRecord` containing: ids, component
identity + version + declared source (git commit/dirty when provided),
capability + contract version, canonical parameter JSON digest + bytes,
input/output artifact ids with content digests, executor backend id,
environment keys (names only), exit code, timing, final state. Digests use
hub-namespaced domains (`scirust-hub:<domain>:v1`) with the same
length-framed SHA-256 construction as `scirust-digest`; cross-repository
verification is deferred until a shared digest crate is extracted
deliberately.

### DAG

A generic `Dag<T>` with cycle detection, topological ordering and size limits
ships now (unit-tested). Multi-node workflow *orchestration* is deferred;
single-run orchestration proves the vertical slice first.

## Consequences

- Adding a container/remote/SciCapsule executor later means implementing one
  trait; no domain change.
- Blocking executors cap concurrency at the caller's thread pool; acceptable
  for a single-node control plane and honest about it.

## Alternatives considered

- `async_trait` executor from day one: rejected, contagion without benefit
  while no network executor exists.
- Shell-string execution API: rejected outright (mission §13).
