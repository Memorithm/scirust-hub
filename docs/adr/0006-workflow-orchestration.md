# ADR-0006: Workflow orchestration (sequential)

Status: accepted
Date: 2026-08-25

## Context

Single runs proved the vertical slice; ecosystem workloads need multi-step
chains (produce → transform → store) with provenance at every hop. The DAG
primitive shipped in v0.1 but nothing scheduled it. Parallel/distributed
execution remains out of reach for a single-node control plane.

## Decision

- `WorkflowSpec` = ordered set of steps; each step is a normal single-run
  spec plus a unique key and optional explicit `after` dependencies.
- Dependencies come from two sources: data flow (`InputSource::FromStep`
  consumes a named output of another step) and explicit `after` entries.
  Both feed the existing cycle-checked `Dag`; execution follows its
  deterministic topological order.
- Execution is **sequential and fail-fast**: steps run one at a time; any
  failed step (or unresolvable input) fails the whole workflow immediately.
  This is stated in docs rather than dressed up as scheduling.
- Step inputs referencing prior steps resolve to that step's recorded output
  artifact (matched by label: `stdout`, `stderr`, `file:<name>`), so the
  artifact graph between runs is explicit in provenance.
- Workflows get their own typed id, lifecycle state machine
  (`created → running → succeeded|failed|cancelled`) and repository port;
  SQLite storage arrives as migration v2, exercising the schema-evolution
  path promised in ADR-0005.
- Workflow cancellation is modelled in the state machine but **not wired to
  running steps yet** — documented as deferred, not simulated.

## Consequences

- Long workflows block one thread of the daemon's blocking pool; acceptable
  for single-node use and honest about it.
- Adding parallel execution later changes only the engine loop, not specs or
  records.

## Alternatives considered

- Embedding steps inside RunSpec: rejected, run semantics stay single-step
  and simple.
- Building a generic scheduler now: rejected before a real workload exists.
