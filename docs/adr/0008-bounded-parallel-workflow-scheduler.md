# ADR-0008: Bounded parallel workflow scheduler

Status: accepted
Date: 2026-08-29

## Context

Hub's first workflow engine executed one topological node at a time. The
cancellation/retry layer made attempts explicit and restart-observable, but
independent DAG nodes still could not overlap.

## Decision

- `WorkflowSpec.max_concurrency` is explicit and bounded to `1..=64`.
  Missing values deserialize as `1`, preserving existing workflow timing.
- The scheduler owns a lexicographically ordered ready set. A node becomes
  ready only after every explicit and data-flow dependency succeeds.
- Up to `max_concurrency` ready nodes execute on scoped worker threads through
  the existing synchronous `Executor` port. Executor location is not part of
  scheduling semantics.
- The persisted workflow record remains one coherent document. Worker writes
  are serialized through one record mutex; `StepResult` rows are sorted by key
  before persistence so concurrency does not leak timing into record order.
- Fail-fast remains the workflow policy: the first terminal step failure stops
  admission of new nodes and actively cancels every in-flight sibling. A user
  cancellation is distinguished by its persisted cancellation intent.
- Retries remain per-step and retain fresh attempt/run identities. Retry
  backoff occupies only that step's worker and does not block unrelated nodes.
- On daemon restart, persisted cancellation intent is reconciled first.
  A remaining `running` workflow is failed closed: local process attempts are
  not replayed automatically because Hub cannot prove whether a pre-crash
  process performed external side effects. This is recovery without duplicate
  execution, not transparent process reattachment.

## Consequences

- Parallelism is opt-in; old workflow JSON remains sequential.
- SQLite continues to serialize metadata writes behind its connection mutex,
  while execution itself can overlap.
- A later remote executor may reuse the same ready-set and attempt semantics;
  it must not fork scheduler policy.
- Transparent continuation of an in-flight local subprocess is deliberately
  out of scope until process identity/lease semantics make it safe.
