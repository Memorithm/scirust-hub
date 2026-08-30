# ADR 0014 — Configured multi-worker placement

Status: accepted

## Context

The first remote execution substrate targeted one configured worker endpoint.
That proved transport, lease identity, liveness, cancellation and duplicate
attempt/result semantics, but independent parallel workflow steps could not be
spread across workers.

A naive retry/load-balancer wrapper would be unsafe: if a lease-create request
reaches worker A but the response is lost, replaying the execution on worker B
can duplicate externally visible side effects.

## Decision

`--executor remote` accepts one or more configured worker URLs. One URL keeps
the original direct `RemoteExecutor`. Two or more URLs construct a
`RemotePoolExecutor` using the same worker bearer credential.

Before every dispatch the pool queries each worker descriptor and keeps only
workers that are reachable, authorized, protocol-compatible, advertise
`process.execute.v1`, and satisfy the configured transport-size contract.
Worker ids must be unique across eligible endpoints; duplicate identity fails
closed before any lease is created.

Placement is deterministic within one Hub process: choose the eligible endpoint
with the lowest current Hub-local in-flight count, then worker id and endpoint
as lexical tie-breakers. The local count is only a placement hint; it is not a
claim about global cluster load.

After selection, the invocation is pinned to that endpoint and the existing
lease protocol remains authoritative. The pool does **not** fail over an
ambiguous lease creation or active lease to a second worker.

The executor port gains an `ExecutionReport` wrapper with a default
implementation. Placement-aware executors override it to return the concrete
selected target. `RunOutcome.executor_backend` therefore records e.g.
`remote:worker-a@http://...` instead of only the generic pool identity. This is
per-invocation data, so no shared mutable "last worker" field is introduced.

## Security and limits

All configured workers currently share one `SCIRUST_HUB_REMOTE_WORKER_TOKEN`.
The pool does not weaken the existing transport boundary: bearer credentials
still require trusted TLS/tunnel protection on untrusted networks, and workers
remain process executors rather than OS sandboxes.

This is **not** dynamic worker registration, expiry/heartbeating at the pool
level, global capacity accounting, resource labels, or capability-aware
placement beyond the existing process-execution protocol capability. Those are
separate future scheduler features.
