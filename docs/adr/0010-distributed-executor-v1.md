# ADR 0010 — Distributed executor v1

Status: accepted

## Decision

Hub's first distributed execution substrate remains behind the existing
`hub_core::exec::Executor` port. `RemoteExecutor` transports a run-local
workdir to an authenticated `scirust-hub-worker`, leases one process execution,
polls worker liveness, and materializes the returned workdir files locally.
The normal Hub orchestrator then performs the same output validation, artifact
ingestion and provenance recording used by the local process executor.

The wire contract is versioned independently (`WORKER_PROTOCOL_VERSION = 1`).
Every attempt has a stable `attempt_id`; lease creation is idempotent for an
identical retry and conflicts if the same attempt id carries a different
payload. Result commit is likewise idempotent for byte-identical duplicate
results and rejects conflicting duplicates.

The worker advertises identity, protocol version, capabilities, transport size
limit and heartbeat interval. The Hub rejects incompatible workers, stale
heartbeats, expired leases, mismatched attempt/lease/worker identities and
unsafe returned paths. Input and output files are transported as bytes with
workdir-relative paths; no shared filesystem is assumed.

## Failure semantics

Worker unavailability, authentication refusal, protocol mismatch, lease loss,
stale heartbeat and unsafe transport data become observed failed execution
outcomes. They do not create successful provenance. Cancellation is forwarded
to the active worker lease and remains represented by the existing Hub run
state machine.

Worker lease state is intentionally ephemeral in v1. Hub remains the durable
authority. A worker restart therefore causes an active remote attempt to fail
closed; later scheduler policy may choose an explicit retry with a new attempt.

## Security boundary

The v1 worker requires a bearer token and never logs it. The worker executes the
absolute program path supplied by an authenticated Hub using the same
`ProcessExecutor` resource controls. It is **not a sandbox** and bearer auth is
**not transport encryption**. Plain HTTP must therefore be limited to loopback
or a trusted private/tunneled network; production Internet exposure requires a
TLS/authenticated transport boundary in front of the worker.

## Deliberate non-goals

- no generic job queue or web-service platform;
- no shared filesystem requirement;
- no silent retry after worker loss;
- no claim that bearer auth is equivalent to mTLS;
- no distributed artifact cache yet;
- no capability-aware multi-worker scheduler yet.
