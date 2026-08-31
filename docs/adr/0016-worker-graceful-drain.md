# ADR 0016 — Remote worker graceful drain

Status: accepted

## Context

`scirust-hub-worker` already gives every lease a cooperative `CancelToken`, and
the process executor kills its direct child when cancellation is observed.
Before this change, however, SIGINT/SIGTERM only ended the worker process by
tearing down its runtime. There was no admission barrier for new leases and no
explicit propagation of shutdown to active lease cancellation.

A worker disappearing is already fail-closed at the Hub: remote leases are
ephemeral and an affected run is never silently replayed on another worker.
That crash behavior remains the final safety net, but planned service shutdown
should use the cancellation machinery that already exists.

## Decision

The worker lease book owns a `draining` state under the same mutex that guards
attempt reservation. This makes the transition to drain mode atomic with
respect to genuinely new lease admission.

When draining starts:

1. descriptor discovery returns `503 Service Unavailable`, allowing a
   configured pool to exclude the worker before dispatch;
2. new attempt ids are rejected with `503`;
3. an idempotent replay of an already-reserved attempt is still returned, so
   drain mode does not turn a safe retry into a conflicting second execution;
4. queued leases are marked `cancelled` before process start and their tokens
   are cancelled;
5. running leases receive the same `CancelToken` cancellation path used by the
   normal lease-cancel endpoint.

The binary handles SIGINT/SIGTERM by entering drain mode, keeping status/cancel
HTTP handling available while active leases settle, and waiting up to 30
seconds before initiating server shutdown. TLS uses the existing
`axum-server` handle; plaintext HTTP uses Axum graceful shutdown. Runtime
blocking-task shutdown receives an additional short bound after the server
returns.

## Invariants and non-goals

- Worker protocol v1 is unchanged; no new wire fields are fabricated.
- Draining never admits a new attempt after the drain bit is committed.
- A queued lease cancelled before execution must not spawn a process.
- A running lease is not reported as terminal until its executor path commits a
  terminal result/failure.
- This does not make worker lease state durable and does not add post-dispatch
  failover, process sandboxing, descendant-process tree supervision, dynamic
  worker registration or global scheduling.
- A hard crash remains distinguishable from a graceful drain and continues to
  fail the corresponding Hub run closed.
