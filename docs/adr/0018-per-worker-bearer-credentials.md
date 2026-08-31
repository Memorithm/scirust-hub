# ADR 0018 — Per-worker bearer credentials

Status: accepted

## Context

Configured multi-worker placement originally cloned one
`SCIRUST_HUB_REMOTE_WORKER_TOKEN` into every `RemoteExecutor`. That is simple
and remains useful for small trusted deployments, but compromise or rotation of
one shared bearer affects the whole configured worker pool. The worker protocol
already authenticates every request independently, so credential isolation does
not require a wire-protocol change.

`RemoteExecutor` also derived `Debug` while storing its token as a `String`. No
current production log intentionally formats an executor with `Debug`, but a
secret-bearing type should not make accidental debug disclosure easy.

## Decision

The shared `SCIRUST_HUB_REMOTE_WORKER_TOKEN` mode remains supported. A daemon may
instead set the environment-only `SCIRUST_HUB_REMOTE_WORKER_TOKENS_JSON` value
to a JSON object mapping configured worker endpoint to bearer token. No CLI flag
is added for the JSON map, avoiding routine placement of multiple bearer values
in process argv/history.

Configuration fails closed when:

- shared and per-worker credential modes are both present;
- a configured endpoint is duplicated after trailing-slash normalization;
- the JSON value is malformed or empty;
- a bearer value is empty;
- distinct JSON keys normalize to the same endpoint;
- a configured endpoint lacks a credential; or
- the credential map contains an endpoint that is not configured.

Endpoint order continues to come from `SCIRUST_HUB_REMOTE_WORKER_URL` /
`--remote-worker-url`; credentials do not affect placement ordering. The pool
receives `(endpoint, token)` pairs and constructs one `RemoteExecutor` per pair.
Single-worker remote mode can use either shared or mapped configuration through
the same resolver.

`RemoteExecutor` now implements a manual `Debug` representation that renders the
token field as `[REDACTED]`. Validation errors and executor backend identities
contain endpoint/worker identity where useful but never bearer values.

## Invariants

- Worker protocol v1 and lease semantics are unchanged.
- A credential is bound only to its configured endpoint before descriptor
  discovery or lease creation.
- Missing/extra/ambiguous credential configuration cannot silently fall back to
  another token.
- Bearer values are not added to provenance, backend ids or logs by this change.
- Existing shared-token deployments remain source/configuration compatible.

## Non-goals

This does not add dynamic worker registration, automatic secret rotation, a
secret-manager integration, mTLS/client certificates, fine-grained worker
authorization, global scheduler state or post-dispatch failover. Those require
separate designs.
