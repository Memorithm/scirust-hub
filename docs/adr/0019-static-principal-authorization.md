# ADR 0019: Static control-plane principals and authorization

Status: accepted for HUB2.

## Context

The Hub HTTP control plane previously supported either no authentication on the local loopback deployment or one optional `SCIRUST_HUB_TOKEN`. Once authenticated, every caller had the same authority. HUB2 needs least-privilege transport identity without moving HTTP authorization into `hub-core` execution/scientific semantics.

## Decision

The daemon accepts an optional versioned `SCIRUST_HUB_PRINCIPALS_JSON` document with `schema_version: 1` and an ordered `principals` array. Each principal declares a bounded non-secret lowercase identifier, one bearer credential, and one or more permissions from the closed V1 vocabulary:

- `inspect`: read-only `/api/v1/*` GET/HEAD inspection;
- `control`: state-changing protected operations;
- `metrics`: `/metrics` inspection.

Unknown fields, schema versions, permissions, malformed identifiers, empty credentials or permission sets, duplicate principal identifiers, duplicate permissions, and shared bearer credentials fail closed during startup. `SCIRUST_HUB_TOKEN` and the multi-principal document are mutually exclusive. Legacy single-token mode remains a full-control `legacy-control` principal. With neither configuration, existing unauthenticated loopback behavior is unchanged. `/health` and `/ready` remain outside the protected router.

Bearer plaintext is reduced to SHA-256 when a `StaticPrincipal` is constructed and is not retained in shared state. Verification keeps the existing non-early-exit digest comparison property. Principal identity is derived only from the matched configured verifier; request headers or bodies cannot assert it.

The authorization middleware classifies the existing control-plane actions before handlers execute. Missing or invalid credentials return the existing machine-readable `unauthorized` envelope with HTTP 401. A valid principal lacking the required permission returns additive `forbidden` with HTTP 403. The protocol remains schema version 1 because the error-code variant is additive and request DTOs are unchanged.

## Audit boundary

`hub-core` lifecycle events are append-only records produced by authoritative domain mutations and intentionally do not carry transport context. Injecting HTTP principal identity into those domain APIs solely for audit would violate the adapter boundary and would also invite invented actors for non-HTTP/internal mutations.

For HUB2 V1, successful HTTP `control` operations therefore **emit** the authenticated non-secret `principal_id`, method, path, and status through structured control-plane tracing after the handler succeeds. Tokens and token digests are never emitted. Historical lifecycle events are unchanged. If durable actor attribution becomes required for multiple ingress adapters, it should be introduced as a separately versioned audit-evidence facility with an explicit authoritative boundary rather than retrofitted into scientific lifecycle semantics.

## Consequences

Least privilege is available without changing `hub-core`, request DTOs, remote-worker lease semantics, or MCP's read-only policy. Metrics permission is explicit rather than inherited accidentally. This slice does not add mTLS identity, OIDC, dynamic principal management, rotation, or secret-manager integration.

Tests cover legacy authentication, 401 versus 403, inspect/metrics access, every current mutating route category, principal-spoof attempts, strict configuration validation, duplicate identities/credentials, and unauthenticated health/readiness compatibility. The repository CI remains authoritative for integration validation.
