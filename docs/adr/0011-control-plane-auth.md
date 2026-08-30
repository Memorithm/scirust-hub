# ADR 0011 — Control-plane bearer authentication

Status: accepted

## Decision

`/api/v1/*` supports static bearer authentication configured only through the
`SCIRUST_HUB_TOKEN` environment variable. `/health` and `/ready` remain
unauthenticated so supervisors can probe process health without holding control
plane credentials.

The daemon refuses any non-loopback listen address unless a non-empty control
plane token is configured. Loopback remains unauthenticated by default for
backward-compatible local development. When a token is configured, it protects
the API even on loopback.

The HTTP adapter retains only `SHA-256(token)` in shared state. Incoming bearer
values are hashed and their fixed-size digests are compared without an early
exit. The CLI and read-only MCP adapter automatically attach
`SCIRUST_HUB_TOKEN` when it is present. Tokens are not accepted as command-line
arguments and are never intentionally logged.

## Boundary

Bearer authentication establishes possession of a shared secret; it does not
provide confidentiality or peer identity equivalent to TLS/mTLS. The daemon
therefore emits a warning for authenticated non-loopback plaintext HTTP.
Production network exposure must terminate TLS at a trusted reverse proxy,
service mesh or tunnel until native TLS/mTLS is deliberately implemented.

This change is authentication, not authorization. All authenticated API callers
retain the same control-plane permissions. Fine-grained principals/roles are a
future protocol decision.
