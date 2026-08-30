# ADR-0015: Opt-in native server TLS

- Status: Accepted
- Date: 2026-08-30

## Context

SciRust Hub already authenticates the control plane and remote worker protocol
with distinct bearer credentials, and `RemoteExecutor` accepts HTTPS worker
URLs. Before this change the two Rust servers themselves only accepted plaintext
HTTP, so deployments on untrusted networks required an external TLS terminator
or tunnel.

## Decision

`scirust-hubd` and `scirust-hub-worker` gain optional native HTTPS using
`axum-server` 0.8 with its Rustls feature. TLS is enabled only when both a PEM
certificate-chain path and PEM private-key path are configured. A half-configured
pair fails startup before binding. Malformed or unreadable PEM material also
fails startup.

The Hub variables are `SCIRUST_HUB_TLS_CERT` and `SCIRUST_HUB_TLS_KEY`; the
worker variables are `SCIRUST_HUB_WORKER_TLS_CERT` and
`SCIRUST_HUB_WORKER_TLS_KEY`. Matching CLI flags are available.

HTTP remains the default for local compatibility. A non-loopback control-plane
bind still requires `SCIRUST_HUB_TOKEN` regardless of TLS: encryption does not
replace application authentication. The worker bearer token also remains
mandatory. No secret value or private-key contents are logged.

The TLS daemon path uses `axum-server`'s handle-based graceful shutdown with a
30-second drain bound. The plaintext path retains Axum's existing graceful
shutdown behavior.

## Client trust

`RemoteExecutor` already accepts `https://` endpoints through its TLS-enabled
HTTP client. Standard certificate validation remains enabled. This ADR does not
add an insecure verification bypass; private/self-signed CAs must be installed
in the Hub host's trust configuration.

## Non-goals

This change does not implement:

- mTLS or client-certificate authorization;
- certificate issuance/rotation;
- certificate hot reload;
- ACME;
- replacement of bearer credentials.

## Consequences

Deployments can terminate TLS in the Hub/worker binaries without introducing a
reverse proxy, while existing loopback HTTP workflows continue unchanged. The
security boundary remains explicit: Rustls provides transport confidentiality
and server authentication; bearer tokens remain the application authentication
mechanism.
