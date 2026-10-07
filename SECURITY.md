# Security policy

## Honest threat model

SciRust Hub executes processes. It provides resource control and execution
hygiene, not a sandbox:

- Process execution uses structured argv (no implicit shell construction).
- Child environments are constructed from scratch; only explicitly selected
  values are passed, and provenance records environment variable names rather
  than secret values.
- Captured output streams are bounded and truncation is recorded.
- Executions have wall-clock timeouts and cooperative cancellation.
- Working directories are per-run beneath the configured data directory.
- Remote worker transport rejects absolute/parent-traversal paths and does not
  assume a shared filesystem.

**A subprocess is not a sandbox.** Local and remote worker children execute with
the OS privileges of their respective daemon/worker process. They may access
resources permitted to that OS identity unless deployment-level isolation
prevents it.

## Control-plane authentication and authorization

The daemon supports two mutually exclusive bearer configurations:

- `SCIRUST_HUB_TOKEN` preserves the legacy single `legacy-control` principal
  with all current permissions.
- `SCIRUST_HUB_PRINCIPALS_JSON` accepts the strict version-1 static-principal
  document described by
  [ADR-0019](docs/adr/0019-static-principal-authorization.md). Each principal
  receives one or more closed permissions: `inspect` for protected GET/HEAD
  routes, `control` for state-changing routes and `metrics` for `/metrics`.

Unknown fields, versions and permissions, malformed or duplicate principals,
shared credentials and empty permission sets fail startup closed. Missing or
invalid credentials return HTTP 401; an authenticated principal without the
required permission receives HTTP 403. The CLI and read-only MCP adapter attach
`SCIRUST_HUB_TOKEN` when configured; clients using the multi-principal mode must
send their assigned bearer explicitly. `/health` and `/ready` intentionally
remain unauthenticated supervisor probes.

Bearer plaintext is reduced to a SHA-256 verifier in shared state and is not
intentionally logged. Successful control-plane mutations emit the non-secret
principal identifier through structured tracing, but the authoritative domain
lifecycle stream does not retroactively invent transport actors. This is
coarse route-category authorization, not tenancy or per-object authorization;
there is no dynamic principal management, OIDC, credential rotation or
secret-manager integration.

The remote worker separately requires `SCIRUST_HUB_WORKER_TOKEN`. Worker bearer
authentication has no control-plane principal/permission semantics, and these
credentials serve different trust boundaries and should not be reused.

## Transport security

Hub and worker can terminate native HTTPS when both members of their PEM
certificate/key pair are configured: `SCIRUST_HUB_TLS_CERT` plus
`SCIRUST_HUB_TLS_KEY`, or `SCIRUST_HUB_WORKER_TLS_CERT` plus
`SCIRUST_HUB_WORKER_TLS_KEY`. Supplying only one member fails closed. Clients
validate the server certificate through their TLS trust roots; private CAs must
be installed rather than bypassed.

TLS protects transport but does not replace bearer authorization. Native TLS
does not currently provide mTLS/client-certificate identity, certificate hot
reload or workload identity. Plain HTTP must therefore remain on loopback or a
trusted private/tunneled boundary.

## Additional boundaries

- Registration is metadata-only; registering a manifest never executes it.
- HTTP bodies are size-limited; manifests are version-checked and validated at
  domain construction time.
- Input artifact names are validated path components; blobs are
  content-addressed and written atomically.
- SciCapsule format/trust/extraction ownership remains in SciCapsule/SciRust;
  Hub validates only the published integration contract.

## Reporting

Report vulnerabilities privately to the maintainers via GitHub security
advisories for `Memorithm/scirust-hub` rather than public issues.
