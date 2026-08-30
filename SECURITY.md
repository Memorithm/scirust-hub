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

## Control-plane authentication

`/api/v1/*` can be protected with a static bearer token supplied only through
`SCIRUST_HUB_TOKEN`. The daemon refuses a non-loopback listen address when that
token is absent. The CLI and the read-only MCP adapter automatically attach the
same environment variable when configured. `/health` and `/ready` intentionally
remain unauthenticated for supervisor probes.

The API stores only a SHA-256 verifier for the bearer token in shared state and
does not intentionally log token contents. The remote worker separately
requires `SCIRUST_HUB_WORKER_TOKEN`; these credentials serve different trust
boundaries and should not be reused.

Bearer authentication over HTTP **does not encrypt traffic** and does not
provide mTLS-style peer identity. Plain HTTP should remain on loopback or a
trusted private/tunneled network. Production exposure beyond that boundary must
terminate TLS at a trusted reverse proxy, service mesh or tunnel until native
TLS/mTLS support exists.

Authentication is currently coarse-grained: possession of the Hub token grants
access to the complete `/api/v1` surface. There are no per-principal roles yet.

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
